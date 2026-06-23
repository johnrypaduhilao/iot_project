import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    flink_env = StreamExecutionEnvironment.get_execution_environment()
    flink_env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(
        flink_env,
        environment_settings=settings
    )

    kafka_jar_path = os.path.abspath(
        "./flink-sql-connector-kafka-4.0.1-2.0.jar"
    )

    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"file:///{kafka_jar_path.replace(os.sep, '/')}"
    )

    table_env.execute_sql("""
        CREATE TABLE ev_telemetry (
            station_id STRING,
            time_new STRING,
            duration DOUBLE,
            kwh DOUBLE,

            event_time AS TO_TIMESTAMP(time_new),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ev-telemetry',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'phase2-flink-feature-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # upsert-kafka is required because rate_of_change does a self-join on the previous window
    table_env.execute_sql("""
        CREATE TABLE features (
            station_id STRING,
            time_bin STRING,
            mean_kwh DOUBLE,
            variance_kwh DOUBLE,
            rate_of_change DOUBLE,
            capacity_utilization_ratio DOUBLE,
            hour_of_day INT,
            day_of_week INT,
            anomaly_flag INT,
            data_completeness DOUBLE,
            PRIMARY KEY (station_id, time_bin) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'features',
            'properties.bootstrap.servers' = 'localhost:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE data_quality (
            station_id STRING,
            time_new STRING,
            original_kwh DOUBLE,
            corrected_kwh DOUBLE,
            rolling_mean_kwh DOUBLE,
            rolling_std_kwh DOUBLE,
            reason STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'data-quality',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)

    statement_set = table_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO features
        WITH event_stats AS (
            SELECT
                station_id,
                time_new,
                event_time,
                duration,
                kwh,

                AVG(kwh) OVER (
                    PARTITION BY station_id
                    ORDER BY event_time
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
                ) AS rolling_mean_kwh,

                STDDEV_POP(kwh) OVER (
                    PARTITION BY station_id
                    ORDER BY event_time
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
                ) AS rolling_std_kwh

            FROM ev_telemetry
        ),

        cleaned_events AS (
            SELECT
                station_id,
                time_new,
                event_time,
                duration,
                kwh AS original_kwh,
                rolling_mean_kwh,
                rolling_std_kwh,

                CASE
                    WHEN rolling_mean_kwh IS NOT NULL
                         AND rolling_std_kwh IS NOT NULL
                         AND rolling_std_kwh > 0
                         AND ABS(kwh - rolling_mean_kwh) > 3 * rolling_std_kwh
                    THEN 1
                    ELSE 0
                END AS anomaly_flag,

                CASE
                    WHEN rolling_mean_kwh IS NOT NULL
                         AND rolling_std_kwh IS NOT NULL
                         AND rolling_std_kwh > 0
                         AND ABS(kwh - rolling_mean_kwh) > 3 * rolling_std_kwh
                    THEN rolling_mean_kwh
                    ELSE kwh
                END AS corrected_kwh

            FROM event_stats
        ),

        windowed_features AS (
            SELECT
                station_id,

                TUMBLE_START(event_time, INTERVAL '15' MINUTE) AS window_start,

                AVG(corrected_kwh) AS mean_kwh,

                VAR_POP(corrected_kwh) AS variance_kwh,

                AVG(corrected_kwh) / 15.0 AS capacity_utilization_ratio,

                CAST(
                    EXTRACT(
                        HOUR FROM TUMBLE_START(event_time, INTERVAL '15' MINUTE)
                    ) AS INT
                ) AS hour_of_day,

                MOD(
                    DAYOFWEEK(TUMBLE_START(event_time, INTERVAL '15' MINUTE)) + 5,
                    7
                ) AS day_of_week,

                MAX(anomaly_flag) AS anomaly_flag,

                CAST(
                    SUM(CASE WHEN original_kwh > 0 THEN 1 ELSE 0 END)
                    AS DOUBLE
                ) / COUNT(*) AS data_completeness

            FROM cleaned_events

            GROUP BY
                station_id,
                TUMBLE(event_time, INTERVAL '15' MINUTE)
        ),

        -- LAG() OVER (ORDER BY window_start) doesn't work here: window_start
        -- is a plain TIMESTAMP (not a rowtime), and Flink's streaming planner
        -- can't build the OVER frame for it ("OVER RANGE FOLLOWING windows
        -- are not supported yet"). A self-join on station_id + 15-minute
        -- offset gives the same "previous window" value without needing
        -- rowtime ordering.
        features_with_previous AS (
            SELECT
                curr.station_id,
                curr.window_start,
                curr.mean_kwh,
                curr.variance_kwh,
                curr.capacity_utilization_ratio,
                curr.hour_of_day,
                curr.day_of_week,
                curr.anomaly_flag,
                curr.data_completeness,

                COALESCE(prv.mean_kwh, curr.mean_kwh) AS previous_mean_kwh

            FROM windowed_features curr
            LEFT JOIN windowed_features prv
                ON prv.station_id = curr.station_id
               AND prv.window_start = curr.window_start - INTERVAL '15' MINUTE
        )

        SELECT
            station_id,

            DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS time_bin,

            mean_kwh,

            variance_kwh,

            mean_kwh - previous_mean_kwh AS rate_of_change,

            capacity_utilization_ratio,

            hour_of_day,

            day_of_week,

            anomaly_flag,

            data_completeness

        FROM features_with_previous
    """)

    statement_set.add_insert_sql("""
        INSERT INTO data_quality
        WITH event_stats AS (
            SELECT
                station_id,
                time_new,
                event_time,
                kwh,

                AVG(kwh) OVER (
                    PARTITION BY station_id
                    ORDER BY event_time
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
                ) AS rolling_mean_kwh,

                STDDEV_POP(kwh) OVER (
                    PARTITION BY station_id
                    ORDER BY event_time
                    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
                ) AS rolling_std_kwh

            FROM ev_telemetry
        ),

        cleaned_events AS (
            SELECT
                station_id,
                time_new,
                kwh AS original_kwh,
                rolling_mean_kwh,
                rolling_std_kwh,

                CASE
                    WHEN rolling_mean_kwh IS NOT NULL
                         AND rolling_std_kwh IS NOT NULL
                         AND rolling_std_kwh > 0
                         AND ABS(kwh - rolling_mean_kwh) > 3 * rolling_std_kwh
                    THEN rolling_mean_kwh
                    ELSE kwh
                END AS corrected_kwh,

                CASE
                    WHEN rolling_mean_kwh IS NOT NULL
                         AND rolling_std_kwh IS NOT NULL
                         AND rolling_std_kwh > 0
                         AND ABS(kwh - rolling_mean_kwh) > 3 * rolling_std_kwh
                    THEN 1
                    ELSE 0
                END AS anomaly_flag

            FROM event_stats
        )

        SELECT
            station_id,
            time_new,
            original_kwh,
            corrected_kwh,
            rolling_mean_kwh,
            rolling_std_kwh,
            'kwh value exceeded 3 standard deviations from station rolling mean' AS reason

        FROM cleaned_events
        WHERE anomaly_flag = 1
    """)

    result = statement_set.execute()

    print("Phase 2 feature engineering job started.")
    print("Reading from Kafka topic: ev-telemetry")
    print("Writing feature vectors to Kafka topic: features")
    print("Writing anomalous records to Kafka topic: data-quality")

    result.wait()


if __name__ == "__main__":
    main()
