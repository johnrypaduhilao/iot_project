import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    # Creating Flink environment
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
        f"file:///{kafka_jar_path.replace(os.sep, '/')}" )

    # Reading raw telemetry from Phase 1 Kafka topic

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

    
    # Writing feature vectors to Kafka topic "features"
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
            data_completeness DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'features',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """)


    # Grouping by station_id, creating 15-minute tumbling windows, computing feature vector
   
    result = table_env.execute_sql("""
        INSERT INTO features
        SELECT
            station_id,

            DATE_FORMAT(
                TUMBLE_START(event_time, INTERVAL '15' MINUTE),
                'yyyy-MM-dd HH:mm:ss'
            ) AS time_bin,

            AVG(kwh) AS mean_kwh,

            VAR_POP(kwh) AS variance_kwh,

            0.0 AS rate_of_change,

            AVG(kwh) / 15.0 AS capacity_utilization_ratio,

            CAST(
                EXTRACT(
                    HOUR FROM TUMBLE_START(event_time, INTERVAL '15' MINUTE)
                ) AS INT
            ) AS hour_of_day,

            MOD(
                DAYOFWEEK(TUMBLE_START(event_time, INTERVAL '15' MINUTE)) + 5,
                7
            ) AS day_of_week,

            0 AS anomaly_flag,

            CAST(
                SUM(CASE WHEN kwh > 0 THEN 1 ELSE 0 END)
                AS DOUBLE
            ) / COUNT(*) AS data_completeness

        FROM ev_telemetry

        GROUP BY
            station_id,
            TUMBLE(event_time, INTERVAL '15' MINUTE)
    """)

    print("Phase 2 feature engineering job started.")
    print("Reading from Kafka topic: ev-telemetry")
    print("Writing feature vectors to Kafka topic, features")

    result.wait()


if __name__ == "__main__":
    main()