This phase receives `ev-telemetry` from phase 1 and creates 15 minute feature vectors for each charging station using Apache Flink. Please refer to the pdf doc for more details. 


## How to run

1. First, run phase 1 producer and check `ev-telemetry` contains messages in the proper format. Please refer to phase1 README.md for details. 
2. You will need Python 3.11. If you have a higher version installed you need to create a python virtual environment. 

```bash
py -3.11 -m venv .venv
```

Then activate it by:

```bash
py -3.11 -m venv .venv
```

3. Now, inside the .venv enter

```bash
python -m pip install -r requirements.txt
```

4. Then

```bash
python flink_feature_engineering.py

```

Now you should see:
```bash
Phase 2 feature engineering job started.                               
Reading from Kafka topic: ev-telemetry
Writing feature vectors to Kafka topic, features

```
5. Keep phase 2 running and open a new terminal and enter
```bash
docker exec -it phase1-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic features --from-beginning --max-messages 5
```

Sample output:

```json
{"station_id":"114389",
"time_bin":"2023-01-01 00:00:00",
"mean_kwh":0.5499999999999999,
"variance_kwh":-3.700743415417188E-17,
"rate_of_change":0.0,
"capacity_utilization_ratio":0.03666666666666666,
"hour_of_day":0,
"day_of_week":6,
"anomaly_flag":0,
"data_completeness":1.0
}
```




