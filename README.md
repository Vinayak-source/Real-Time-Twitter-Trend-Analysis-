# Real-Time Twitter Trend Analysis

A Kafka + Spark Structured Streaming pipeline that replays stored tweets as a simulated live feed, counts trending hashtags in event-time windows, prints the results to the console and stores them in MySQL so they can be queried with SQL.

> **Note:** this is a single-machine simulation. There is no live Twitter API access. Tweets that were collected earlier and stored in MySQL are replayed through Kafka at random intervals and batch sizes.

## Overview

- **MySQL** holds the collected tweets (`tweets`, `hashtags`, `tweet_hashtags`) and, after processing, the trend results (`hashtag_trends`).
- **Kafka producer** (`kafka_producer.py`) reads tweets from MySQL and publishes them to Kafka topics at random intervals to imitate a live stream.
- **Spark consumer** (`spark_streaming_consumer_mysql.py`) reads the `tweets` topic, extracts hashtags, counts tweets per hashtag in 15-minute tumbling windows, prints each micro-batch and then upserts the counts into MySQL.

## Architecture

```
MySQL (tweets, hashtags, tweet_hashtags)
        │
        ▼
kafka_producer.py  ──►  Kafka topic: tweets
                              │
                              ▼
          spark_streaming_consumer_mysql.py
          (parse → extract hashtags → 15-min window counts)
                    │                    │
                    ▼                    ▼
              console output     MySQL table: hashtag_trends
```

## Tech Stack

- **Ingestion:** Apache Kafka (KRaft mode, no ZooKeeper)
- **Processing:** Apache Spark Structured Streaming (`spark-sql-kafka` connector), PySpark
- **Storage:** MySQL
- **Language:** Python

## Project Structure

```
.
├── kafka_producer.py                  # Replays stored tweets from MySQL into Kafka topics
├── spark_streaming_consumer_mysql.py  # Hashtag trends: prints each batch, stores results in MySQL
├── spark_streaming_consumer.py        # Earlier streaming experiment (console/CSV), kept for reference
├── spark_batch_consumer.py            # Earlier experiment: tweet counts per language, 5-minute window
├── Database/
│   └── General Database Structure/
│       └── TABLES_TWEET_DATABASE.sql  # Input tables (schema only, no data)
└── steps.txt                          # Short setup notes
```

The full database dump (scraped tweet content) is not included; only the schema is.

## Setup & Usage

### 1. Database
Import the schema, then load your own tweet data into the three tables:
```bash
mysql -u <user> -p <database_name> < "Database/General Database Structure/TABLES_TWEET_DATABASE.sql"
```
The results table `hashtag_trends` is created automatically by the consumer.

### 2. Kafka
Start Kafka in KRaft mode and create the topic:
```bash
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic tweets
```

### 3. Producer
Set your database details in the placeholders at the top of `kafka_producer.py` (do not commit real credentials), then:
```bash
source kafka-env/bin/activate
python3 kafka_producer.py
```

### 4. Consumer
Install the MySQL driver and set the database details as environment variables:
```bash
pip install mysql-connector-python
export DB_HOST=... DB_USER=... DB_PASSWORD=... DB_NAME=...
```
Run the consumer. The Kafka package version must match your Spark version (`spark-submit --version`):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> spark_streaming_consumer_mysql.py
```
Set `SINK=console` for a dry run that prints results without writing to MySQL.

### 5. Query the results
```sql
SELECT hashtag, tweet_count
FROM hashtag_trends
WHERE window_start = (SELECT MAX(window_start) FROM hashtag_trends)
ORDER BY tweet_count DESC
LIMIT 10;
```

## Design Decisions

- **Kafka in KRaft mode** avoids a separate ZooKeeper dependency and keeps local setup simple.
- **Kafka between MySQL and Spark** decouples the producer from the consumer and buffers messages if Spark is slow or restarts.
- **Update output mode + upsert.** Each micro-batch contains only the windows whose counts changed. They are written with `INSERT ... ON DUPLICATE KEY UPDATE` on `(window_start, hashtag)`, so writing the same batch twice gives the same table.
- **Watermark + de-duplication.** `kafka_producer.py` can send the same rows more than once, so the consumer drops duplicates on `(tweet_id, event_time)`; the watermark lets Spark discard old state.
- **Hashtags are lower-cased** so `#Python` and `#python` count together.
- **Normalised MySQL schema** (`tweets`, `hashtags`, `tweet_hashtags` with keys and `ON DELETE CASCADE`) supports querying hashtags over time.

## Status and Known Limitations

- Runs on one machine with one partition per topic; no cluster or load testing was done.
- Tweets are replayed from MySQL, so the feed is simulated, not live.
- Hashtags are extracted from the tweet text with a regular expression, because the `tweets` topic has no hashtag field.
- `date_time` is stored as `VARCHAR` in the source table; the consumer reads its first 19 characters (`yyyy-MM-dd HH:mm:ss`).
- The earlier consumers (`spark_streaming_consumer.py`, `spark_batch_consumer.py`) parse JSON, while the current producer sends comma-separated text, so they need adapting before use.
- No automated tests and no benchmark numbers for streaming versus batch processing.


## Author

Vinayak — B.Tech Computer Science, PES University
