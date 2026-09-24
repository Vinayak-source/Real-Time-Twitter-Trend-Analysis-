# Real-Time Twitter Trend Analysis

A real-time data streaming pipeline that ingests live tweet data using Apache Kafka and processes it with Apache Spark Streaming to detect trending hashtags, storing structured results in MySQL for querying and analytics.

## Overview

This project implements a producer-consumer architecture to analyze Twitter data in real time:

- **Kafka Producer** streams live tweet data into Kafka topics.
- **Spark Streaming Consumer** processes tweets in near real-time to extract and rank trending hashtags.
- **Spark Batch Consumer** runs batch-based analysis for historical/aggregate insights.
- **MySQL Database** stores structured output (tweets, hashtags, top trends) for querying and analytics.

## Architecture

```
Twitter Data → Kafka Producer → Kafka Topics (tweets, hashtags, top_tweets)
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    ▼                                           ▼
          Spark Streaming Consumer                   Spark Batch Consumer
          (real-time hashtag trends)                 (historical/batch analysis)
                    │                                           │
                    └─────────────────────┬─────────────────────┘
                                          ▼
                                    MySQL Database
```

## Tech Stack

- **Ingestion:** Apache Kafka (KRaft mode, no ZooKeeper dependency)
- **Processing:** Apache Spark Streaming (`spark-sql-kafka` connector)
- **Storage:** MySQL
- **Language:** Python

## Project Structure

```
.
├── kafka_producer.py             # Streams tweet data into Kafka topics
├── spark_streaming_consumer.py   # Real-time trend detection from Kafka stream
├── spark_batch_consumer.py       # Batch processing for historical analysis
├── Database/
│   └── General Database Structure/
│       └── TABLES_TWEET_DATABASE.sql   # DB schema (tables only, no data)
└── steps.txt                     # Setup and run instructions
```

> Note: The full database dump (with scraped tweet content) is excluded from this repository. Only the schema is included to demonstrate database design.

## Setup & Usage

### 1. Create Kafka topics
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic tweets
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic hashtags
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic top_tweets
```

### 2. Start Kafka (KRaft mode)
```bash
bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

### 3. Run the producer
```bash
source kafka-env/bin/activate
python3 kafka_producer.py
```

### 4. Run the Spark consumers
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 spark_streaming_consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 spark_batch_consumer.py
```

### 5. Database setup
Import the schema before running the consumers:
```bash
mysql -u <user> -p <database_name> < "Database/General Database Structure/TABLES_TWEET_DATABASE.sql"
```

## Key Design Decisions

- Used **Kafka's KRaft mode** to avoid a separate ZooKeeper dependency, simplifying local setup.
- Split processing into a **streaming consumer** (low-latency trend detection) and a **batch consumer** (periodic, heavier aggregation), balancing real-time responsiveness with more thorough analysis.
- Structured MySQL schema to support efficient querying of trending hashtags over time.

## Author

Vinayak — B.Tech Computer Science, PES University
