"""
spark_streaming_consumer_mysql.py
=================================
Spark Structured Streaming consumer that
  1. reads tweets from the Kafka topic `tweets` (as sent by kafka_producer.py),
  2. counts how many tweets mention each hashtag in 15-minute event-time windows,
  3. PRINTS every micro-batch result to the console, and then
  4. STORES the same rows in MySQL (table `hashtag_trends`) so they can be queried.

This file works with the existing, unchanged kafka_producer.py.

How it handles the producer's output
  * The producer sends plain text:  "tweet_id,tweet,date_time,language".
    The tweet text itself may contain commas, so the fields are cut from both
    ends (id = before the first comma, language and date_time = after the last
    two commas, tweet = everything in between).
  * The `tweets` topic has no hashtag field, so hashtags are extracted from the
    tweet text (#word).
  * kafka_producer.py re-sends the same rows on every loop, so duplicates are
    removed (dropDuplicates on tweet_id + event time, bounded by the watermark).
    Otherwise the counts would keep growing for the same tweets.

How the results are stored
  * Output mode is "update": each micro-batch holds only the windows whose
    counts changed, with their latest cumulative counts.
  * Those rows are UPSERTED (INSERT ... ON DUPLICATE KEY UPDATE) on
    (window_start, hashtag).  Writing the same batch twice gives the same table.
  * Writing happens on the driver with mysql-connector-python (no JDBC jar).

Requirements
    pip install mysql-connector-python
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> \
        spark_streaming_consumer_mysql.py
    (<SPARK_VERSION> must match `spark-submit --version`, e.g. 3.5.5)

Configuration (environment variables; avoid putting passwords in the file)
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME   MySQL connection
    KAFKA_BOOTSTRAP   default localhost:9092
    KAFKA_TOPIC       default tweets
    WINDOW_DURATION   default "15 minutes"  (tumbling window)
    WATERMARK         default "15 minutes"
    STARTING_OFFSETS  default earliest
    CHECKPOINT_DIR    default ./checkpoints/hashtag_trends
    SINK              "mysql" (default) or "console" (dry run: print only)

Query the results afterwards, for example:
    SELECT hashtag, tweet_count FROM hashtag_trends
    WHERE window_start = (SELECT MAX(window_start) FROM hashtag_trends)
    ORDER BY tweet_count DESC LIMIT 10;
"""
import os

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Regular expressions (Java syntax, used by Spark) for the producer's text format.
RE_TWEET_ID = r"^(\d+),"
RE_TWEET = r"(?s)^\d+,(.*),[^,]*,[^,]*$"
RE_DATE_TIME = r",([^,]*),[^,]*$"
RE_LANGUAGE = r",([^,]*)$"
HASHTAG_SQL = r"regexp_extract_all(tweet, '#(\\w+)', 1)"   # SQL string: \\w -> \w

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS hashtag_trends (
    window_start DATETIME NOT NULL,
    window_end   DATETIME NOT NULL,
    hashtag      VARCHAR(300) COLLATE utf8mb4_bin NOT NULL,
    tweet_count  BIGINT NOT NULL,
    updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, hashtag)
)
"""

UPSERT_SQL = (
    "INSERT INTO hashtag_trends (window_start, window_end, hashtag, tweet_count) "
    "VALUES (%s, %s, %s, %s) "
    "ON DUPLICATE KEY UPDATE tweet_count = VALUES(tweet_count)"
)


def db_config():
    return dict(
        host=os.getenv("DB_HOST", "YOUR_HOST_NAME"),
        user=os.getenv("DB_USER", "YOUR_USER_NAME_OF_DATABASE"),
        password=os.getenv("DB_PASSWORD", "DATABASE_PASSWORD"),
        database=os.getenv("DB_NAME", "DATABASE_NAME"),
    )


def to_upsert_rows(records):
    """Convert Spark rows (or dicts) into parameter tuples for UPSERT_SQL."""
    return [
        (r["window_start"], r["window_end"], r["hashtag"], int(r["tweet_count"]))
        for r in records
        if r["hashtag"]
    ]


def ensure_table():
    import mysql.connector

    conn = mysql.connector.connect(**db_config())
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    finally:
        conn.close()


def make_batch_writer(sink):
    """Return the function Spark calls once per micro-batch: print, then store."""
    from pyspark.sql.functions import col, date_format

    def write_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            # 1) print to the console
            print(f"\n===== micro-batch {batch_id} =====")
            batch_df.orderBy(col("window.start"), col("hashtag")).show(50, truncate=False)

            # 2) then store in MySQL
            records = batch_df.select(
                date_format(col("window.start"), TS_FORMAT).alias("window_start"),
                date_format(col("window.end"), TS_FORMAT).alias("window_end"),
                col("hashtag"),
                col("tweet_count"),
            ).collect()
            rows = to_upsert_rows(records)
            if not rows or sink == "console":
                return

            import mysql.connector

            conn = mysql.connector.connect(**db_config())
            try:
                cur = conn.cursor()
                cur.executemany(UPSERT_SQL, rows)
                conn.commit()
                print(f"[MySQL] batch {batch_id}: upserted {len(rows)} rows into hashtag_trends")
            finally:
                conn.close()
        finally:
            batch_df.unpersist()

    return write_batch


def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, count, explode, expr, lower, regexp_extract, substring, to_timestamp, window,
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "tweets")
    window_duration = os.getenv("WINDOW_DURATION", "15 minutes")
    watermark = os.getenv("WATERMARK", "15 minutes")
    starting_offsets = os.getenv("STARTING_OFFSETS", "earliest")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "./checkpoints/hashtag_trends")
    sink = os.getenv("SINK", "mysql").lower()

    if sink != "console":
        ensure_table()

    spark = (
        SparkSession.builder.appName("HashtagTrendsToMySQL")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")   # same result on any machine
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )

    tweets = (
        raw.select(
            regexp_extract(col("value"), RE_TWEET_ID, 1).alias("tweet_id"),
            regexp_extract(col("value"), RE_TWEET, 1).alias("tweet"),
            regexp_extract(col("value"), RE_DATE_TIME, 1).alias("date_time"),
            regexp_extract(col("value"), RE_LANGUAGE, 1).alias("language"),
        )
        # first 19 characters = "yyyy-MM-dd HH:mm:ss" (ignores a trailing zone such as " EDT")
        .withColumn("event_time",
                    to_timestamp(substring(col("date_time"), 1, 19), TS_FORMAT))
        .where((col("tweet_id") != "") & col("event_time").isNotNull())
    )

    hashtag_rows = (
        tweets.withWatermark("event_time", watermark)
        .dropDuplicates(["tweet_id", "event_time"])          # producer re-sends rows
        .withColumn("hashtag", explode(expr(HASHTAG_SQL)))   # one row per (tweet, hashtag)
        .withColumn("hashtag", lower(col("hashtag")))
    )

    trends = (
        hashtag_rows.groupBy(window(col("event_time"), window_duration), col("hashtag"))
        .agg(count("*").alias("tweet_count"))
    )

    query = (
        trends.writeStream.outputMode("update")
        .foreachBatch(make_batch_writer(sink))
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
