"""
spark_streaming_job.py
PySpark Structured Streaming — Transaction Validation Pipeline

Flow:
  Kafka topic: transactions
    → 5 validasi
    → valid   → Kafka topic: transactions_valid
    → invalid → Kafka topic: transactions_dlq
    → Tumbling window (1 menit) → console output
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER        = "kafka:29092"          # internal Docker network
INPUT_TOPIC         = "transactions"
VALID_TOPIC         = "transactions_valid"
DLQ_TOPIC           = "transactions_dlq"
WATERMARK_DELAY     = "3 minutes"
WINDOW_SIZE         = "1 minute"
CHECKPOINT_BASE     = "/tmp/checkpoints"

# ─────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TransactionStreamingJob")
    .config("spark.sql.shuffle.partitions", "2")   # ringan untuk dev
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session started")

# ─────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────
transaction_schema = StructType([
    StructField("user_id",   StringType(),  True),
    StructField("amount",    LongType(),    True),
    StructField("timestamp", StringType(),  True),   # raw string dulu
    StructField("source",    StringType(),  True),
])

VALID_SOURCES = ["mobile", "web", "pos"]

# ─────────────────────────────────────────
# 1. READ FROM KAFKA
# ─────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Deserialize JSON → DataFrame dengan schema terdefinisi
parsed_df = (
    raw_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            transaction_schema
        ).alias("data"),
        F.col("timestamp").alias("kafka_ingest_time")   # waktu Kafka nerima
    )
    .select("data.*", "kafka_ingest_time")
)

# ─────────────────────────────────────────
# 2. PARSE TIMESTAMP & APPLY WATERMARK
# ─────────────────────────────────────────
# Cast timestamp string → TimestampType (null jika format salah)
with_ts_df = parsed_df.withColumn(
    "event_time",
    F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

# Apply watermark pada event_time untuk handle late events
watermarked_df = with_ts_df.withWatermark("event_time", WATERMARK_DELAY)

# ─────────────────────────────────────────
# 3. DUPLICATE DETECTION
#    Pakai dropDuplicatesWithinWatermark (user_id + timestamp)
# ─────────────────────────────────────────
dedup_df = watermarked_df.dropDuplicatesWithinWatermark(["user_id", "timestamp"])

# ─────────────────────────────────────────
# 4. 5 VALIDASI WAJIB
# ─────────────────────────────────────────

validated_df = dedup_df.withColumn(
    "validation_errors",
    F.array(
        # Validasi 1: Mandatory field check
        F.when(
            F.col("user_id").isNull() | (F.col("user_id") == "") |
            F.col("amount").isNull() |
            F.col("timestamp").isNull() | (F.col("timestamp") == ""),
            F.lit("MISSING_MANDATORY_FIELD")
        ),
        # Validasi 2: Type validation — amount harus numeric (LongType), timestamp parse-able
        F.when(
            F.col("event_time").isNull(),
            F.lit("INVALID_TIMESTAMP_FORMAT")
        ),
        # Validasi 3: Range validation — amount harus 1 s/d 10.000.000
        F.when(
            (F.col("amount") < 1) | (F.col("amount") > 10_000_000),
            F.lit("AMOUNT_OUT_OF_RANGE")
        ),
        # Validasi 4: Source validation
        F.when(
            ~F.col("source").isin(VALID_SOURCES),
            F.lit("INVALID_SOURCE")
        ),
        # Validasi 5: Late event — event_time lebih dari watermark (null setelah dedup/watermark)
        #   Event yang sudah expired watermark akan otomatis di-drop oleh Spark,
        #   tapi kita tandai yang mendekati batas juga
        F.when(
            F.col("event_time").isNull() & F.col("timestamp").isNotNull(),
            F.lit("LATE_EVENT_EXPIRED")
        ),
    )
).withColumn(
    # Hapus null dari array error
    "validation_errors",
    F.array_compact(F.col("validation_errors"))
).withColumn(
    "is_valid",
    F.size(F.col("validation_errors")) == 0
).withColumn(
    "error_reason",
    F.when(
        F.size(F.col("validation_errors")) > 0,
        F.concat_ws(" | ", F.col("validation_errors"))
    ).otherwise(F.lit(None).cast(StringType()))
)

# ─────────────────────────────────────────
# 5. ROUTING: valid vs DLQ
# ─────────────────────────────────────────
valid_df   = validated_df.filter(F.col("is_valid") == True)
invalid_df = validated_df.filter(F.col("is_valid") == False)


def to_kafka_value(df):
    """Serialize seluruh row kembali ke JSON string untuk dikirim ke Kafka."""
    return df.select(
        F.to_json(F.struct(
            "user_id", "amount", "timestamp", "source",
            "is_valid", "error_reason"
        )).alias("value")
    )


# ── Write VALID → transactions_valid ────
valid_query = (
    to_kafka_value(valid_df)
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", VALID_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid")
    .outputMode("append")
    .start()
)

# ── Write INVALID → transactions_dlq ────
dlq_query = (
    to_kafka_value(invalid_df)
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", DLQ_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/dlq")
    .outputMode("append")
    .start()
)

# ─────────────────────────────────────────
# 6. TUMBLING WINDOW MONITORING (1 menit)
#    Hitung total transaksi valid per window
# ─────────────────────────────────────────
window_df = (
    valid_df
    .groupBy(
        F.window(F.col("event_time"), WINDOW_SIZE)
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count"),
        F.col("total_amount")
    )
)

# ─────────────────────────────────────────
# 7. OUTPUT DATA — running_total ke console
#    Kolom: timestamp (waktu Spark output) + running_total
# ─────────────────────────────────────────
def write_window_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # Hitung running total (cumulative sum dalam batch)
    from pyspark.sql.window import Window
    window_spec = Window.orderBy("window_start").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    result_df = batch_df.withColumn(
        "running_total", F.sum("transaction_count").over(window_spec)
    ).withColumn(
        "timestamp", F.current_timestamp()
    ).select("timestamp", "running_total", "window_start", "window_end", "transaction_count", "total_amount")

    print(f"\n{'='*70}")
    print(f"  📊 WINDOW MONITORING — Batch ID: {batch_id}")
    print(f"{'='*70}")
    result_df.show(truncate=False)


window_query = (
    window_df
    .writeStream
    .foreachBatch(write_window_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/window")
    .outputMode("complete")
    .trigger(processingTime="30 seconds")
    .start()
)

# ─────────────────────────────────────────
# 8. AWAIT TERMINATION
# ─────────────────────────────────────────
print("\n🚀 Streaming job running...")
print(f"   Input  : {INPUT_TOPIC}")
print(f"   Valid  → {VALID_TOPIC}")
print(f"   Invalid→ {DLQ_TOPIC}")
print(f"   Watermark: {WATERMARK_DELAY} | Window: {WINDOW_SIZE}")
print("   Press Ctrl+C to stop.\n")

spark.streams.awaitAnyTermination()
