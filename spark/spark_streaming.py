"""
Spark Structured Streaming — Fraud Detection Pipeline
Kafka (transactions) → parse → fraud rules → PostgreSQL

Tables:
  - transactions      : all valid transactions
  - fraud_alerts      : flagged transactions
  - dlq_messages      : schema-invalid messages
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [spark] %(message)s")
log = logging.getLogger("spark")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
TOPIC           = os.getenv("TOPIC",            "transactions")
PG_URL          = os.getenv("PG_URL",           "jdbc:postgresql://postgres:5432/fraud")
PG_USER         = os.getenv("PG_USER",          "fraud")
PG_PASSWORD     = os.getenv("PG_PASSWORD",      "fraud")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR",   "/tmp/spark-checkpoints")

PG_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Transaction schema ────────────────────────────────────────────────────────
TX_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("merchant",       StringType(),  True),
    StructField("country",        StringType(),  True),
    StructField("card_type",      StringType(),  True),
    StructField("card_last4",     StringType(),  True),
    StructField("ip_address",     StringType(),  True),
    StructField("velocity_flag",  BooleanType(), True),
    StructField("timestamp",      StringType(),  True),
])

# ── Fraud rules (defined inside functions — F.col requires active SparkSession) ─
HIGH_RISK_COUNTRIES = ["NG", "CN", "RU", "UA"]


def get_fraud_rules() -> dict:
    return {
        "HIGH_AMOUNT":       F.col("amount") > 9_000,
        "HIGH_RISK_COUNTRY": F.col("country").isin(HIGH_RISK_COUNTRIES) & (F.col("amount") > 500),
        "SUSPICIOUS_IP":     F.col("ip_address").startswith("185."),
        "AMEX_LARGE":        (F.col("card_type") == "amex") & (F.col("amount") > 5_000),
        "VELOCITY_FRAUD":    F.col("velocity_flag") == True,
    }


def build_alert_reason(df):
    """Add fraud_reason column: first matching rule label."""
    rules = get_fraud_rules()
    reason_col = F.lit(None).cast(StringType())
    for label, condition in reversed(list(rules.items())):
        reason_col = F.when(condition, F.lit(label)).otherwise(reason_col)
    return df.withColumn("fraud_reason", reason_col)


def is_fraud(df):
    rules = get_fraud_rules()
    combined = None
    for condition in rules.values():
        combined = condition if combined is None else combined | condition
    return combined


# ── Write helper ──────────────────────────────────────────────────────────────
def write_batch(batch_df, batch_id, table: str, cols: list):
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    log.info("Batch %d → %s (%d rows)", batch_id, table, count)
    (
        batch_df.select(cols)
        .write
        .jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — reading from %s", TOPIC)

    # ── Read raw Kafka stream ─────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ────────────────────────────────────────────────────────────
    parsed = (
        raw.select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.from_json(F.col("value").cast("string"), TX_SCHEMA).alias("tx")
        )
        .select("kafka_offset", "kafka_partition", "tx.*")
        .withColumn("timestamp",   F.to_timestamp("timestamp"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # ── Split: valid vs DLQ (missing transaction_id or user_id) ──────────────
    valid  = parsed.filter(
        F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull()
    )
    invalid = parsed.filter(
        F.col("transaction_id").isNull() | F.col("user_id").isNull()
    )

    # ── Apply fraud rules ─────────────────────────────────────────────────────
    enriched = build_alert_reason(valid)
    fraud_df  = enriched.filter(is_fraud(enriched))
    clean_df  = enriched.filter(~is_fraud(enriched))

    # ── Sink 1: all valid transactions ────────────────────────────────────────
    tx_cols = [
        "transaction_id", "user_id", "amount", "currency", "merchant",
        "country", "card_type", "card_last4", "ip_address",
        "velocity_flag", "timestamp", "ingested_at",
    ]
    tx_query = (
        valid.writeStream
        .foreachBatch(lambda df, bid: write_batch(df, bid, "transactions", tx_cols))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/transactions")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Sink 2: fraud alerts ──────────────────────────────────────────────────
    fraud_cols = [
        "transaction_id", "user_id", "amount", "currency", "merchant",
        "country", "card_type", "card_last4", "ip_address",
        "fraud_reason", "timestamp", "ingested_at",
    ]
    fraud_query = (
        fraud_df.writeStream
        .foreachBatch(lambda df, bid: write_batch(df, bid, "fraud_alerts", fraud_cols))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/fraud")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Sink 3: DLQ ──────────────────────────────────────────────────────────
    dlq_cols = ["kafka_offset", "kafka_partition", "ingested_at"]

    def write_dlq(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        dlq = batch_df.withColumn(
            "raw_payload",
            F.col("transaction_id").cast("string")
        ).withColumn("error_type", F.lit("SCHEMA_VIOLATION"))
        write_batch(dlq, batch_id, "dlq_messages",
                    ["kafka_offset", "kafka_partition", "error_type", "ingested_at"])

    dlq_query = (
        invalid.writeStream
        .foreachBatch(write_dlq)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("All streaming queries started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
