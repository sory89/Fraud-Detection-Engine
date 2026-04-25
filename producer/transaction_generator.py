"""
Transaction Generator
Generates fake payment transactions → Kafka topic: transactions
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP        = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC                  = os.getenv("TOPIC", "transactions")
INTERVAL_SECONDS       = float(os.getenv("INTERVAL_SECONDS", "1"))

MERCHANTS  = ["Amazon", "Netflix", "Apple", "Uber", "Airbnb",
               "Spotify", "Steam", "PayPal", "Shopify", "Stripe"]
COUNTRIES  = ["FR", "US", "GB", "DE", "ES", "IT", "JP", "CN", "BR", "NG", "RU", "UA"]
CURRENCIES = ["EUR", "USD", "GBP", "JPY", "CHF"]
CARD_TYPES = ["visa", "mastercard", "amex", "discover"]


def generate_transaction() -> dict:
    roll = random.random()

    # ~5% — anomalous high amount
    amount = round(random.uniform(1.0, 9_999.0), 2)
    if roll < 0.05:
        amount = round(random.uniform(10_000.0, 99_999.0), 2)

    # ~3% — missing required fields (schema violation → DLQ)
    if roll < 0.03:
        return {
            "transaction_id": str(uuid.uuid4()),
            "amount":         amount,
            "merchant":       random.choice(MERCHANTS),
            "timestamp":      datetime.now(timezone.utc).isoformat(),
        }

    # ~2% — rapid successive transactions (velocity fraud)
    velocity_flag = roll < 0.07

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id":        f"user_{random.randint(1000, 9999)}",
        "amount":         amount,
        "currency":       random.choice(CURRENCIES),
        "merchant":       random.choice(MERCHANTS),
        "country":        random.choice(COUNTRIES),
        "card_type":      random.choice(CARD_TYPES),
        "card_last4":     str(random.randint(1000, 9999)),
        "ip_address":     (
            f"185.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"
            if roll < 0.08 else
            f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"
        ),
        "velocity_flag":  velocity_flag,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


def build_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=50,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return p
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 3s…")
            time.sleep(3)


def main():
    producer = build_producer()
    log.info("Transaction generator started → topic=%s", TOPIC)

    while True:
        tx = generate_transaction()
        producer.send(TOPIC, key=tx["transaction_id"], value=tx)
        log.info("→ tx=%s amount=%.2f country=%s",
                 tx["transaction_id"][:8],
                 tx.get("amount", 0),
                 tx.get("country", "?"))
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
