"""
producer.py
Kafka Event Producer - Transaction Events
Mengirim event valid, invalid, dan late events ke topic 'transactions'
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC = "transactions"

VALID_SOURCES = ["mobile", "web", "pos"]
INVALID_SOURCES = ["atm", "unknown", "fax", ""]  # source tidak dikenal

# ─────────────────────────────────────────
# PRODUCER SETUP
# ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def past_iso(minutes: int) -> str:
    """Generate timestamp di masa lalu untuk simulasi late events."""
    t = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────
# EVENT GENERATORS
# ─────────────────────────────────────────

def make_valid_event() -> dict:
    return {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randint(1, 10_000_000),
        "timestamp": now_iso(),
        "source": random.choice(VALID_SOURCES),
    }


def make_invalid_events() -> list[dict]:
    """Minimal 3 event invalid sesuai requirement."""
    duplicate_base = {
        "user_id": "U99999",
        "amount": 50000,
        "timestamp": "2025-12-14T09:00:20Z",
        "source": "mobile",
    }
    return [
        # 1. Amount negatif
        {
            "user_id": "U11111",
            "amount": -500,
            "timestamp": now_iso(),
            "source": "mobile",
        },
        # 2. Amount terlalu besar (> 10.000.000)
        {
            "user_id": "U22222",
            "amount": 99_999_999,
            "timestamp": now_iso(),
            "source": "web",
        },
        # 3. Timestamp tidak valid
        {
            "user_id": "U33333",
            "amount": 75000,
            "timestamp": "not-a-valid-timestamp",
            "source": "mobile",
        },
        # 4. Source tidak dikenal
        {
            "user_id": "U44444",
            "amount": 200000,
            "timestamp": now_iso(),
            "source": "fax",
        },
        # 5. Duplicate event (user_id + timestamp sama)
        {**duplicate_base},
        {**duplicate_base},  # duplikat persis
    ]


def make_late_events() -> list[dict]:
    """Minimal 3 late events — timestamp > 3 menit ke belakang (melewati watermark)."""
    return [
        {
            "user_id": f"U{random.randint(10000, 99999)}",
            "amount": random.randint(1, 500_000),
            "timestamp": past_iso(minutes=5),   # 5 menit lalu → lewat watermark 3 menit
            "source": random.choice(VALID_SOURCES),
        },
        {
            "user_id": f"U{random.randint(10000, 99999)}",
            "amount": random.randint(1, 500_000),
            "timestamp": past_iso(minutes=10),  # 10 menit lalu
            "source": random.choice(VALID_SOURCES),
        },
        {
            "user_id": f"U{random.randint(10000, 99999)}",
            "amount": random.randint(1, 500_000),
            "timestamp": past_iso(minutes=7),   # 7 menit lalu
            "source": "pos",
        },
    ]


# ─────────────────────────────────────────
# SEND HELPERS
# ─────────────────────────────────────────

def send_event(event: dict, label: str = "EVENT"):
    producer.send(TOPIC, value=event)
    print(f"[{label}] Sent → {json.dumps(event)}")


# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def main():
    print(f"🚀 Producer started — publishing to topic: {TOPIC}")
    print(f"   Broker: {KAFKA_BROKER}\n")

    # 1. Kirim batch invalid events di awal
    print("── Sending INVALID events ──────────────────")
    for ev in make_invalid_events():
        send_event(ev, label="INVALID")
        time.sleep(0.5)

    # 2. Kirim batch late events
    print("\n── Sending LATE events (watermark test) ────")
    for ev in make_late_events():
        send_event(ev, label="LATE")
        time.sleep(0.5)

    # 3. Loop: kirim valid events setiap 1-2 detik
    print("\n── Sending VALID events (continuous) ───────")
    count = 0
    try:
        while True:
            ev = make_valid_event()
            send_event(ev, label="VALID")
            count += 1
            delay = random.uniform(1, 2)
            time.sleep(delay)
    except KeyboardInterrupt:
        print(f"\n⛔ Producer stopped. Total valid events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
