"""
Simulated e-commerce clickstream event producer.

Generates realistic user events (page_view, add_to_cart, purchase, etc.)
and publishes them to a Kafka topic as JSON, at a configurable rate.

Usage:
    python produce_events.py --events-per-sec 20 --duration-sec 0  # 0 = run forever
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase", "search"]
# Weighted so page_view is most common, purchase is rare -- mirrors real funnels
EVENT_WEIGHTS = [0.55, 0.20, 0.08, 0.07, 0.10]

CATEGORIES = ["electronics", "home", "fashion", "sports", "books", "toys"]

# Small fixed product catalog so aggregations by product_id are meaningful
PRODUCT_CATALOG = [
    {"product_id": f"P{str(i).zfill(4)}", "category": random.choice(CATEGORIES)}
    for i in range(1, 201)
]

# Simulate a pool of returning users so "unique_users" aggregations make sense
USER_POOL = [str(uuid.uuid4()) for _ in range(500)]


def build_event() -> dict:
    product = random.choice(PRODUCT_CATALOG)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": random.choice(USER_POOL),
        "session_id": str(uuid.uuid4()),
        "product_id": product["product_id"],
        "category": product["category"],
        "price": round(random.uniform(5, 500), 2),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "country": fake.country_code(),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if event_type == "purchase":
        event["quantity"] = random.randint(1, 3)

    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="clickstream-events")
    parser.add_argument("--events-per-sec", type=float, default=10.0)
    parser.add_argument("--duration-sec", type=int, default=0, help="0 = run until Ctrl+C")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,
    )

    print(f"Producing to topic '{args.topic}' at ~{args.events_per_sec} events/sec. Ctrl+C to stop.")

    sleep_interval = 1.0 / args.events_per_sec
    start_time = time.time()
    sent = 0

    try:
        while True:
            event = build_event()
            # Partition by product_id so all events for a product land on the same partition
            producer.send(args.topic, key=event["product_id"], value=event)
            sent += 1

            if sent % 100 == 0:
                print(f"[{sent} events sent]")

            if args.duration_sec and (time.time() - start_time) >= args.duration_sec:
                break

            time.sleep(sleep_interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. Total events sent: {sent}")


if __name__ == "__main__":
    main()
