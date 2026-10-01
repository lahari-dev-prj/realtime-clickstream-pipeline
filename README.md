# Real-Time Clickstream Pipeline

A streaming data pipeline that ingests simulated e-commerce clickstream events,
processes them in near real-time with windowed aggregations, and lands the
results in a queryable analytics table — all running locally with Docker.

## Architecture

```
 Python Producer          Kafka              Spark Structured Streaming        Postgres
 (Faker-generated   -->   topic:       -->   - 5-min tumbling windows    -->   product_activity_5min
  clickstream events)     clickstream-        - watermark for late data
                           events             - event_count / unique_users
                                               per (product_id, event_type)
```

**Why this design:**
- **Kafka** decouples ingestion from processing — the producer doesn't care
  who or what is consuming events, and multiple consumers could subscribe
  to the same topic independently.
- **Spark Structured Streaming** with `foreachBatch` gives exactly-once-ish
  write semantics into Postgres while still expressing the aggregation
  logic declaratively (`groupBy(window(...))` instead of hand-rolled
  state management).
- **Watermarking** (2 minutes) bounds how long the job waits for late
  events before finalizing a window — a deliberate tradeoff between
  result completeness and memory/latency. In a real system this threshold
  would be tuned against observed event lateness.
- **Tumbling 5-minute windows** keep the aggregation state bounded and the
  output granularity useful for a "what's trending right now" dashboard.

## Stack

Kafka (Confluent images) · Spark 3.5 Structured Streaming · PostgreSQL 16 ·
Python (kafka-python, Faker) · Docker Compose

## Running it locally

**Prerequisites:** Docker Desktop, Python 3.10+

```bash
# 1. Start the infrastructure
docker compose up -d

# 2. Install producer dependencies
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Start generating events (runs until Ctrl+C)
python producer/produce_events.py --events-per-sec 20

# 4. In another terminal, submit the Spark streaming job (runs in local[*] mode
#    inside the spark container; production would instead target a standalone
#    or YARN/Kubernetes cluster)
docker exec -it spark /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  /opt/spark-job/stream_processor.py

# 5. Check results landing in Postgres
python analysis/query_results.py
```

**Sample output** (`python analysis/query_results.py`, after letting the
producer + Spark job run for a few minutes):

```
         window_start product_id   event_type  event_count  unique_users
2025-06-01 14:35:00  PROD-00182    page_view           47            31
2025-06-01 14:35:00  PROD-00041  add_to_cart           18            15
2025-06-01 14:35:00  PROD-00182       search           12             9
2025-06-01 14:30:00  PROD-00093    page_view           52            34
2025-06-01 14:30:00  PROD-00041     purchase            6             6
...
```

**UIs while running:**
- Kafka UI: http://localhost:8080
- Spark job UI (while the job is running): http://localhost:4040

## What this demonstrates

- Designing a Kafka topic/partitioning strategy (partitioned by `product_id`
  so all events for a product land on one partition, preserving order)
- Structured Streaming windowed aggregation with watermarking for late data
- Streaming-to-relational sink pattern via `foreachBatch`
- Running a multi-service data stack with Docker Compose
- Schema design for a streaming analytics sink table

## Troubleshooting

**`psycopg2.OperationalError: role "de_user" does not exist`**

This shows up if you already have a *native* Postgres installation running
on your machine (common on macOS if you installed Postgres via Homebrew or
Postgres.app at some point). Both the native instance and Docker's `postgres`
container try to bind to port 5432 on `localhost`/`127.0.0.1`/`::1`, and
whichever claimed the port first wins — so your Python client can silently
connect to the *wrong* Postgres and get a "role does not exist" error because
it's talking to an instance that was never initialized with `de_user`.

Diagnose it with:
```bash
lsof -nP -i :5432 | grep LISTEN
```
If you see two different processes (e.g. a plain `postgres` process alongside
Docker's `com.docke...`) both listening on 5432, that's the conflict.

**Fix:** remap the Docker container to a different host port so it can't
collide, e.g. in `docker-compose.yml`:
```yaml
postgres:
  ports:
    - "5433:5432"   # host:container — only the host side changes
```
Then update the host-side connection in `analysis/query_results.py`
(`port=5433`). No change is needed inside `stream_processor.py` — Spark
connects to Postgres over Docker's internal network (`postgres:5432`), which
is unaffected by how the port is published to the host.

## Possible extensions

- Swap the Postgres sink for a proper OLAP store (ClickHouse / BigQuery)
- Add a Grafana dashboard reading from Postgres
- Add schema registry (Avro/Protobuf) instead of raw JSON
- Add dead-letter handling for malformed events

## License

MIT
