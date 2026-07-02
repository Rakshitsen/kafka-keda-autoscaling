# Kafka Producer & Consumer Microservices

Production-quality educational microservices for learning event-driven architecture with Kafka on Kubernetes.

## Overview

Two stateless Python services demonstrate real-world Kafka patterns:

- **Producer**: Generates realistic order events at a configurable rate with explicit configuration and retry logic.
- **Consumer**: Consumes and processes events with manual offset management, structured logging, and lag observability.

Both are designed for deployment on Kubernetes with KEDA autoscaling based on consumer lag.

---

## Architecture & Design Rationale

### Why These Design Choices?

#### 1. **Explicit Kafka Configuration** (Not Framework Magic)
All Kafka configuration is visible in code with comments explaining *why* each setting matters:

**Producer Configuration:**
- `acks='all'`: Durability over speed. Critical for financial/order data; wait for leader + replicas.
- `retries=-1` + `max.in.flight.requests.per.connection=5`: Automatic retry with ordering guarantees.
- `batch.size` + `linger.ms`: Trade latency for throughput without excessive buffering.
- `compression.type='snappy'`: Balance CPU vs. network (LZ4 faster, gzip better ratio).

**Consumer Configuration:**
- `enable.auto.commit=false`: Manual offset management ensures we only commit *after* processing succeeds.
  - Prevents silent data loss if processing fails mid-transaction.
  - Consumer is responsible for explicit commit (not framework-managed).
- `auto.offset.reset='earliest'`: Start from beginning if group is new (good for testing/replay).
- `session.timeout.ms + heartbeat.interval.ms`: Rebalance if consumer hangs (5-6s detection).
- `max.poll.interval.ms=300s`: Allow time for processing; set high if business logic is slow.
- `isolation.level='read_committed'`: Only read committed messages (prevents reading uncommitted writes).

#### 2. **At-Least-Once Semantics**
The consumer loop is explicit and shows exactly when data is lost:

```
1. Poll message
2. Process (may fail, may succeed)
3. Commit offset ONLY if success
```

If the consumer crashes after processing but before commit, it will reprocess on restart. This is *at-least-once* delivery: your business logic must be idempotent.

#### 3. **Structured JSON Logging**
Every log is valid JSON with consistent fields:

```json
{
  "timestamp": "2026-06-25T12:00:00Z",
  "level": "INFO",
  "logger": "kafka-consumer",
  "message": "Event processed",
  "event_id": "evt_1234567890_1234",
  "order_id": "ord_123456",
  "partition": 0,
  "offset": 42,
  "processing_time_seconds": 0.1
}
```

This integrates with ELK/Loki/Datadog for alerting on lag, errors, and SLOs.

#### 4. **Graceful Shutdown**
Both services handle `SIGTERM` (Kubernetes sends this on pod termination):

**Producer:**
- Stops accepting new messages.
- Calls `flush(timeout=30)` to wait for pending messages to be delivered.
- Exits cleanly; no message loss.

**Consumer:**
- Stops polling.
- Commits current offset if processing succeeded.
- Closes consumer; triggers rebalance to redistribute partitions.
- Exit; partitions reassigned to other replicas.

#### 5. **Health Endpoints**
Each service exposes `/health` for Kubernetes liveness/readiness probes:

**Producer `/health`:**
```json
{
  "status": "healthy",
  "messages_sent": 1000,
  "messages_failed": 0
}
```

**Consumer `/health`:**
```json
{
  "status": "healthy",
  "messages_processed": 1000,
  "messages_failed": 0,
  "current_lag_estimate": 5
}
```

This allows Kubernetes to:
- Detect dead services and restart them.
- Keep track of throughput and errors for debugging.
- Base autoscaling decisions on lag (via KEDA).

---

## Quick Start (Local Development)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- `curl` (for health checks)

### Run Locally

```bash
# Start Kafka, producer, and consumer
docker-compose up -d

# Check logs
docker-compose logs -f producer
docker-compose logs -f consumer

# Health check (should return JSON with "healthy" status)
curl http://localhost:8080/health  # Producer
curl http://localhost:8081/health  # Consumer

# View Kafka topics
docker exec kafka-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Monitor consumer lag
docker exec kafka-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-processors

# Stop all
docker-compose down
```

### Monitor with Kafka UI

Open `http://localhost:8888` in your browser. You'll see:
- Producer sending messages (messages appear in topic).
- Consumer group progress (current offset, lag).
- Partition distribution.

---

## Deployment to Kubernetes

### Prerequisites
- Kubernetes cluster with Kafka (Strimzi) already deployed.
- KEDA installed (for autoscaling consumer).

### 1. Build Docker Images

```bash
# Producer
docker build -t your-registry/kafka-producer:latest \
  --build-arg SERVICE=producer .
docker push your-registry/kafka-producer:latest

# Consumer
docker build -t your-registry/kafka-consumer:latest \
  --build-arg SERVICE=consumer .
docker push your-registry/kafka-consumer:latest
```

### 2. Update Manifests

In `kubernetes-deployment.yaml`:
- Update `KAFKA_BOOTSTRAP_SERVERS` to match your Kafka bootstrap address (e.g., `my-cluster-kafka-bootstrap.kafka.svc:9092`).
- Update image references to your registry.
- Adjust `MESSAGE_RATE_PER_SEC` and `PROCESSING_DELAY_SECONDS` as needed.

### 3. Deploy

```bash
# Create namespace and deployments
kubectl apply -f kubernetes-deployment.yaml

# Watch rollout
kubectl rollout status -n kafka-apps deployment/kafka-producer
kubectl rollout status -n kafka-apps deployment/kafka-consumer

# Check running pods
kubectl get pods -n kafka-apps

# View logs
kubectl logs -n kafka-apps -f deployment/kafka-producer
kubectl logs -n kafka-apps -f deployment/kafka-consumer

# Check lag (from inside cluster)
kubectl -n kafka run kafka-group -ti --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
  bin/kafka-consumer-groups.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc:9092 \
  --describe --group order-processors
```

### 4. Autoscaling with KEDA

The `ScaledObject` in the manifest scales the consumer based on Kafka lag:

```yaml
offsetLagTarget: "100"  # Scale up if lag > 100 messages per partition
minReplicaCount: 1
maxReplicaCount: 10
```

When producer rate increases (or processing slows), KEDA detects lag and scales up the consumer. When lag decreases, it scales down.

---

## Environment Variables

### Producer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address. |
| `KAFKA_TOPIC` | `orders` | Topic to produce to. |
| `MESSAGE_RATE_PER_SEC` | `10` | Messages per second. |
| `KAFKA_PRODUCER_BATCH_SIZE` | `1000` | Batch size in bytes. |
| `KAFKA_PRODUCER_LINGER_MS` | `100` | Wait time before sending batch. |
| `KAFKA_COMPRESSION_TYPE` | `snappy` | `snappy`, `gzip`, `lz4`, `zstd`, or `none`. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `HTTP_PORT` | `8080` | Health endpoint port. |

### Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address. |
| `KAFKA_TOPIC` | `orders` | Topic to consume from. |
| `KAFKA_CONSUMER_GROUP` | `order-processors` | Consumer group ID. |
| `PROCESSING_DELAY_SECONDS` | `0.1` | Simulated processing time (seconds). |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `HTTP_PORT` | `8081` | Health endpoint port. |

---

## Real-World Production Architecture

These services map to a real event-driven system:

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Kafka Cluster (Strimzi)                       │
│                  Topic: orders (3 partitions, 2 RF)                 │
└─────────────────────────────────────────────────────────────────────┘
                              ▲              │
                              │              │
                    (produces)│              │(consumes)
                              │              ▼
┌──────────────────────┐                  ┌──────────────────────┐
│  Producer Service    │                  │ Consumer Service     │
│  (1 replica)         │                  │ (2-10 replicas)      │
│                      │                  │                      │
│ - Generates events   │                  │ - Processes events   │
│ - Rate: 10 msg/s     │                  │ - Delay: 0.1s        │
│ - Retries & durability                  │ - Lag-based scaling  │
│ - /health endpoint   │                  │ - /health endpoint   │
└──────────────────────┘                  └──────────────────────┘
         │                                        │
         └────────────┬─────────────────────────┘
                      │
           ┌──────────▼──────────┐
           │  Structured Logs    │
           │  (JSON format)      │
           └─────────┬───────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
   ┌────▼───┐             ┌──────▼──────┐
   │   ELK  │             │  Alerting   │
   │  Stack │             │  (lag > 1k) │
   └────────┘             └─────────────┘
```

### Components:

1. **Kafka Cluster**: Stores order events with replication and partitioning.
2. **Producer**: Continuously generates realistic order events.
3. **Consumer Group**: Multiple instances process events in parallel.
   - Kafka assigns partitions to each consumer.
   - Each partition is processed by exactly one consumer (strong ordering per partition).
   - If consumer dies, its partitions are reassigned (rebalance).
4. **Logging**: Structured JSON logs shipped to ELK/Loki for alerts (lag, errors, latency).
5. **Autoscaling**: KEDA scales consumers based on lag (`offsetLagTarget`).

### Failure Scenarios & Recovery:

| Failure | Impact | Recovery |
|---------|--------|----------|
| Producer dies | No new messages | Restart; resume from where it left off. |
| Consumer dies | Lag increases | Rebalance; other replicas pick up partitions. |
| Broker dies (RF=2) | No impact | Failover to other replicas; no data loss. |
| Network partition | Lag increases | Heals; catch-up begins. |
| Consumer slow | Lag increases | KEDA scales up replicas; distribute load. |

---

## Code Structure

```
.
├── producer.py              # Producer service (explicit Kafka config, retries)
├── consumer.py              # Consumer service (manual offset management)
├── requirements.txt         # Python dependencies
├── Dockerfile               # Multi-stage, non-root user, production-ready
├── docker-compose.yml       # Local dev environment
├── kubernetes-deployment.yaml # K8s manifests + KEDA scaler
└── README.md               # This file
```

### Key Files & Sections:

**producer.py:**
- `KafkaProducerService`: Explicit configuration, retries, batch management.
- `generate_order_event()`: Realistic event generation.
- `delivery_callback()`: Tracks delivery success/failure.
- `produce_events()`: Main loop with backoff.
- Flask `/health` endpoint for Kubernetes.

**consumer.py:**
- `KafkaConsumerService`: Explicit configuration, manual offset management.
- `process_event()`: Simulates business logic (DB write, API call).
- `run()`: Main consumer loop showing at-least-once semantics.
- Flask `/health` endpoint with lag estimate.

---

## Debugging & Observability

### 1. Check Consumer Lag (from Kubernetes)

```bash
kubectl -n kafka run kafka-group -ti \
  --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
  bin/kafka-consumer-groups.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc:9092 \
  --describe --group order-processors
```

Output shows:
- `PARTITION`: Which partition.
- `CURRENT-OFFSET`: Where consumer is.
- `LOG-END-OFFSET`: Where topic ends.
- `LAG`: Difference (how far behind).

### 2. Stream Logs with JSON Parsing

```bash
# Real-time logs from consumer
kubectl logs -n kafka-apps -f deployment/kafka-consumer | \
  jq 'select(.level=="ERROR") | {timestamp, message, event_id}'
```

Filter by event, partition, lag, etc.

### 3. Health Endpoint Monitoring

```bash
watch -n 1 'curl -s http://localhost:8081/health | jq .'
```

Monitor processing rate and error count.

### 4. Kafka CLI: Topic Details

```bash
# Describe topic
kafka-topics --bootstrap-server kafka:9092 --topic orders --describe

# Measure throughput
kafka-consumer-perf-test --bootstrap-server kafka:9092 --topic orders \
  --messages 10000 --threads 1 | tail -5
```

---

## Testing & Tuning

### 1. Simulate Slow Processing

Increase `PROCESSING_DELAY_SECONDS` to simulate slow business logic:

```bash
kubectl set env deployment/kafka-consumer \
  PROCESSING_DELAY_SECONDS="5" -n kafka-apps
```

Watch lag increase; KEDA scales up the consumer.

### 2. Simulate Slow Producer

Decrease `MESSAGE_RATE_PER_SEC`:

```bash
kubectl set env deployment/kafka-producer \
  MESSAGE_RATE_PER_SEC="1" -n kafka-apps
```

Lag decreases; KEDA scales down.

### 3. Monitor Rebalancing

Kill a consumer pod:

```bash
kubectl delete pod -n kafka-apps -l app=kafka-consumer
```

Watch logs:
- Consumer detects rebalance.
- Partitions are revoked.
- Other consumers pick up the work.
- Service recovers.

---

## Known Limitations & Future Improvements

### Current:
- ✅ At-least-once delivery (idempotent processing required).
- ✅ Manual offset management (explicit, not hidden).
- ✅ Graceful shutdown (SIGTERM/SIGINT).
- ✅ Structured JSON logging.
- ✅ Kubernetes-ready (health, resource limits, probes).

### Future (Production):
1. **Dead-Letter Queue (DLQ)**: Send processing failures to a separate topic for investigation.
2. **Exactly-Once Semantics**: Use Kafka transactions + idempotent producer (more complex, overkill for orders).
3. **Prometheus Metrics**: Expose `/metrics` endpoint with processing latency, lag, throughput.
4. **Distributed Tracing**: OpenTelemetry spans for order → processing → database.
5. **Schema Registry**: Validate event format with Avro/Protobuf (prevent schema drift).
6. **Partitioning Strategy**: Partition by customer_id or order_id for ordering guarantees per customer.

---

## References

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [confluent-kafka-python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
- [KEDA Kafka Scaler](https://keda.sh/docs/latest/scalers/kafka/)
- [Event-Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html)

---

## License

Educational. Use as reference for production systems.
