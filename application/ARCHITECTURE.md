# Kafka Microservices: Architecture & Production Mapping

## TL;DR

Two production-ready Python microservices for learning event-driven architecture:

- **Producer**: Generates realistic order events with explicit Kafka config and retry logic.
- **Consumer**: Processes events with manual offset management and lag observability.
- **Design**: At-least-once semantics, graceful shutdown, structured logging, Kubernetes-ready.
- **Scaling**: KEDA autoscales consumer based on Kafka lag.

---

## How This Fits Into Your Kafka + KEDA Project

### Your Current Stage (Milestone 3)

You've completed:
1. ✅ **Architecture Design**: Understand layered approach (Kafka → Consumer Group → Processing).
2. ✅ **Kafka Platform**: Strimzi deployed, topic created, brokers reachable.
3. ✅ **Application Connectivity**: CLI consumers verified connectivity, offsets working.
4. 🟡 **Consumer Lag Investigation**: Measured lag with CLI; need application consumers.

### What These Services Provide (Milestone 4)

**Moving from CLI to Application Consumers:**

| Aspect | CLI Consumer | These Services |
|--------|--------------|-----------------|
| **Observability** | One-shot lag check | Continuous lag tracking via health endpoint |
| **Scalability** | Manual: spin up/down pods | Automatic: KEDA scales 1-10 replicas |
| **Processing Logic** | None (just reads) | Simulated business logic (configurable delay) |
| **Offset Management** | Auto-commit | Manual commit after success (at-least-once) |
| **Logging** | Human-readable | Structured JSON (ELK/Loki integration) |
| **Graceful Shutdown** | N/A | SIGTERM handling, flush on exit |
| **Health Monitoring** | None | `/health` endpoint for K8s probes |

**Progression:**

```
Milestone 3                          Milestone 4                         Milestone 5
──────────────────────────────────────────────────────────────────────────────────────
CLI Consumer                         Application Consumer               Real-Time Monitoring
(kafka-console-consumer)   ─────►    (producer.py/consumer.py)  ─────► (Prometheus + Grafana)
                                     
Lag = manual check                   Lag = health endpoint              Lag = metrics dashboard
                                     
Manual scaling                       KEDA autoscaling                  Predictive scaling
                                     
No business logic                    Simulated processing              Real order processing
```

---

## Architecture: Event-Driven Order Processing

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Your Strimzi Kafka                             │
│  Topic: orders (3 partitions, 2 replicas, KRaft enabled)                │
│  Retention: 24h, Compression: snappy                                     │
└──────────────────────────────────────────────────────────────────────────┘
                              ▲                    │
                              │                    │
                   ┌──────────┘                    └────────┐
                   │                                        │
         (publishes order events)          (consumes order events)
                   │                                        │
                   ▼                                        ▼
    ┌──────────────────────────┐         ┌──────────────────────────┐
    │   Producer Service       │         │  Consumer Service        │
    │   (kafka-producer)       │         │  (kafka-consumer)        │
    │                          │         │                          │
    │ • 1 instance             │         │ • 2-10 instances         │
    │ • Generates events       │         │ • Process events         │
    │ • Rate: 10 msgs/sec      │         │ • Delay: 0.1s            │
    │ • Batching + compression │         │ • Manual commits         │
    │ • Retries on error       │         │ • Lag-based scaling      │
    │ • Health: /health:8080   │         │ • Health: /health:8081   │
    └──────────────────────────┘         └──────────────────────────┘
              │                                     │
              │ Event: {"order_id": "ord_123",     │
              │          "amount": 99.99,          │
              │          "timestamp": "2026-..."}  │
              │                                     ▼
              │                          ┌─────────────────────┐
              │                          │  Business Logic     │
              │                          │  • Validate order   │
              │                          │  • Write to DB      │
              │                          │  • Call payment API │
              │                          │  • Send notification│
              │                          └─────────────────────┘
              │                                     │
              │         ┌───────────────────────────┤
              │         │                           │
              ▼         ▼                           ▼
       ┌──────────┐ ┌──────────┐           ┌──────────────┐
       │ Logs     │ │ Metrics  │           │ Structured   │
       │ (JSON)   │ │ (lag,    │           │ Events/DLQ   │
       │          │ │  latency)│           │              │
       └──────────┘ └──────────┘           └──────────────┘
```

### Data Flow: Single Order Event

```
1. [Producer] Generates order event
   Event: {"event_id": "evt_...", "order_id": "ord_123", "amount": 99.99, ...}
   Key: "ord_123"  (ensures ordering per order)

2. [Kafka] Partitions the event
   Key hash → Partition 0 (example)
   Offset: 1042

3. [Consumer] Polls event
   Partition: 0
   Offset: 1042
   Message: (same as step 1)

4. [Consumer] Processes
   a) Parse JSON
   b) Validate order
   c) Write to database
   d) Wait 0.1s (simulated)

5. [Consumer] Commits offset
   Only after processing succeeds
   Offset 1042 marked as committed

6. [Monitoring]
   Lag = (log-end-offset) - (current-offset)
   E.g., if log-end-offset=1100, current=1042, lag=58

7. [KEDA] Detects lag
   If lag > 100: scale up consumer (2 → 5 replicas)
   If lag < 100: scale down (5 → 2 replicas)

8. [Logs] Structured entry
   {
     "timestamp": "2026-06-25T12:00:00Z",
     "level": "INFO",
     "event_id": "evt_...",
     "order_id": "ord_123",
     "partition": 0,
     "offset": 1042,
     "processing_time_seconds": 0.1
   }
```

---

## Key Design Decisions (Educational Value)

### 1. **Explicit Kafka Configuration**

Why not use a framework?

```python
# ✗ Framework-hidden (Flask-Kafka, Spring Boot, etc.)
@app.route("/produce")
def produce():
    send_message({"data": "..."}  # Where's the config? How does it retry?
    
# ✓ Explicit (confluent-kafka)
producer_config = {
    "bootstrap.servers": "...",
    "acks": "all",                    # Durability choice
    "retries": -1,                    # Retry strategy
    "compression.type": "snappy",     # Performance vs. bandwidth tradeoff
}
producer = Producer(producer_config)
producer.produce(topic=..., key=..., value=..., callback=...)
```

**Why explicit is better:**
- You *understand* durability: `acks='all'` means wait for replicas.
- You *understand* performance: batch.size vs. linger.ms.
- You *understand* semantics: what happens on network failure?
- You can *tune* for your use case (high-volume? low-latency?).

### 2. **At-Least-Once Delivery (Manual Commits)**

Why manual offset management?

```python
# ✗ Auto-commit (framework default)
while True:
    msg = consumer.poll()
    process(msg)  # If this fails, message is lost (framework already committed)

# ✓ Manual commit (explicit)
while True:
    msg = consumer.poll()
    success = process(msg)
    if success:
        consumer.commit()  # Only commit after processing
    # If crash here, message is reprocessed on restart
```

**Semantics:**
- **At-most-once** (auto-commit): Fast, but loses messages on failure.
- **At-least-once** (manual commit): Requires idempotent processing (you see orders twice? OK, deduplicate by order_id).
- **Exactly-once**: Kafka transactions; complex, overkill for most systems.

For order processing: **at-least-once is standard**. Your database deduplicates by order_id.

### 3. **Structured JSON Logging**

Why not just `print()` or `logger.info("message")`?

```python
# ✗ Unstructured
logger.info("Order processed")

# ✓ Structured
logger.info("Order processed", extra={
    "order_id": "ord_123",
    "partition": 0,
    "offset": 1042,
    "processing_time_seconds": 0.1,
})
# Output: {"timestamp": "...", "level": "INFO", "order_id": "ord_123", ...}
```

**Why structured:**
- Searchable: `jq 'select(.order_id=="ord_123")'`
- Alertable: Query `lag > 1000` across all replicas.
- Traceable: Correlate orders → processing → database.
- Integrable: Ship to ELK/Loki/Datadog with `kubectl logs | jq . | forward-to-collector`.

### 4. **Graceful Shutdown (SIGTERM)**

Why does it matter?

```python
# ✗ Ungraceful (process.kill())
# Messages in flight are lost
# Consumer leaves hanging in group (rebalance delay)

# ✓ Graceful (SIGTERM → signal handler)
def signal_handler(sig, frame):
    logger.info("SIGTERM received")
    producer.flush(timeout=30)  # Wait for pending messages
    consumer.commit()            # Finalize offset
    consumer.close()             # Trigger rebalance
    sys.exit(0)
```

**Why Kubernetes cares:**
- Rolling updates need pods to drain cleanly.
- If ungraceful, pod may appear "running" but not processing.
- Graceful shutdown = 0 message loss during deployment.

---

## Real-World Production System

This architecture scales to production order processing:

```
┌─────────────────────────────────────────────────────────────┐
│  Merchant A                    Merchant B                    │
│  ├─ 100 orders/sec             ├─ 10 orders/sec             │
│  └─ Avg: $50                   └─ Avg: $500                 │
└────────────────────────────────────────────────────────────┬┘
                                                               │
                                                               │ REST API
                                                               │ (gRPC)
                                                               ▼
                                           ┌──────────────────────────────┐
                                           │  API Gateway                 │
                                           │  (rate limiting, auth)       │
                                           └────────────┬─────────────────┘
                                                        │
                                                        ▼
                                    ┌───────────────────────────────────┐
                                    │  Kafka Cluster (Strimzi)          │
                                    │  Topic: orders (12 partitions,    │
                                    │         3 replicas, RF=2)         │
                                    │  Retention: 7 days (compliance)   │
                                    └─────┬─────────────────────────────┘
                                          │
                    ┌─────────────────────┼─────────────────────┐
                    │                     │                     │
                    ▼                     ▼                     ▼
            ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
            │  Consumer    │      │  Consumer    │      │  Analytics   │
            │  (Order      │      │  (Payment    │      │  Consumer    │
            │  Processor)  │      │  Processor)  │      │  (Data Lake) │
            │              │      │              │      │              │
            │ 10 replicas  │      │ 5 replicas   │      │ 2 replicas   │
            │ (KEDA scaled)│      │ (KEDA scaled)│      │ (dashboards) │
            └──────┬───────┘      └──────┬───────┘      └──────┬───────┘
                   │                     │                     │
                   ▼                     ▼                     ▼
            [PostgreSQL]        [Payment API]           [Data Warehouse]
             (Transactions)                              (Reporting)
```

**Components:**
- **Producers**: Different merchants/services (each publishes to orders topic).
- **Kafka**: Central event log (immutable, replay-able).
- **Consumer Groups**: 
  - Order Processor: Validates, persists orders.
  - Payment Processor: Charges customer, handles refunds.
  - Analytics: Aggregates for dashboards.
- **Scaling**: KEDA scales each based on lag (order arrivals vs. processing speed).

**Real-World Concerns:**
- **Ordering**: Partition by customer_id to ensure orders per customer are processed in order.
- **Idempotence**: What if payment processor crashes after charging but before confirming? (Answer: make API calls idempotent with request ID).
- **Dead-Letter Queue**: Failed payments → DLQ → manual investigation.
- **Exactly-Once**: Use Kafka transactions (producer + consumer atomic commit).
- **Security**: SASL/TLS for auth; encrypt messages at rest.
- **Compliance**: Audit logs, data retention, encryption, GDPR erasure.

---

## Progression to Production

### Phase 1 (This Implementation)
- ✅ Educational: Understand Kafka semantics explicitly.
- ✅ At-least-once delivery (manual commits).
- ✅ Structured logging.
- ✅ Graceful shutdown.
- ✅ KEDA autoscaling.

### Phase 2 (Next Sprint)
- [ ] Add Prometheus metrics: `kafka_consumer_lag`, `order_processing_latency`.
- [ ] Add database persistence: Write processed orders to PostgreSQL.
- [ ] Add error handling: Retry with exponential backoff, send failures to DLQ.
- [ ] Add tracing: OpenTelemetry spans for order → processing → DB.

### Phase 3 (Later)
- [ ] Exactly-once semantics: Kafka transactions (if required by compliance).
- [ ] Schema registry: Avro schemas for order events (prevent drift).
- [ ] Multi-consumer support: Different processors (payments, shipping, notifications).
- [ ] Disaster recovery: Backup Kafka to S3, replay on failure.
- [ ] Security hardening: SASL/TLS, encryption, secrets rotation.

---

## Testing & Validation Checklist

Before moving to production:

- [ ] **Connectivity**: Producer and consumer reach Kafka broker.
- [ ] **Ordering**: Messages from same producer arrive in order (within partition).
- [ ] **Lag Tracking**: KEDA correctly reports lag; scaling triggers at threshold.
- [ ] **Graceful Shutdown**: Kill pod; verify offset committed, no message loss.
- [ ] **Rebalancing**: Kill consumer; partitions reassigned to others.
- [ ] **Durability**: Kill broker; data survives (RF=2+).
- [ ] **Idempotence**: Reprocess same event twice; same result (no double-charge).
- [ ] **Monitoring**: Health endpoint reports correct metrics.
- [ ] **Load Test**: Can handle peak volume (e.g., 1000 msg/sec).

---

## What You're Learning

By building this yourself (not using a framework):

1. **Kafka's Mental Model**: Partitions, offsets, consumer groups, rebalancing.
2. **Durability Tradeoffs**: acks='all' vs. speed; retries vs. latency.
3. **At-Least-Once Semantics**: What happens when things fail?
4. **Graceful Degradation**: Shutdown, rebalancing, lag management.
5. **Observability**: Structured logging, metrics, health checks.
6. **Kubernetes Integration**: Deployments, health probes, stateless services.
7. **Autoscaling Logic**: KEDA, lag-driven scaling, replica management.

These principles transfer to **any** event-driven system (not just Kafka).

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [confluent-kafka-python API](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Kafka: The Definitive Guide (O'Reilly)](https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/)
- [Event-Driven Architecture (O'Reilly)](https://www.oreilly.com/library/view/building-event-driven-microservices/9781492057321/)
- [Kubernetes Best Practices](https://kubernetes.io/docs/concepts/configuration/overview/)
- [KEDA Kafka Scaler](https://keda.sh/docs/latest/scalers/kafka/)

---

Good luck with Milestone 4! 🚀
