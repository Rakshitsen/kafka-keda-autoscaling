# Validation Guide

This guide helps you prove that the Kafka, Strimzi, KEDA, and observability stack deployed correctly before you start experiments or debug failures.

## 1. Purpose

Use this guide to verify that the platform is healthy end to end:

- Kafka is running in Kubernetes.
- The `orders` topic exists.
- The producer is publishing events.
- The consumer is processing events and committing offsets.
- KEDA is watching lag and the HPA is created.
- Prometheus and Grafana can observe the system.

If any of those pieces are missing, the autoscaling story is incomplete.

## 2. Validation Philosophy

Validation should always move in this order:

```text
Deploy
↓
Verify
↓
Observe
↓
Experiment
```

The point is not to trust the deployment step. The point is to gather evidence that the system behaves the way the architecture says it should.

Good validation answers three questions:

- Is the platform healthy?
- Is application traffic flowing?
- Is the autoscaling loop reacting to lag, not guesswork?

## 3. Validation Matrix

This matrix is the fastest way to check the system without jumping between documents.

| Component | What to Validate | Evidence |
| --- | --- | --- |
| Strimzi | Kafka is managed by the operator | Kafka custom resource reports `Ready` |
| Kafka cluster | Brokers are reachable | Kafka broker pod is running and responsive |
| Kafka topic | `orders` exists with expected shape | Topic is listed with 3 partitions |
| Producer | Events are entering the cluster | Producer metrics are increasing |
| Consumer | Events are being processed | Consumer metrics increase and offsets advance |
| Consumer group | Work is assigned to replicas | Group members and partition assignment are visible |
| Lag | Backlog is measurable | Kafka lag metric is present and non-zero under load |
| KEDA | Scaling trigger is active | `ScaledObject` is `Ready` |
| HPA | Kubernetes scaling object exists | HPA is created for the consumer deployment |
| Prometheus | Metrics are being scraped | Targets are `UP` |
| Grafana | Telemetry is visible | Dashboard loads and shows live series |

## 4. Platform Validation

This section checks the infrastructure layer first, because everything else depends on it.

### Strimzi

- **Question:** Is Strimzi actually managing the Kafka cluster?
- **Evidence:** The Kafka custom resource is `Ready`, and the Strimzi-managed pods are running in the `kafka` namespace.

### Kafka Cluster

- **Question:** Can the cluster accept client traffic?
- **Evidence:** The Kafka broker is reachable through `my-cluster-kafka-bootstrap.kafka.svc:9092`, and the broker pod is healthy.

### Topic

- **Question:** Does the expected workload topic exist?
- **Evidence:** The `orders` topic is present with 3 partitions, matching [platform/strimzi/topic.yaml](platform/strimzi/topic.yaml).

### What this proves

If these checks pass, the platform can store and route events. If they fail, application validation is meaningless because there is no reliable event backbone yet.

## 5. Application Validation

Now validate the producer and consumer behavior on top of the platform.

### Producer

- **Question:** Is work entering Kafka?
- **Evidence:** `kafka_producer_messages_total` is increasing, and the producer logs show successful sends.

### Consumer

- **Question:** Is work leaving Kafka?
- **Evidence:** `kafka_consumer_messages_total` is increasing, offsets are advancing, and structured logs show processed events with partition and offset data.

### Consumer Group

- **Question:** Is the group actually consuming in parallel?
- **Evidence:** The `order-processors` group has active members and partition assignment consistent with the number of available replicas.

### Processing correctness

The consumer in [application/consumer.py](application/consumer.py) is designed for at-least-once delivery:

- It polls Kafka explicitly.
- It processes the message.
- It commits only after successful processing.

That means validation should confirm both throughput and offset progression, not just "the pod is running."

## 6. Autoscaling Validation

This is where the project’s main behavior shows up.

The scaling chain is:

```text
Kafka lag
↓
KEDA Kafka scaler
↓
External metrics
↓
HPA
↓
Consumer replicas
```

### What to verify

- The `ScaledObject` exists and is `Ready`.
- The scaler points at the correct bootstrap server, topic, and consumer group.
- The HPA is created for the consumer deployment.
- Replica count changes when lag changes.

### Repo-specific evidence

The current scaler in [platform/keda/scaledobject.yaml](platform/keda/scaledobject.yaml) uses:

- Namespace: `kafka`
- Consumer group: `order-processors`
- Topic: `orders`
- Lag threshold: `100`
- Max replicas: `10`

That means a valid autoscaling test should create enough backlog to cross the `lagThreshold`, then confirm the consumer deployment scales up.

### Important distinction

KEDA queries Kafka directly for lag. Kafka Exporter is still useful, but it is part of observability, not the scaling decision itself.

## 7. Observability Validation

Observability is what lets you explain the system after it changes.

Validate that you can see:

- Producer metrics
- Consumer metrics
- Kafka lag metrics from Kafka Exporter
- KEDA-related metrics
- HPA-related metrics
- The Grafana dashboard

You want to prove that the telemetry path is working before you rely on it during experiments.

### What this should tell you

- Whether traffic is entering the system
- Whether consumers are keeping up
- Whether lag is building or shrinking
- Whether the autoscaler reacted for the right reason

If metrics are missing, you may still have a working system, but you do not yet have a measurable one.

## 8. End-to-End Validation

This is the one test that matters most.

Run a simple load scenario and confirm the loop:

```text
Producer
↓
Kafka
↓
Consumer
↓
Lag increases
↓
KEDA reacts
↓
HPA creates more replicas
↓
Lag decreases
```

### Expected outcome

- Producer throughput increases.
- Consumer lag becomes visible.
- KEDA recognizes the lag.
- The HPA appears for the consumer deployment.
- Consumer replicas increase.
- Lag starts to fall once extra replicas are active.

### What success means

If that loop works, then the platform is not just deployed. It is behaving correctly.

## 9. Success Checklist

Use this as a final pass before running experiments.

- [ ] Strimzi operator is healthy
- [ ] Kafka cluster is reachable
- [ ] `orders` topic exists
- [ ] Producer is publishing
- [ ] Consumer is processing
- [ ] Consumer offsets are advancing
- [ ] Consumer lag is measurable
- [ ] `ScaledObject` is ready
- [ ] HPA exists for the consumer
- [ ] Prometheus targets are up
- [ ] Grafana dashboard loads
- [ ] End-to-end autoscaling was verified

## Related Docs

| Document | Purpose |
| --- | --- |
| [README.md](../README.md) | Project overview and quick start |
| [docs/architecture.md](architecture.md) | Why the platform is designed this way |
| [docs/deployment-guide.md](deployment-guide.md) | How to deploy the stack |
| [docs/dashboard-guide.md](dashboard-guide.md) | How to interpret the telemetry |
