# Architecture

This project demonstrates an event-driven platform on Kubernetes where Kafka consumer workloads scale automatically based on consumer lag.

The architecture intentionally separates infrastructure, application logic, autoscaling, and observability so each layer can be understood independently.

## Requirements / Constraints

The architecture is optimized to:

- Autoscale consumers based on Kafka lag.
- Preserve at-least-once delivery.
- Explain every scaling decision through telemetry.
- Keep the platform Kubernetes-native.
- Keep the example small enough to reason about end to end.

## High-Level Architecture

```text
Producer ---> Kafka ---> Consumer
                  |
                  +----> Kafka Exporter
                               |
Producer Metrics -------------->|
Consumer Metrics -------------->|
KEDA Metrics ------------------->| Prometheus ---> Grafana
HPA Metrics -------------------->|
```

The producer publishes order events to Kafka. Kafka stores the events durably and distributes them across partitions. A consumer group reads the topic in parallel.

Observability is shown separately because it serves a different purpose from the data path:

- Kafka Exporter exposes lag and topic metrics.
- Prometheus collects producer, consumer, exporter, KEDA, and HPA metrics.
- Grafana visualizes those signals so scaling decisions can be explained.

## Architecture Layers

### Platform Layer

Purpose: provide the event streaming platform.

Components:

- Kubernetes
- Strimzi
- Kafka

Responsibilities:

- Cluster management
- Topic management
- Persistent storage

### Application Layer

Purpose: generate and process events.

Components:

- Producer
- Consumer

Responsibilities:

- Publish orders
- Consume orders
- Commit offsets after successful processing

### Autoscaling Layer

Purpose: adjust consumer capacity.

Components:

- KEDA
- HPA

Responsibilities:

- Observe lag
- Convert lag into external metrics
- Scale the consumer deployment

### Observability Layer

Purpose: explain system behavior.

Components:

- Producer metrics
- Consumer metrics
- Kafka Exporter
- Prometheus
- Grafana

Responsibilities:

- Metrics collection
- Dashboarding
- Troubleshooting

## Data Flow

```text
Producer

↓

Kafka Topic

↓

Partition

↓

Consumer Group

↓

Consumer Pod

↓

Offset Commit
```

Event flow in this system:

- The producer creates a new order event.
- Kafka appends the event to a topic partition.
- Kafka assigns partitions to consumers in the group.
- A consumer receives the event.
- Business logic executes.
- The consumer commits the offset only after processing succeeds.
- Metrics are updated so the system state is visible.

### Request Lifecycle

```text
Producer creates order
↓
Kafka stores event
↓
Consumer receives event
↓
Business logic executes
↓
Offset committed
↓
Metrics updated
```

That final step is important because it gives the system at-least-once delivery semantics. If a consumer crashes after processing but before the commit, the same event may be processed again after restart.

## Scaling Flow

```text
Producer Rate ↑

↓

Consumer Lag ↑

↓

Lag

├── KEDA Kafka Scaler → External Metrics → HPA → Consumer Deployment → More Consumers → Lag ↓
└── Kafka Exporter → Prometheus → Grafana
```

How the flow works:

- When the producer rate increases, Kafka lag grows.
- KEDA queries Kafka directly for lag and converts that into external metrics for HPA.
- Kafka Exporter is not part of the scaling path; it exists for observability.
- HPA uses the external metrics to adjust the consumer replica count.
- More consumer pods reduce lag by processing the backlog in parallel.

This is a control loop, not a one-way reaction. If traffic drops, lag decreases and the deployment scales back down.

## Observability Flow

Observability is separate from autoscaling. Scaling answers "how many replicas do we need?" Observability answers "what is the system doing?"

```text
Producer Metrics

↓

Prometheus

↓

Grafana

-------------------

Consumer Metrics

↓

Prometheus

↓

Grafana

-------------------

Kafka Exporter

↓

Prometheus

↓

Grafana

-------------------

KEDA Metrics

↓

Prometheus

↓

Grafana

-------------------

HPA Metrics

↓

Prometheus

↓

Grafana
```

Why this matters:

- Producer metrics show event generation rate and delivery health.
- Consumer metrics show processing throughput and failure rate.
- Kafka Exporter exposes lag so the backlog is visible even outside the scaling loop.
- KEDA and HPA metrics show why replica counts changed.
- Grafana turns all of that into a timeline that explains behavior over time.

## Component Responsibilities

| Component | Responsibility |
| --- | --- |
| Producer | Generate events |
| Kafka | Durable event storage |
| Consumer | Process events |
| Strimzi | Kafka lifecycle management |
| KEDA | Convert lag into external metrics |
| HPA | Scale deployments |
| Prometheus | Collect metrics |
| Grafana | Visualize metrics |

## Why These Technologies?

### Why Kafka?

- Durable event log
- Consumer groups
- Replay capability
- Partitioning for parallelism

Kafka fits this project because it models real event-driven workloads and makes lag visible as a first-class signal.

### Why Strimzi?

- Operator pattern
- Declarative Kafka management
- Kubernetes-native lifecycle

Strimzi keeps the Kafka cluster aligned with the rest of the Kubernetes control plane.

### Why KEDA?

- HPA cannot directly read Kafka lag
- KEDA bridges Kafka and Kubernetes
- It turns lag into a scaling signal

### Why Prometheus?

- Time-series metrics
- Historical analysis
- Queryable data for dashboards and alerts

### Why Grafana?

- Correlates metrics across layers
- Makes scaling decisions easier to explain
- Helps distinguish normal backlog from unhealthy backlog

## Design Decisions

### Manual Offset Commit

Offsets are committed only after processing succeeds so a failure does not silently drop data.

### Why Consumer Lag as the Scaling Signal?

Consumer lag was selected as the scaling signal because it reflects backlog rather than infrastructure utilization. CPU usage alone cannot distinguish a slow application from a burst of incoming events, whereas lag directly measures how much work remains unprocessed.

### Processing Delay

The consumer includes an artificial delay to create lag during experiments and demonstrate autoscaling behavior.

### Metrics Instrumentation

Metrics are added to correlate throughput, lag, and scaling behavior rather than to collect data blindly.

### Single Topic

One topic keeps the example focused while still preserving the important concepts of partitions, consumer groups, and lag.

### Kafka Exporter

Kafka lag is the key signal for this architecture, so exporter visibility is essential.

### Dashboard

The dashboard exists to explain scaling decisions, not just to show metrics.

### Declarative Platform

Kubernetes, Strimzi, and KEDA are used together so the full stack can be described and reproduced as manifests rather than imperative runtime setup.

## Why Scaling Happens

Scaling decisions are driven by consumer backlog (Kafka lag), not by CPU or memory utilization.

When event production outpaces event processing, lag grows. That lag indicates the consumer group is falling behind. KEDA converts the lag into a signal that the Horizontal Pod Autoscaler can use, and the deployment gets more consumer replicas.

The important limitation is that more pods only help if there is partition-level parallelism available. In other words, replica count alone does not guarantee unlimited throughput. Partition count still bounds how much work can be parallelized.

## Known Limitations

Current implementation does not include:

### Security

- TLS
- SASL authentication

### Reliability

- Dead Letter Queue
- Multi-broker Kafka high availability

### Operations

- GitOps deployment
- Distributed tracing

### Platform Features

- Schema Registry
- Kafka Connect

These are all valid future improvements, but they are intentionally left out to keep the architecture focused on the autoscaling problem.

## Future Evolution

Future improvements may include:

- Kafka Connect + CDC
- Schema Registry
- Dead Letter Queue
- OpenTelemetry
- Alertmanager
- Multi-broker Kafka
- GitOps with Argo CD

## Engineering Learnings

- Consumer lag alone does not guarantee useful scaling; partition count limits effective parallelism.
- KEDA translates external metrics into HPA decisions rather than scaling workloads directly.
- Dashboards explain behavior, while logs remain essential for diagnosing connectivity and configuration failures.
- Instrumentation should be driven by questions, not by collecting every available metric.

## Related Docs

| Document | Purpose |
| --- | --- |
| [README.md](../README.md) | Project overview and quick start |
| [docs/deployment-guide.md](deployment-guide.md) | Deploy the platform |
| [docs/dashboard-guide.md](dashboard-guide.md) | Understand observability |
| [docs/validation-guide.md](validation-guide.md) | Verify functionality |
