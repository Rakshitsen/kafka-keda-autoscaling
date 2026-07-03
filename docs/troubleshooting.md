# Troubleshooting Guide

## 1. Purpose

This guide provides a structured approach to diagnosing and resolving deployment, connectivity, autoscaling, and observability issues within the Kafka autoscaling platform. The objective is to identify root causes using evidence from logs, metrics, and system state rather than relying on assumptions.

## 2. Troubleshooting Philosophy

Follow a systematic, evidence-based approach. Do not skip steps.

```text
Observe → Collect Evidence → Form Hypothesis → Verify → Fix → Validate
```

1.  **Observe**: Notice that the system is not behaving as expected (e.g., "consumer lag is high").
2.  **Collect Evidence**: Gather data from logs, metrics (Grafana), and Kubernetes resource status.
3.  **Form Hypothesis**: Based on the evidence, propose a potential root cause (e.g., "The consumer has the wrong bootstrap server address").
4.  **Verify**: Perform a specific check to confirm or deny the hypothesis (e.g., "Check the consumer's environment variables and logs for connection errors").
5.  **Fix**: Implement the solution.
6.  **Validate**: Confirm that the fix has resolved the issue and the system has returned to a healthy state.

## 3. Investigation Workflow

When a problem occurs, follow this workflow to narrow down the root cause.

```text
Problem → What changed? → Metrics (Dashboard) → Logs → Kubernetes Resources → Kafka Resources → Root Cause
```

1.  **Problem**: Clearly define the issue (e.g., "Orders are not being processed").
2.  **What changed?**: Was there a recent deployment, configuration change, or traffic spike?
3.  **Metrics (Dashboard)**: Start with the Grafana dashboard. It provides the fastest overview of the system's health. Look at producer/consumer throughput, lag, and HPA replicas.
4.  **Logs**: If metrics point to a specific component (e.g., zero producer throughput), check its logs for errors.
5.  **Kubernetes Resources**: Inspect the state of Pods, Deployments, Services, and `ScaledObjects`.
6.  **Kafka Resources**: Use CLI tools to inspect the Kafka topic, consumer group, and offsets directly.
7.  **Root Cause**: Synthesize the evidence to pinpoint the exact cause.

---

## 4. Common Issues

| Symptom | Possible Cause | First Place to Look |
| :--- | :--- | :--- |
| **Consumer lag is high and rising** | 1. Producer rate > Consumer rate (legitimate load) <br> 2. Consumer is slow or has crashed <br> 3. Not enough partitions for consumer replicas | Grafana: `Consumer Lag` panel |
| **Consumer deployment is not scaling up** | 1. KEDA `ScaledObject` is misconfigured <br> 2. KEDA is not running <br> 3. Lag has not crossed the `lagThreshold` | `kubectl describe scaledobject` |
| **Producer is not sending messages** | 1. Incorrect bootstrap server or topic name <br> 2. Kafka cluster is down <br> 3. Producer pod is in a crash loop | `kubectl logs -l app=kafka-producer` |
| **Consumer pods are stuck at partition count** | This is expected behavior (`allowIdleConsumers: false`) | `kubectl get hpa` & `kafka-topics --describe` |
| **Grafana dashboard is empty** | 1. Prometheus is not scraping targets <br> 2. `ServiceMonitor` or `PodMonitor` is misconfigured <br> 3. Time range in Grafana is incorrect | Prometheus UI: Targets page |
| **Pods are in `CrashLoopBackOff`** | 1. Application error on startup <br> 2. Misconfigured environment variables <br> 3. Liveness/readiness probe is failing | `kubectl describe pod <pod-name>` |

---

## 5. Component-Specific Checks

### Strimzi & Kafka

- **Is the Kafka cluster ready?**
  ```bash
  kubectl get kafka my-cluster -n kafka -w
  # Wait for READY status to be True.
  ```
- **Are the broker pods running?**
  ```bash
  kubectl get pods -n kafka -l strimzi.io/cluster=my-cluster,strimzi.io/kind=Kafka
  ```
- **Does the `orders` topic exist?**
  ```bash
  kubectl -n kafka run kafka-client -ti --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka:9092 --describe --topic orders
  ```

### Producer Application

- **Is the pod running and healthy?**
  ```bash
  kubectl get pods -n kafka -l app=kafka-producer
  ```
- **Check logs for connection or send errors:**
  ```bash
  kubectl logs -n kafka -l app=kafka-producer
  # Look for "Broker resolution failure", "UNKNOWN_TOPIC_OR_PART", or other Kafka errors.
  ```
- **Check its configuration:**
  ```bash
  kubectl describe deployment kafka-producer -n kafka | grep KAFKA_
  ```

### Consumer Application

- **Are pods running? How many?**
  ```bash
  kubectl get pods -n kafka -l app=kafka-consumer
  ```
- **Check logs for processing errors or rebalancing storms:**
  ```bash
  kubectl logs -n kafka -l app=kafka-consumer
  ```
- **Are offsets being committed?**
  ```bash
  # Use the Kafka CLI command in the "Useful Commands" section below.
  # The CURRENT-OFFSET should be increasing.
  ```

### KEDA (Autoscaler)

- **Are the KEDA components running?**
  ```bash
  kubectl get deploy -n keda
  # Ensure keda-operator and keda-operator-metrics-apiserver are AVAILABLE.
  ```
- **Is the `ScaledObject` configured correctly and ready?**
  ```bash
  kubectl get scaledobject -n kafka
  # READY status should be True.
  kubectl describe scaledobject kafka-consumer-so -n kafka
  # Check that bootstrapServer, topic, consumerGroup, and lagThreshold are correct.
  ```
- **Did KEDA create an HPA?**
  ```bash
  kubectl get hpa -n kafka
  # An HPA named keda-hpa-kafka-consumer-so should exist.
  ```

### Prometheus & Grafana (Observability)

- **Are the monitoring stack pods running?**
  ```bash
  kubectl get pods -n prometheus
  ```
- **Is Prometheus scraping the application targets?**
  1. Port-forward to the Prometheus UI: `kubectl port-forward -n prometheus svc/prometheus-community-kube-prometheus 9090`
  2. Open `http://localhost:9090`
  3. Go to Status -> Targets. Look for `kafka-producer` and `kafka-consumer` targets. They should be `UP`.

---

## 6. Useful Commands

**Check Consumer Group Lag & Members (from inside cluster):**
```bash
kubectl -n kafka run kafka-client -ti --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
  bin/kafka-consumer-groups.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka:9092 \
  --describe --group order-processors
```

**Watch HPA and Replica Count Changes:**
```bash
kubectl get hpa -n kafka -w
```

**Stream Logs from All Consumer Pods:**
```bash
kubectl logs -n kafka -l app=kafka-consumer -f --tail=50
```

**Get a Shell Inside a Pod for Debugging:**
```bash
kubectl exec -it <pod-name> -n kafka -- /bin/sh
```

**Check KEDA Operator Logs for Scaling Decisions:**
```bash
kubectl logs -n keda -l app.kubernetes.io/name=keda-operator
```

---

## 7. Escalation Path

If you have followed this guide and are still unable to resolve the issue, follow these steps:

1.  **Document your findings**: Collect all relevant logs, metric screenshots, and command outputs.
2.  **Consult a team member**: A fresh pair of eyes can often spot something that was missed.
3.  **Contact the platform team**: If the issue appears to be with the underlying Kubernetes or Kafka infrastructure, escalate to the responsible team with your documented evidence.

---

## 8. Related Documents

| Document | Purpose |
| :--- | :--- |
| architecture.md | Understand the "why" behind the system's design. |
| deployment-guide.md | Step-by-step instructions for deploying the platform. |
| validation-guide.md | How to verify a healthy deployment. |
| dashboard-guide.md | How to interpret the Grafana dashboard panels. |
| experiments.md | Scenarios for testing system behavior under different conditions. |

This updated file provides a clear, step-by-step process for anyone trying to diagnose a problem with the system. It starts broad with philosophy and common issues, then drills down into specific component checks and useful commands.

Let me know if you'd like any adjustments or further details added!
<!--
[PROMPT_SUGGESTION]Add a section to the troubleshooting guide for network-related issues like NetworkPolicy or DNS failures.[/PROMPT_SUGGESTION]
[PROMPT_SUGGESTION]Based on the troubleshooting guide, create a new `troubleshooting-runbook.md` with checklists for specific scenarios like "Consumer Lag High".[/PROMPT_SUGGESTION]
