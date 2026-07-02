## Experiment 1 — Consumer Failure and Scale-to-Zero

### Objective

Understand how KEDA reacts when no consumer pods are running and Kafka backlog begins to accumulate.

### Hypothesis

If consumers are unavailable while producers continue publishing events:

- Kafka lag will increase.
- Once lag exceeds the activation threshold, KEDA will activate.
- HPA will create consumer replicas.
- New consumers will join the consumer group and begin processing the backlog.
- After the backlog is cleared and the cooldown period expires, the deployment will scale back to zero.

### Environment

- Kafka deployed using Strimzi
- Consumer Deployment managed by KEDA
- Scale-to-zero enabled
- Producer continuously generating order events

### Procedure

1. Scale the consumer deployment to zero.
2. Keep the producer running.
3. Observe Kafka lag.
4. Observe KEDA metrics.
5. Observe HPA.
6. Observe consumer deployment.

### Expected Outcome

Kafka lag should continue increasing until the activation threshold is exceeded. KEDA should activate and request HPA to create consumer replicas.

### Observations

- Producer continued publishing events.
- Consumer throughput dropped to zero.
- Kafka lag increased steadily.
- KEDA became active after the configured activation lag.
- HPA increased desired replicas.
- Consumer pods started.
- Consumer group resumed processing.
- Lag gradually returned to zero.
- After the cooldown period, replicas scaled back to zero.

### Evidence

Dashboard Panels:

- Producer Messages/sec
- Consumer Messages/sec
- Consumer Lag
- KEDA Scaler Active
- KEDA Metric Value
- HPA Desired Replicas

Supporting Evidence:

- Consumer logs
- kubectl get hpa
- kubectl get scaledobject

### Root Cause

The absence of consumer pods prevented messages from being processed, causing Kafka lag to grow. Lag became the external metric that triggered autoscaling.

### Recovery

No manual intervention was required. KEDA automatically restored consumer capacity once the lag threshold was exceeded.

### Lessons Learned

- Consumer lag is the primary scaling signal.
- Scale-to-zero depends on activation lag and cooldown settings.
- Scaling is driven by backlog rather than CPU utilization.



## Experiment 2 — Producer Surge & Autoscaling

### Objective

Understand how the system reacts to a sudden increase in message production rate and validates the KEDA autoscaling loop.

### Hypothesis

A sudden surge in the producer's message rate will overwhelm the existing consumers, causing Kafka lag to increase. KEDA will detect this lag, trigger the Horizontal Pod Autoscaler (HPA), and scale up the number of consumer pods to handle the increased load.

### Environment

- Kafka, KEDA, and the producer/consumer deployments are running in a steady state.
- Consumer `minReplicas` is set (e.g., to 1).
- KEDA `lagThreshold` is configured (e.g., to 100).

### Procedure

1.  Note the current number of consumer replicas.
2.  Increase the producer's message rate by updating its environment variable: `kubectl set env deployment/kafka-producer MESSAGE_RATE_PER_SEC=30`.
3.  Observe the "Consumer Lag" panel in Grafana.
4.  Observe the "HPA Desired Replicas" panel to see the scaling decision.
5.  Verify that new consumer pods are created: `kubectl get pods -l app=kafka-consumer -w`.
6.  Once the lag stabilizes, reduce the producer's rate back to its original value.
7.  Observe the system scaling back down after the cooldown period.

### Expected Outcome

Kafka lag will rise sharply. KEDA will activate and instruct the HPA to scale up the consumer deployment. The new consumers will help process the backlog, and lag will decrease. Once the producer rate is lowered, the system will scale back down to the minimum replica count.

### Observations

- As the producer rate increased, the "Consumer Lag" metric spiked.
- KEDA's metric value for the HPA increased, causing the HPA to raise the desired replica count.
- New consumer pods were scheduled and started.
- As more consumers joined the group, the message processing rate increased, and the lag began to decrease.

### Evidence

Dashboard Panels:

- Producer Messages/sec (shows the initial spike)
- Consumer Lag (shows the corresponding increase and subsequent decrease)
- HPA Desired Replicas (shows the scale-up and scale-down events)
- Consumer Messages/sec (shows an increase as more pods come online)

### Root Cause

The rate of message production exceeded the processing capacity of the initial set of consumers, causing a backlog (lag) to form. This lag is the direct trigger for KEDA's scaling logic.

### Recovery

The system recovered automatically. The autoscaling mechanism added capacity to match the workload, clearing the backlog without manual intervention.

### Lessons Learned

- The system is elastic and can respond to load spikes automatically.
- The `lagThreshold` is the key tuning parameter for responsiveness. A lower value will make the system scale up more aggressively.

## Experiment 3 — Wrong Bootstrap Server

### Objective

Validate the system's observability and fault tolerance when a critical configuration, like the Kafka bootstrap server address, is incorrect.

### Hypothesis

If the producer is configured with an incorrect bootstrap server address, it will fail to connect to Kafka. This will result in message delivery failures, which should be visible in the producer's logs and its health endpoint.

### Procedure

1.  Deploy the producer with an invalid `KAFKA_BOOTSTRAP_SERVERS` value (e.g., `kafka.invalid:9092`).
2.  Check the Grafana dashboard for producer throughput. It should be zero.
3.  Check the producer pod's status: `kubectl get pods -l app=kafka-producer`. It should be running, as the application itself hasn't crashed.
4.  Inspect the producer's logs for connection errors: `kubectl logs -l app=kafka-producer`.
5.  Query the producer's health endpoint.

### Expected Outcome

The producer pod will be in a `Running` state, but logs will be filled with connection timeout or DNS resolution errors. The `/health` endpoint will report a healthy status but show `messages_failed` increasing and `messages_sent` at zero.

### Observations

- The producer pod started successfully but was unable to send any messages.
- The Grafana dashboard showed zero throughput for the producer.
- The producer logs clearly indicated a "Broker resolution failure" or "Connection refused" error, pointing directly to the misconfiguration.
- The health endpoint correctly reported an increasing count of `messages_failed`.

### Root Cause

A configuration error prevented the producer application from establishing a network connection with the Kafka cluster.

### Recovery

Manual intervention is required. The deployment configuration must be corrected with the valid `KAFKA_BOOTSTRAP_SERVERS` address and redeployed.

### Lessons Learned

- Application-level metrics and health checks are crucial for distinguishing between a crashed pod and a running-but-failing application.
- Structured logs are essential for quickly diagnosing the root cause of connectivity issues.

## Experiment 4 — Wrong Topic Name

### Objective

Understand how the system behaves when the producer tries to publish to a non-existent topic, considering the cluster's topic auto-creation policy.

### Hypothesis

If `auto.create.topics.enable` is `false` on the Kafka brokers (as is the case in this project's setup), the producer's attempt to send a message to a wrong topic will fail. This failure will be reported in the producer's logs and metrics.

### Procedure

1.  Configure the producer to publish to a topic that does not exist (e.g., `KAFKA_TOPIC=wrong_orders`).
2.  Observe the producer's logs for errors.
3.  Query the producer's `/health` endpoint and observe the `messages_failed` count.

### Expected Outcome

The producer will fail to send messages. The logs will contain `UNKNOWN_TOPIC_OR_PART` errors. The health endpoint will show that `messages_sent` is zero while `messages_failed` increases.

### Observations

- The producer logs immediately showed errors indicating the topic was unknown.
- The health endpoint reflected these failures, providing a clear signal that something was wrong with message delivery.

### Root Cause

The producer was configured to send messages to a topic that did not exist, and the Kafka cluster was correctly configured to reject such requests.

### Recovery

Manual intervention is required to correct the `KAFKA_TOPIC` environment variable in the producer's deployment configuration.

### Lessons Learned

- Disabling automatic topic creation (`auto.create.topics.enable=false`) is a production best practice that prevents typos and misconfigurations from creating unwanted topics.
- The combination of logs and health endpoint metrics provides a robust way to detect and diagnose configuration errors without needing to inspect the Kafka cluster directly.

## Experiment 5 — Wrong Consumer Group

### Objective

Understand the system's behavior when a consumer is deployed with a new, previously unseen consumer group ID.

### Hypothesis

When a consumer starts with a new `KAFKA_CONSUMER_GROUP` name, Kafka will create a new group. Since this group has no committed offsets, its starting position will be determined by the `auto.offset.reset` policy. With this project's setting of `'earliest'`, the new consumer group will start processing messages from the very beginning of the topic, causing a massive initial lag report.

### Procedure

1.  Modify the consumer's deployment manifest (`kubernetes-consumer.yaml` or similar) and change the `KAFKA_CONSUMER_GROUP` environment variable to a new value (e.g., `order-processors-v2`).
2.  Apply the manifest to deploy the consumer.
3.  Check the consumer group lag using the Kafka CLI tools for the new group name.
4.  Observe the consumer logs to see which offsets it begins processing.

### Expected Outcome

A new consumer group will be created in Kafka. It will start consuming from offset 0 on all assigned partitions, effectively re-processing every message currently in the topic. The reported lag will initially be equal to the total number of messages in the topic.

### Observations

- A new consumer group was created as expected.
- The consumer began processing messages from the earliest available offset.
- The reported consumer lag was immediately very high, reflecting the entire history of the topic.

### Root Cause

The behavior is a direct result of the `auto.offset.reset: 'earliest'` configuration. This policy instructs Kafka on what to do when a consumer from a new group joins: start from the beginning.

### Recovery

This is expected behavior, not an error. To "recover," one would either let the new group process all the historical data or revert the configuration to the original consumer group ID and redeploy.

### Lessons Learned

- The consumer group ID is the fundamental mechanism for tracking progress in Kafka. Changing it is equivalent to starting over.
- The `auto.offset.reset` policy is critical. `'earliest'` is useful for full replayability and testing, while `'latest'` is typically used when you only care about messages from this point forward.

## Experiment 6 — Partition Limit vs. Consumer Replicas

### Objective

Demonstrate that the number of topic partitions acts as the upper bound for consumer parallelism and how KEDA respects this limit.

### Hypothesis

If consumer lag increases, KEDA will scale up consumer replicas. However, since a single partition can only be consumed by one consumer in a group at a time, the number of *active* consumers cannot exceed the number of partitions. With the default KEDA setting (`allowIdleConsumers: false`), KEDA will not scale the deployment beyond the partition count, even if the lag is extremely high. Any extra pods created would be idle.

### Environment

- A Kafka topic with a fixed number of partitions (e.g., 3).
- A KEDA `ScaledObject` with `allowIdleConsumers` set to `false` (the default).

### Procedure

1.  Ensure the `orders` topic has a known number of partitions (e.g., 3).
2.  Induce a high consumer lag by significantly increasing the producer's `MESSAGE_RATE_PER_SEC`.
3.  Observe the number of consumer replicas scaled by the HPA: `kubectl get hpa -w`.
4.  Observe the number of running pods: `kubectl get pods -l app=kafka-consumer`.

### Expected Outcome

Even if the lag is high enough to theoretically justify scaling to the `maxReplicaCount` (e.g., 10), KEDA will cap the desired replicas at the partition count (3). The HPA will not scale the consumer deployment beyond 3 pods.

### Observations

- As lag increased, KEDA and the HPA scaled the consumer deployment up.
- The number of replicas stopped increasing once it matched the number of topic partitions (3), even as lag remained high.
- If more replicas were manually added, the extra pods would start but remain idle, as there were no available partitions for them to claim.

### Root Cause

This is a core principle of Kafka's consumer group model. KEDA is aware of this and, by default, prevents wasting resources by scaling to idle consumers. It uses a formula to cap the target replica count based on the partition count to ensure scaling is effective.

The formula KEDA uses when `allowIdleConsumers` is false is effectively: `desiredReplicas = min(calculatedReplicasBasedOnLag, partitionCount)`.

### Recovery

This is the correct and efficient behavior. If more processing power is needed, the solution is not to add more consumers but to increase the number of partitions for the topic (which is a more involved operational task).

### Lessons Learned

- **The number of partitions is the ultimate ceiling for parallelism in a Kafka consumer group.** You cannot have more active consumers than partitions.
- KEDA's default behavior (`allowIdleConsumers: false`) is smart and cost-effective, preventing scaling beyond the point of usefulness.
- When designing a Kafka-based system, the number of partitions must be chosen carefully based on the expected peak workload and desired parallelism.
