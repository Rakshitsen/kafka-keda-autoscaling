# Quick Start: Deploying to Your Kubernetes Cluster

You have Strimzi + Kafka running in the `kafka` namespace. Here's how to deploy these microservices.

## Step 1: Build & Push Docker Images

```bash
# Build producer
docker build -t your-docker-registry/kafka-producer:v1.0.0 .
docker push your-docker-registry/kafka-producer:v1.0.0

# Build consumer
docker build -t your-docker-registry/kafka-consumer:v1.0.0 .
docker push your-docker-registry/kafka-consumer:v1.0.0
```

If you don't have a registry (local testing), use `kind load docker-image` or `minikube image load`:

```bash
# For Kind
kind load docker-image kafka-producer:v1.0.0
kind load docker-image kafka-consumer:v1.0.0

# For Minikube
minikube image load kafka-producer:v1.0.0
minikube image load kafka-consumer:v1.0.0
```

## Step 2: Update Manifests

In `kubernetes-deployment.yaml`:

1. Find these lines:
   ```yaml
   - name: KAFKA_BOOTSTRAP_SERVERS
     value: "my-cluster-kafka-bootstrap.kafka.svc:9092"  # <- Update if needed
   ```

2. Verify your Kafka broker address:
   ```bash
   kubectl get svc -n kafka
   # Should show: my-cluster-kafka-bootstrap   ClusterIP   10.x.x.x   <none>   9092/TCP
   ```

3. Update image references (replace `kafka-producer:latest` with your tag):
   ```yaml
   image: your-docker-registry/kafka-producer:v1.0.0
   image: your-docker-registry/kafka-consumer:v1.0.0
   ```

4. (Optional) Adjust scaling parameters:
   ```yaml
   MESSAGE_RATE_PER_SEC: "10"        # Messages per second
   PROCESSING_DELAY_SECONDS: "0.1"   # Simulated processing time
   offsetLagTarget: "100"            # KEDA scale trigger
   ```

## Step 3: Deploy to Kubernetes

```bash
# Create namespace and deploy
kubectl apply -f kubernetes-deployment.yaml

# Watch rollout
kubectl get deployments -n kafka-apps -w
# Expected:
#   NAME               READY   UP-TO-DATE   AVAILABLE   AGE
#   kafka-consumer     2/2     2            2           10s
#   kafka-producer     1/1     1            1           10s
```

## Step 4: Verify Deployment

```bash
# Check pods are running
kubectl get pods -n kafka-apps
# Both should be in Running state with 1/1 ready

# Check health endpoints
kubectl port-forward -n kafka-apps svc/kafka-producer 8080:8080 &
curl http://localhost:8080/health
# Expected response:
# {"status":"healthy","messages_sent":150,"messages_failed":0}

kubectl port-forward -n kafka-apps svc/kafka-consumer 8081:8081 &
curl http://localhost:8081/health
# Expected response:
# {"status":"healthy","messages_processed":100,"messages_failed":0,"current_lag_estimate":0}

kill %1 %2  # Kill port-forward processes
```

## Step 5: Monitor Logs

```bash
# Producer logs
kubectl logs -n kafka-apps -f deployment/kafka-producer

# Consumer logs
kubectl logs -n kafka-apps -f deployment/kafka-consumer

# Parse JSON logs (filter by event or error)
kubectl logs -n kafka-apps -f deployment/kafka-consumer | jq 'select(.level=="ERROR")'
```

## Step 6: Check Kafka Consumer Lag

```bash
# From your local machine (if you have Kafka CLI installed)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group order-processors

# OR from inside cluster
kubectl -n kafka run kafka-group -ti \
  --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc:9092 \
  --describe --group order-processors
```

Expected output:
```
GROUP             TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-processors  orders  0          342             402             60
order-processors  orders  1          351             401             50
order-processors  orders  2          348             400             52
```

## Step 7: Test Autoscaling (KEDA)

**Prerequisites:** KEDA must be installed.

```bash
# Install KEDA (if not already done)
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda -n keda --create-namespace

# Verify ScaledObject is recognized
kubectl get scaledobjects -n kafka-apps
# Should show: kafka-consumer-scaler
```

**Trigger Autoscaling:**

1. Increase producer rate:
   ```bash
   kubectl set env deployment/kafka-producer \
     MESSAGE_RATE_PER_SEC="100" -n kafka-apps
   ```

2. Watch consumer lag increase:
   ```bash
   watch -n 2 'kubectl logs -n kafka-apps deployment/kafka-consumer | tail -10'
   ```

3. KEDA detects lag and scales up:
   ```bash
   kubectl get hpa -n kafka-apps -w
   # Shows replica count increasing (2 → 5 → 10)
   ```

4. As lag decreases, KEDA scales down:
   ```bash
   kubectl set env deployment/kafka-producer \
     MESSAGE_RATE_PER_SEC="10" -n kafka-apps
   
   watch -n 5 'kubectl get hpa -n kafka-apps'
   # Replicas gradually decrease
   ```

## Step 8: Graceful Shutdown & Rollout

```bash
# Rolling update (existing pods drain gracefully)
kubectl set image deployment/kafka-producer \
  producer=your-registry/kafka-producer:v1.0.1 -n kafka-apps

# Watch pods terminating gracefully (preStop sleep allows final flushes)
kubectl get pods -n kafka-apps -w

# Check rollout status
kubectl rollout status deployment/kafka-producer -n kafka-apps

# If something goes wrong, rollback
kubectl rollout undo deployment/kafka-producer -n kafka-apps
```

---

## Troubleshooting

### Producer not sending messages

```bash
# Check bootstrap server connectivity
kubectl exec -it deployment/kafka-producer -n kafka-apps -- \
  python -c "from confluent_kafka import Producer; \
  p = Producer({'bootstrap.servers': 'my-cluster-kafka-bootstrap.kafka.svc:9092'}); \
  print('Connected')"

# Check logs for Kafka errors
kubectl logs -n kafka-apps deployment/kafka-producer | grep -i "kafka\|error"
```

### Consumer lag not decreasing

```bash
# Check if consumer is running
kubectl get pods -n kafka-apps -l app=kafka-consumer

# Check if processing is slow
kubectl logs -n kafka-apps deployment/kafka-consumer | jq '.message' | grep "processed\|failed" | tail -20

# Check current offset vs log-end-offset
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group order-processors
```

### KEDA not scaling

```bash
# Check ScaledObject exists and is active
kubectl describe scaledobject kafka-consumer-scaler -n kafka-apps

# Check HPA status
kubectl describe hpa kafka-consumer-scaler -n kafka-apps

# Check KEDA logs
kubectl logs -n keda deployment/keda-operator
```

---

## Next Steps

1. **Add observability**: Expose Prometheus metrics (`/metrics` endpoint) for lag, latency, throughput.
2. **Add persistence**: Use a database (PostgreSQL, Redis) to track processing state.
3. **Add schema validation**: Use Schema Registry (Confluent or Karapace) to validate events.
4. **Add DLQ**: Send failed events to a separate topic for later analysis.
5. **Add distributed tracing**: Use OpenTelemetry to track order flow end-to-end.
6. **Production hardening**: Add resource quotas, network policies, pod disruption budgets.

---

## Commands Quick Reference

```bash
# Deploy
kubectl apply -f kubernetes-deployment.yaml

# Teardown
kubectl delete -f kubernetes-deployment.yaml

# Logs
kubectl logs -n kafka-apps -f deployment/kafka-producer
kubectl logs -n kafka-apps -f deployment/kafka-consumer

# Health
kubectl port-forward -n kafka-apps svc/kafka-producer 8080:8080
curl http://localhost:8080/health

# Lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group order-processors

# Scale manually (before KEDA)
kubectl scale deployment kafka-consumer -n kafka-apps --replicas=5

# Debug pod
kubectl exec -it deployment/kafka-consumer -n kafka-apps -- /bin/sh
```

---

Good luck! 🚀
