# Deployment Guide

This guide describes how to deploy the Kafka autoscaling platform on Kubernetes.

## Prerequisites

- Kubernetes cluster
- kubectl configured
- Helm installed

### Create Namespaces

Create all required namespaces upfront.
```bash
kubectl create ns strimzi
kubectl create ns kafka
kubectl create ns keda
kubectl create ns prometheus
```

## Deployment Order

The deployment follows a layered approach where each component depends on the previous one.

Strimzi Operator

 ↓
 
 Kafka Cluster
  
 ↓

 Kafka Topic
 
↓

Applications

↓

KEDA

↓

Observability`

Each layer depends on the previous one. Do not skip validation before proceeding.

---

## Step 1: Deploy Strimzi (Kafka Operator)

### Purpose
Deploy the Strimzi Operator, which is responsible for managing Kafka resources within the cluster.

### 1. Install Strimzi Operator
```bash
helm install my-strimzi-cluster-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  -f ../platform/strimzi/operator-values.yaml \
  --namespace strimzi
```

### 2. Expected Result

Check that the operator deployed successfully.

-   **Command**: `helm ls -n strimzi`
    -   **Result**: Shows `my-strimzi-cluster-operator` with `deployed` status.
-   **Command**: `kubectl get deploy -n strimzi`
    -   **Result**: The `my-strimzi-cluster-operator-strimzi-kafka-operator` deployment is `AVAILABLE`.

---

## Step 2: Deploy Kafka Cluster & Topic

With the operator running, we can now declaratively create a Kafka cluster and a topic.

### 1. Deploy Kafka Resources

Deploy the `Kafka` cluster and `KafkaTopic` custom resources.
```bash
kubectl apply -f ../platform/strimzi/kafka-cluster.yaml -n kafka
kubectl apply -f ../platform/strimzi/topic.yaml -n kafka
```

### 2. Expected Result

Check that the custom resources for the cluster and topic are `Ready`.

-   **Command**: `kubectl get kafka my-cluster -n kafka -w`
    -   **Result**: The `READY` status becomes `True`.
-   **Command**: `kubectl get kafkatopic orders -n kafka`
    -   **Result**: The `READY` status is `True`.

### 3. Smoke Test

Run a temporary producer and consumer pod to ensure the cluster is working.

*   In one terminal, start the consumer:
    ```bash
    kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
      bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka:9092 --topic orders --from-beginning
    ```

*   In another terminal, start the producer and type some messages:
    ```bash
    kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:latest --rm=true --restart=Never -- \
      bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap.kafka:9092 --topic orders
    ```
You should see the messages appear in the consumer terminal.

---

## Step 3: Deploy Application Services

Now, deploy the Python producer and consumer applications.

### 1. Build and Deploy

The deployment flow for the applications is as follows:

`Build producer/consumer image ↓ Push image to registry ↓ Update deployment manifests ↓ Deploy applications`

Refer to the `../application/README.md` for detailed instructions on building and pushing the container images.

### 2. Deploy the Applications

Apply the Kubernetes manifests. Ensure the `KAFKA_BOOTSTRAP_SERVERS` and image names are correct in the YAML files.
```bash
kubectl apply -f ../application/kubernetes-producer.yaml -n kafka
kubectl apply -f ../application/kubernetes-consumer.yaml -n kafka
```

### 3. Expected Result

Check that the application pods are running.

-   **Command**: `kubectl get deploy -n kafka`
    -   **Result**: The `kafka-consumer` and `kafka-producer` deployments are `AVAILABLE`.

---

## Step 4: Deploy KEDA for Autoscaling

Install KEDA to enable event-driven autoscaling for our consumer.

### 1. Install KEDA
```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda
```

### 2. Expected Result (KEDA)

Check that the KEDA operator components are running.
-   **Command**: `kubectl get deploy -n keda`
    -   **Result**: `keda-admission-webhooks`, `keda-operator`, and `keda-operator-metrics-apiserver` deployments are `AVAILABLE`.

### 3. Create the ScaledObject

This custom resource tells KEDA how to monitor the Kafka lag and which deployment to scale.
```bash
kubectl apply -f ../platform/keda/scaledobject.yaml -n kafka
```

**Note**: The `HorizontalPodAutoscaler` (HPA) is not deployed manually. It is created automatically by KEDA after the `ScaledObject` becomes `Ready`.

### 4. Expected Result (Autoscaling)

Check that the `ScaledObject` is ready and that it created an HPA.

-   **Command**: `kubectl get scaledobject -n kafka`
    -   **Result**: The `kafka-consumer-so` object shows `READY` status as `True`.
-   **Command**: `kubectl get hpa -n kafka`
    -   **Result**: An HPA named `keda-hpa-kafka-consumer-so` exists.

---

## Step 5: Deploy Monitoring Stack (Prometheus & Grafana)

Finally, deploy the observability stack to visualize the system's behavior.

### 1. Install Stack

This Helm chart provides a full Prometheus and Grafana setup.
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install prometheus-community prometheus-community/kube-prometheus-stack --namespace prometheus --create-namespace
```

### 2. Configure Scraping

These manifests tell Prometheus how to discover and scrape metrics from our applications, Kafka, and KEDA.
```bash
kubectl apply -f ../observability/prometheus/producer-servicemonitor.yaml -n kafka
kubectl apply -f ../observability/prometheus/consumer-servicemonitor.yaml -n kafka
kubectl apply -f ../observability/prometheus/kafka-podmonitor.yaml -n kafka
kubectl apply -f ../observability/prometheus/keda-servicemonitor.yaml -n keda
```

### 3. Import Dashboard

First, access the Grafana UI.
```bash
kubectl port-forward svc/prometheus-community-grafana -n prometheus 8080:80
```
-   Open `http://localhost:8080` in your browser.
-   The default login is `admin` / `prom-operator`.

In the Grafana UI, go to "Dashboards" -> "Import" and upload the `../observability/grafana/kafka-autoscaling-decision-timeline.json` file.

---

## Next Steps

Your platform is now fully deployed.

-   Validate the deployment using the Validation Guide.
-   Run the experiments defined in experiments.md.
-   Use the Dashboard Guide to interpret telemetry.
-   If validation fails, consult the troubleshooting guide.


<!--
    This manifest tells the Strimzi operator to provision a Kafka cluster named `my-cluster`.
-->
