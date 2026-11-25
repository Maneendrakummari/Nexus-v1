# Running the pipeline on Kubernetes

These manifests have not been tested against a real cluster.
Use a single-node cluster (Docker Desktop Kubernetes, minikube or kind).

```
k8s/
  Dockerfile                    Spark image + Kafka/Iceberg jars + app jar + data
  build-image.ps1               builds the image orders-pipeline:1.0
  01-base.yaml                  namespace, PVC (warehouse + checkpoints), service account, RBAC
  02-kafka.yaml                 Kafka (KRaft) + Job that creates the topic "retail"
  03-streaming-submit-job.yaml  spark-submit (cluster mode) -> driver pod + executor pod
  04-producer-job.yaml          publishes the 5 micro-batches
  05-read-tables-job.yaml       prints all tables
```

## Quick start (PowerShell, from the project root)

```powershell
powershell -ExecutionPolicy Bypass -File k8s\build-image.ps1   # build the image
powershell -ExecutionPolicy Bypass -File k8s\deploy.ps1        # deploy and run everything once
powershell -ExecutionPolicy Bypass -File k8s\cleanup.ps1       # delete everything
```

## Step by step (what deploy.ps1 does)

```powershell
# 1. image
powershell -ExecutionPolicy Bypass -File k8s\build-image.ps1

# 2. namespace, storage, Kafka, topic
kubectl apply -f k8s/01-base.yaml
kubectl apply -f k8s/02-kafka.yaml
kubectl -n orders wait --for=condition=complete job/create-topic --timeout=180s

# 3. start the stream (creates the driver pod and the executor pod)
kubectl apply -f k8s/03-streaming-submit-job.yaml
kubectl -n orders get pods -w
kubectl -n orders logs -f -l spark-role=driver          # wait for "Streaming started"

# 4. send the data (new window); watch the BATCH 1..5 lines in the driver log
kubectl apply -f k8s/04-producer-job.yaml
kubectl -n orders logs -f job/order-events-producer

# 5. verify
kubectl apply -f k8s/05-read-tables-job.yaml
kubectl -n orders logs -f job/read-order-tables
```

## Run again from scratch

```powershell
kubectl delete namespace orders
```
This deletes the PVC too, so the warehouse and checkpoint start empty.

## Notes

- Spark settings come from environment variables: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `WAREHOUSE`,
  `CHECKPOINT_DIR`. Without them the code uses the local Windows defaults.
- The image is built from `debian:bookworm-slim` (not Ubuntu). It installs Java 17 and downloads Spark `3.5.9` while building, so the first build needs internet access and takes a few minutes. Change `SPARK_VERSION` in `Dockerfile` if you use another Spark version.
- For a multi-node cluster use a ReadWriteMany storage class, or set `WAREHOUSE` to an `s3a://` path (needs hadoop-aws jars).
