# Removes everything the pipeline created in Kubernetes (including the Iceberg data and the checkpoint).
#   powershell -ExecutionPolicy Bypass -File k8s\cleanup.ps1
kubectl delete namespace orders --ignore-not-found
