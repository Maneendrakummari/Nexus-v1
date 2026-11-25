# Run from the project root (the folder that contains build.sbt):
#   powershell -ExecutionPolicy Bypass -File k8s\build-image.ps1
# Builds the app jar and the Docker image orders-pipeline:1.0
# (the Kafka + Iceberg jars are downloaded inside the Docker build, so this needs internet access)

$ErrorActionPreference = "Stop"
$image = "orders-pipeline:1.0"

Write-Host "1/2 sbt package"
sbt package
if ($LASTEXITCODE -ne 0) { throw "sbt package failed" }

Write-Host "2/2 docker build $image (first build takes a few minutes)"
docker build -f k8s\Dockerfile -t $image .
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }

Write-Host ""
Write-Host "Done:"
docker image ls orders-pipeline
Write-Host "Kubernetes in Docker Desktop uses this image directly."
Write-Host "  minikube: minikube image load $image"
Write-Host "  kind:     kind load docker-image $image"
