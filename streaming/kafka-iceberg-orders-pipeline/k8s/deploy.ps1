# Deploys the whole pipeline to the current Kubernetes cluster and runs it once.
# Run from the project root, after build-image.ps1:
#   powershell -ExecutionPolicy Bypass -File k8s\deploy.ps1

$ErrorActionPreference = "Stop"
$ns = "orders"

function Check($what) { if ($LASTEXITCODE -ne 0) { throw "FAILED: $what" } }

# kubectl prints "No resources found" to stderr while a pod does not exist yet. Under
# $ErrorActionPreference = "Stop" that kills the script, so these helpers run with "Continue"
# (the assignment is local to each function).
function DriverLog {
    $ErrorActionPreference = "Continue"
    return (kubectl -n $ns logs -l spark-role=driver --tail=5000 2>$null | Out-String)
}

function WaitForLog($selector, $text, $timeoutSec) {
    $ErrorActionPreference = "Continue"
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $log = kubectl -n $ns logs -l $selector --tail=3000 2>$null | Out-String
        if ($log -match [regex]::Escape($text)) { return $true }
        Start-Sleep -Seconds 5
    }
    return $false
}

# Sum of the end offsets of all 3 partitions of topic retail; -1 if they could not be read.
function TopicTotal {
    $ErrorActionPreference = "Continue"
    $lines = kubectl -n $ns exec deploy/kafka -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic retail 2>$null
    $sum = 0; $parts = 0
    foreach ($l in $lines) {
        $p = "$l" -split ":"
        if ($p.Count -eq 3) { $sum += [int]$p[2]; $parts++ }
    }
    if ($parts -ne 3) { return -1 }
    return $sum
}

Write-Host "Cluster: $(kubectl config current-context)"
if (-not (docker image ls -q orders-pipeline:1.0)) { throw "Image orders-pipeline:1.0 not found - run k8s\build-image.ps1 first" }

Write-Host "`n1/6 namespace, storage, permissions"
kubectl apply -f k8s/01-base.yaml; Check "base"

Write-Host "`n2/6 Kafka + topic retail"
kubectl apply -f k8s/02-kafka.yaml; Check "kafka"
kubectl -n $ns wait --for=condition=available deployment/kafka --timeout=240s; Check "kafka not ready"
kubectl -n $ns wait --for=condition=complete job/create-topic --timeout=300s; Check "topic job"

Write-Host "`n3/6 streaming job (driver + executor pods)"
kubectl -n $ns delete job streaming-submit --ignore-not-found | Out-Null
kubectl apply -f k8s/03-streaming-submit-job.yaml; Check "streaming submit"
if (-not (WaitForLog "spark-role=driver" "Streaming started" 360)) {
    kubectl -n $ns get pods
    throw "The streaming driver did not report 'Streaming started' - check: kubectl -n orders logs -l spark-role=driver"
}
Write-Host "Streaming is running."
$mem = kubectl -n $ns get pod -l spark-role=executor -o 'jsonpath={.items[*].spec.containers[*].resources.limits.memory}'
Write-Host "Executor memory limit: $mem (expected 2560Mi)"

Write-Host "`n4/6 producer (5 micro-batches, about 90 seconds)"
# A leftover message in the topic would be replayed as one batch and push the watermark too far.
$before = TopicTotal
if ($before -ne 0) { throw "Topic retail is not empty (offset total = $before, -1 means unreadable). Run k8s\cleanup.ps1 first." }
kubectl -n $ns delete job order-events-producer --ignore-not-found | Out-Null
kubectl apply -f k8s/04-producer-job.yaml; Check "producer"
kubectl -n $ns wait --for=condition=complete job/order-events-producer --timeout=600s; Check "producer did not finish"
Write-Host "Topic offsets total: $(TopicTotal) (expected 62)"

Write-Host "`n5/6 waiting for the stream to process batch 5"
if (-not (WaitForLog "spark-role=driver" "BATCH 5" 240)) { throw "BATCH 5 not seen in the driver log" }
# The BATCH 5 line is printed before the batch has finished committing (silver MERGE, orders MERGE).
Start-Sleep -Seconds 45
Write-Host "Batch 5 processed."
(DriverLog) -split "`n" | Where-Object { $_ -match "BATCH \d \|" }
$lost = @((DriverLog) -split "`n" | Where-Object { $_ -match "Lost executor" }).Count
Write-Host "Lost-executor lines in the driver log: $lost (expected 0)"

Write-Host "`n6/6 reading the tables"
kubectl -n $ns delete job read-order-tables --ignore-not-found | Out-Null
kubectl apply -f k8s/05-read-tables-job.yaml; Check "read job"
kubectl -n