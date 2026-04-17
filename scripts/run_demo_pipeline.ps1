$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Wait-ForHttpEndpoint {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$MaxAttempts = 30,
        [int]$DelaySeconds = 2
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
            return
        }
        catch {
            Start-Sleep -Seconds $DelaySeconds
        }
    }

    throw "Timed out while waiting for $Url"
}

Write-Host "Step 1: copy sample data"
& "$PSScriptRoot\bootstrap_sample_data.ps1"

Write-Host "Step 2: start infrastructure"
docker compose up -d zookeeper kafka clickhouse minio web

Write-Host "Waiting for ClickHouse to become ready"
Wait-ForHttpEndpoint -Url "http://127.0.0.1:8123/ping"
Write-Host "Waiting for Superset initialization endpoint"
Start-Sleep -Seconds 10

Write-Host "Step 3: start realtime consumer in background"
$streamingJob = Start-Job -ScriptBlock {
    param($root)
    Set-Location $root
    python jobs/streaming/flink_job.py
} -ArgumentList $projectRoot

Write-Host "Step 4: replay sample events to Kafka"
python jobs/replay/replay_gharchive_to_kafka.py --input data/raw --topic github_events --speedup 1000

Write-Host "Step 5: run batch job"
python jobs/batch/spark_job.py --input data/raw --output data/sample

Write-Host "Step 6: load batch results"
python scripts/load_batch_metrics_to_clickhouse.py --input data/sample

Write-Host "Step 7: start Next.js dashboard"
Write-Host "Realtime consumer job id: $($streamingJob.Id)"
Write-Host "Use Receive-Job -Id $($streamingJob.Id) to inspect background consumer logs if needed."
npm run dev --prefix apps/web
