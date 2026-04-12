$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "Step 1: start core services"
docker compose up -d zookeeper kafka clickhouse minio

Write-Host "Step 2: wait for ClickHouse"
Start-Sleep -Seconds 8

Write-Host "Step 3: prepare sample data"
powershell -ExecutionPolicy Bypass -File "$PSScriptRoot\bootstrap_sample_data.ps1"

Write-Host "Step 4: start realtime consumer"
$streamingJob = Start-Job -ScriptBlock {
    param($root)
    Set-Location $root
    python -m jobs.streaming.flink_job
} -ArgumentList $projectRoot

Write-Host "Step 5: replay realtime data"
python -m jobs.replay.replay_gharchive_to_kafka --input data/raw --topic github_events --speedup 1000

Write-Host "Step 6: run offline batch pipeline"
powershell -ExecutionPolicy Bypass -File "$PSScriptRoot\run_batch_pipeline.ps1"

Write-Host "Step 7: start Next.js local dev server"
Write-Host "Realtime consumer job id: $($streamingJob.Id)"
npm run dev --prefix apps/web
