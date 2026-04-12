param(
    [string]$InputDir = "data/raw",
    [string]$CuratedDir = "data/curated",
    [string]$OutputDir = "data/sample"
)

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "Step 1: build and start spark batch image"
docker compose build spark-batch

Write-Host "Step 2: curate raw GH Archive data"
docker compose run --rm spark-batch "/opt/spark/bin/spark-submit /workspace/jobs/batch/curate_events.py --input /workspace/$InputDir --output /workspace/$CuratedDir"

Write-Host "Step 3: run Spark batch aggregations on curated parquet"
docker compose run --rm spark-batch "/opt/spark/bin/spark-submit /workspace/jobs/batch/spark_job.py --input /workspace/$CuratedDir --output /workspace/$OutputDir"

Write-Host "Step 4: load batch metrics into ClickHouse"
python -m scripts.load_batch_metrics_to_clickhouse --input $OutputDir
