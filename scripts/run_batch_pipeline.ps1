param(
    [string]$InputDir = "data/raw",
    [string]$CuratedDir = "data/curated",
    [string]$OutputDir = "data/sample"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker command not found. Please install/start Docker Desktop, or run local Spark with JAVA_HOME + winutils."
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "Step 1: build and start spark batch image"
docker compose build spark-batch
if ($LASTEXITCODE -ne 0) {
    throw "docker compose build failed with exit code $LASTEXITCODE"
}

Write-Host "Step 2: curate raw GH Archive data"
docker compose run --rm spark-batch "/opt/spark/bin/spark-submit /workspace/jobs/batch/curate_events.py --input /workspace/$InputDir --output /workspace/$CuratedDir"
if ($LASTEXITCODE -ne 0) {
    throw "curate_events spark-submit failed with exit code $LASTEXITCODE"
}

Write-Host "Step 3: run Spark batch aggregations on curated parquet"
docker compose run --rm spark-batch "/opt/spark/bin/spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=64 /workspace/jobs/batch/spark_job.py --input /workspace/$CuratedDir --output /workspace/$OutputDir"
if ($LASTEXITCODE -ne 0) {
    throw "spark_job spark-submit failed with exit code $LASTEXITCODE"
}

Write-Host "Step 4: load batch metrics into ClickHouse"
if (-not (Test-Path $OutputDir)) {
    throw "Batch output directory '$OutputDir' was not generated. Aborting ClickHouse load."
}
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
$pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }
& $pythonExe -m scripts.load_batch_metrics_to_clickhouse --input $OutputDir
if ($LASTEXITCODE -ne 0) {
    throw "load_batch_metrics_to_clickhouse failed with exit code $LASTEXITCODE"
}
