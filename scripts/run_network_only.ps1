param(
    [string]$CuratedDir = "data/curated",
    [string]$OutputDir = "data/network_depth",
    [int]$Phase = 1,        # 1 = depth phase-1  |  3 = temporal  |  4 = IC
    [int]$TopN = 200,
    [int]$AlsRank = 12,
    [int]$AlsIter = 10,
    [int]$GmmK = 6,
    [int]$Weeks = 4,
    [int]$IcTopActors = 1500,
    [int]$IcMcRuns = 200,
    [int]$IcKMax = 20,
    [double]$IcP0 = 0.02,
    [double]$IcPMax = 0.2,
    [switch]$SkipBuild,
    [switch]$SkipDdl,
    [switch]$SkipLoad
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker command not found. Please install / start Docker Desktop."
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host ""
Write-Host "=========================================================="
Write-Host " Network-depth pipeline  (phase $Phase)"
Write-Host "   curated  = $CuratedDir"
Write-Host "   output   = $OutputDir"
Write-Host "   top-n    = $TopN"
Write-Host "=========================================================="

# ── Step 1: make sure the spark-batch image has networkx/sklearn/xgboost baked in
if (-not $SkipBuild) {
    Write-Host "`n[1/4] docker compose build spark-batch"
    docker compose build spark-batch
    if ($LASTEXITCODE -ne 0) { throw "docker compose build failed ($LASTEXITCODE)" }
} else {
    Write-Host "`n[1/4] skipping spark-batch image build (-SkipBuild)"
}

# ── Step 2: apply new DDL to the already-running ClickHouse (idempotent CREATE IF NOT EXISTS)
if (-not $SkipDdl) {
    Write-Host "`n[2/4] applying 01_init.sql to ClickHouse (CREATE IF NOT EXISTS)"
    $chRunning = docker ps --filter "name=gba-clickhouse" --filter "status=running" -q
    if (-not $chRunning) {
        Write-Host "   ClickHouse is not running — starting it first"
        docker compose up -d clickhouse
        if ($LASTEXITCODE -ne 0) { throw "failed to start ClickHouse" }
        # wait for port
        for ($i = 0; $i -lt 30; $i++) {
            Start-Sleep -Seconds 1
            $ready = docker exec gba-clickhouse clickhouse-client --query "SELECT 1" 2>$null
            if ($ready -eq "1") { break }
        }
    }
    Get-Content "configs/clickhouse/init/01_init.sql" -Raw | `
        docker exec -i gba-clickhouse clickhouse-client --multiquery
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "some DDL statements reported non-zero exit (often fine for IF NOT EXISTS)."
    }
} else {
    Write-Host "`n[2/4] skipping DDL reapply (-SkipDdl)"
}

# ── Step 3: run appropriate Spark job inside spark-batch container
if ($Phase -eq 4) {
    Write-Host "`n[3/4] spark-submit network_ic.py  (top-actors=$IcTopActors mc=$IcMcRuns k=$IcKMax p0=$IcP0)"
    $cmd = "/opt/spark/bin/spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 /workspace/jobs/batch/network_ic.py --input /workspace/$CuratedDir --output /workspace/$OutputDir --top-actors $IcTopActors --mc-runs $IcMcRuns --k-max $IcKMax --p0 $IcP0 --p-max $IcPMax"
    docker compose run --rm spark-batch $cmd
    if ($LASTEXITCODE -ne 0) { throw "network_ic.py spark-submit failed ($LASTEXITCODE)" }
} else {
    Write-Host "`n[3/4] spark-submit network_depth.py --phase $Phase"
    $cmd = "/opt/spark/bin/spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=32 /workspace/jobs/batch/network_depth.py --input /workspace/$CuratedDir --output /workspace/$OutputDir --phase $Phase --top-n $TopN --als-rank $AlsRank --als-iter $AlsIter --gmm-k $GmmK --weeks $Weeks"
    docker compose run --rm spark-batch $cmd
    if ($LASTEXITCODE -ne 0) { throw "network_depth.py spark-submit failed ($LASTEXITCODE)" }
}

# ── Step 4: load ONLY the network-depth parquet folders into ClickHouse
if (-not $SkipLoad) {
    Write-Host "`n[4/4] load_batch_metrics_to_clickhouse.py --only network"
    if (-not (Test-Path $OutputDir)) {
        throw "output directory '$OutputDir' was not produced — skipping load"
    }
    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    $pythonExe = if (Test-Path $venvPython) { $venvPython } else { "python" }
    & $pythonExe -m scripts.load_batch_metrics_to_clickhouse --input $OutputDir --only network --skip-missing
    if ($LASTEXITCODE -ne 0) { throw "loader failed ($LASTEXITCODE)" }
} else {
    Write-Host "`n[4/4] skipping loader (-SkipLoad)"
}

Write-Host "`n=========================================================="
Write-Host " network-depth pipeline finished (phase $Phase)"
Write-Host "=========================================================="
