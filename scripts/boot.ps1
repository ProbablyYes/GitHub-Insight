# ====================================================================
#  scripts\boot.ps1 — the ONE entry point you need for this project.
# ====================================================================
#
#  What it does (idempotent; re-run any time, safe):
#    1. Make sure Docker is up.
#    2. Start ClickHouse on the *persistent* named volume
#       (gba_clickhouse_data). Data survives container restarts and
#       reboots — it is only lost by an explicit `docker volume rm` or
#       `docker compose down -v`.
#    3. Check whether ClickHouse already has loaded batch_* data.
#         - empty   → automatically run the full compute pipeline:
#                       (a) docker compose build spark-batch
#                       (b) spark_job.py   (curated  → data/sample)
#                       (c) loader         (data/sample → ClickHouse)
#                       (d) network_depth phase 1 (ALS, layers, k-core,
#                           meta-path similarity, repo archetype)
#                       (e) network_depth phase 3 (temporal communities)
#                       (f) network_ic    (influence-cascade seeds)
#         - non-empty → skip the pipeline entirely (fast restart).
#    4. Launch the Next.js dev server on port 3000 (or next free port).
#
#  Usage:
#    .\scripts\boot.ps1                  # normal / daily start
#    .\scripts\boot.ps1 -ForceCompute    # recompute even if data present
#    .\scripts\boot.ps1 -SkipNetwork     # skip the slow network ML jobs
#    .\scripts\boot.ps1 -SkipWeb         # backend only (no dev server)
#
#  After the first successful run, the ClickHouse volume keeps the data.
#  Tomorrow, `boot.ps1` will detect the data is there, skip every Spark
#  step, and just bring the web server up in ~10 seconds.
# ====================================================================

param(
    [switch]$ForceCompute,
    [switch]$SkipNetwork,
    [switch]$SkipWeb,
    [int]$Port = 3000
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Write-Banner($msg) {
    Write-Host ""
    Write-Host ("=" * 72) -ForegroundColor Cyan
    Write-Host " $msg" -ForegroundColor Cyan
    Write-Host ("=" * 72) -ForegroundColor Cyan
}

# ── 0. sanity ────────────────────────────────────────────────────────────
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker not found. Please install / start Docker Desktop."
}

# ── 1. start ClickHouse on persistent volume ─────────────────────────────
Write-Banner "1/4  ClickHouse (persistent volume: gba_clickhouse_data)"
docker compose up -d clickhouse
if ($LASTEXITCODE -ne 0) { throw "docker compose up -d clickhouse failed ($LASTEXITCODE)" }

Write-Host "Waiting for ClickHouse HTTP /ping ..."
$chOk = $false
for ($i = 0; $i -lt 40; $i++) {
    try {
        $r = Invoke-WebRequest -Uri "http://localhost:8123/ping" -UseBasicParsing -TimeoutSec 2
        if ($r.StatusCode -eq 200) { $chOk = $true; break }
    } catch { Start-Sleep -Seconds 1 }
}
if (-not $chOk) {
    throw "ClickHouse /ping did not return 200 within 40s. Check: docker logs gba-clickhouse"
}

# Confirm the container is really on the named volume — otherwise the
# user's data is at risk of being wiped next time compose recreates.
$mounts = docker inspect -f "{{range .Mounts}}{{.Name}}|{{end}}" gba-clickhouse 2>$null
if (($mounts -split "\|") -notcontains "gba_clickhouse_data") {
    Write-Warning ""
    Write-Warning "gba-clickhouse is NOT using the gba_clickhouse_data volume!"
    Write-Warning "Data will be LOST if the container is ever recreated."
    Write-Warning "Fix now with:  scripts\migrate_clickhouse_volume.ps1"
    Write-Warning ""
}

# ── 2. probe whether data is already loaded ──────────────────────────────
Write-Banner "2/4  Check existing data in github_analytics"
$probeRaw = docker exec gba-clickhouse clickhouse-client -u analytics --password analytics -d github_analytics `
    --query "SELECT coalesce(sum(total_rows),0) FROM system.tables WHERE database='github_analytics' AND name LIKE 'batch_%'" 2>$null
$totalRows = 0
if ($probeRaw -match '^\d+$') { $totalRows = [int64]$probeRaw }
Write-Host "  batch_* total rows = $totalRows"

$needCompute = $ForceCompute.IsPresent -or ($totalRows -eq 0)

# ── 3. run pipeline if needed ────────────────────────────────────────────
if ($needCompute) {
    Write-Banner "3/4  Run batch pipeline (one-time cost — persists forever after)"

    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    $pythonExe  = if (Test-Path $venvPython) { $venvPython } else { "python" }

    # (a) spark-batch docker image
    Write-Host ""
    Write-Host "[a] docker compose build spark-batch" -ForegroundColor Green
    docker compose build spark-batch
    if ($LASTEXITCODE -ne 0) { throw "spark-batch build failed ($LASTEXITCODE)" }

    # (b) curate raw → parquet, only if curated is empty
    $curatedExists = (Test-Path "data/curated") -and (
        (Get-ChildItem "data/curated" -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue | Select-Object -First 1) -ne $null
    )
    if (-not $curatedExists) {
        Write-Host ""
        Write-Host "[b] curate_events.py  (data/raw -> data/curated)  [slow: ~10 min on 10 GB]" -ForegroundColor Green
        docker compose run --rm spark-batch "/opt/spark/bin/spark-submit /workspace/jobs/batch/curate_events.py --input /workspace/data/raw --output /workspace/data/curated"
        if ($LASTEXITCODE -ne 0) { throw "curate_events failed" }
    } else {
        Write-Host ""
        Write-Host "[b] data/curated already has parquet — skipping curate" -ForegroundColor DarkGreen
    }

    # (c) main aggregations
    Write-Host ""
    Write-Host "[c] spark_job.py  (data/curated -> data/sample)  [~10 min]" -ForegroundColor Green
    docker compose run --rm spark-batch "/opt/spark/bin/spark-submit --driver-memory 4g --conf spark.sql.shuffle.partitions=64 /workspace/jobs/batch/spark_job.py --input /workspace/data/curated --output /workspace/data/sample"
    if ($LASTEXITCODE -ne 0) { throw "spark_job failed" }

    # (d) load batch metrics into ClickHouse
    Write-Host ""
    Write-Host "[d] load_batch_metrics_to_clickhouse.py (data/sample)" -ForegroundColor Green
    & $pythonExe -m scripts.load_batch_metrics_to_clickhouse --input data/sample
    if ($LASTEXITCODE -ne 0) { throw "loader failed" }

    if (-not $SkipNetwork) {
        Write-Host ""
        Write-Host "[e] network_depth phase 1  (ALS + layers + k-core + meta-path + archetype)" -ForegroundColor Green
        & "$PSScriptRoot\run_network_only.ps1" -Phase 1 -SkipBuild
        if ($LASTEXITCODE -ne 0) { throw "network phase 1 failed" }

        Write-Host ""
        Write-Host "[f] network_depth phase 3  (temporal communities)" -ForegroundColor Green
        & "$PSScriptRoot\run_network_only.ps1" -Phase 3 -SkipBuild
        if ($LASTEXITCODE -ne 0) { throw "network phase 3 failed" }

        Write-Host ""
        Write-Host "[g] network_ic  (influence-cascade seeds)" -ForegroundColor Green
        & "$PSScriptRoot\run_network_only.ps1" -Phase 4 -SkipBuild
        if ($LASTEXITCODE -ne 0) { throw "network IC failed" }
    } else {
        Write-Host ""
        Write-Host "[e-g] -SkipNetwork set: network_* tables will remain empty" -ForegroundColor DarkYellow
    }

    Write-Host ""
    Write-Host "Pipeline complete. Data is now in the persistent volume." -ForegroundColor Green
}
else {
    Write-Banner "3/4  Data already loaded — skipping pipeline"
    Write-Host "Tip: re-run with -ForceCompute to rebuild everything from curated parquet."
}

# ── 4. start web ─────────────────────────────────────────────────────────
if (-not $SkipWeb) {
    Write-Banner "4/4  Next.js dev server"
    & "$PSScriptRoot\start_web.ps1" -Port $Port
}
else {
    Write-Host ""
    Write-Host "Skipping web (-SkipWeb). Data is ready in ClickHouse." -ForegroundColor DarkGreen
}
