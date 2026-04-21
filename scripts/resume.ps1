# One-click resume: start ClickHouse (with persistent volume) + Next.js dev.
# Data is NEVER deleted. Idempotent — safe to run every day.
#
# What this does:
#   1. Checks Docker is up.
#   2. Warns if the migration to the named volume has not been performed yet
#      (and offers to run it).
#   3. `docker compose up -d clickhouse` (no recreate — just start if stopped,
#      create if missing).
#   4. Waits until ClickHouse answers HTTP 200 on /ping.
#   5. Launches the Next.js dev server (on port 3000 or next free port).
#
# What this does NOT do:
#   - Does NOT run `docker compose down` (that would destroy ephemeral state).
#   - Does NOT re-run Spark / loader. Your ClickHouse tables from the
#     previous session are reused as-is. To refresh metrics:
#       scripts\run_batch_pipeline.ps1

param(
    [int]$Port = 3000,
    [switch]$SkipClickHouseCheck
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker command not found. Please install / start Docker Desktop."
}

# --- 1. Sanity: is docker-compose.yml using the named volume yet? ---------
$composeFile = Join-Path $projectRoot "docker-compose.yml"
$composeHasVolume = (Get-Content $composeFile -Raw) -match "clickhouse_data:/var/lib/clickhouse"

# --- 2. Does the existing container already mount the volume? --------------
$container = "gba-clickhouse"
$containerExists = $null -ne (docker ps -a --filter "name=^/$container$" --format "{{.Names}}")
$usesVolume = $false
if ($containerExists) {
    $mounts = docker inspect -f "{{range .Mounts}}{{.Name}}|{{end}}" $container 2>$null
    $usesVolume = ($mounts -split "\|") -contains "gba_clickhouse_data"
}

if ($composeHasVolume -and $containerExists -and -not $usesVolume) {
    Write-Warning ""
    Write-Warning "ClickHouse container exists but is NOT using the persistent volume yet."
    Write-Warning "Your data currently lives only in the container writable layer and will"
    Write-Warning "be lost if the container is ever recreated (e.g. after 'docker compose up'"
    Write-Warning "detects config drift)."
    Write-Warning ""
    $m = Read-Host "Run scripts/migrate_clickhouse_volume.ps1 now? (y/N)"
    if ($m -match '^(y|Y|yes)$') {
        & "$PSScriptRoot\migrate_clickhouse_volume.ps1"
        if ($LASTEXITCODE -ne 0) { throw "Migration script failed." }
    } else {
        Write-Warning "Skipping migration. You can run it later with:"
        Write-Warning "  scripts\migrate_clickhouse_volume.ps1"
    }
}

# --- 3. Start ClickHouse (idempotent) --------------------------------------
Write-Host "Starting ClickHouse (data persists across restarts)..."
docker compose up -d clickhouse
if ($LASTEXITCODE -ne 0) { throw "docker compose up -d clickhouse failed." }

if (-not $SkipClickHouseCheck) {
    Write-Host "Waiting for ClickHouse HTTP endpoint..."
    $ok = $false
    for ($i = 0; $i -lt 20; $i++) {
        try {
            $r = Invoke-WebRequest -Uri "http://localhost:8123/ping" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { $ok = $true; break }
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $ok) {
        Write-Warning "ClickHouse /ping did not return 200 within 20s. Check: docker logs $container"
    } else {
        $tableCount = docker exec $container clickhouse-client -u analytics --password analytics -d github_analytics --query "SELECT count() FROM system.tables WHERE database='github_analytics'" 2>$null
        Write-Host "  github_analytics has $tableCount tables."
    }
}

# --- 4. Start Next.js ------------------------------------------------------
Write-Host ""
Write-Host "Launching Next.js dev server..."
& "$PSScriptRoot\start_web.ps1" -Port $Port
