# Migrate existing ClickHouse container's in-container data into the
# `gba_clickhouse_data` named volume (declared in docker-compose.yml).
#
# Safe to run multiple times:
#   - If migration already happened (container already mounts the named volume
#     AND the volume has non-empty data), this script is a no-op.
#   - If the ClickHouse container does NOT exist yet, we just do
#     `docker compose up -d clickhouse` (starts with fresh empty volume).
#   - Otherwise we:
#       1. Snapshot the current container into a throw-away image.
#       2. `docker compose down` the clickhouse service (keeps volumes, just
#          removes the old container).
#       3. Create a helper container from the snapshot AND mount the empty
#          named volume, then `cp -a /var/lib/clickhouse/. /mnt/vol/` so
#          everything — including ClickHouse-owned permissions — lands in the
#          volume intact.
#       4. `docker compose up -d clickhouse` — new container boots with the
#          populated volume.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts/migrate_clickhouse_volume.ps1

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker command not found. Please install / start Docker Desktop."
}

$container = "gba-clickhouse"
$volName = "gba_clickhouse_data"
$snapImage = "gba-clickhouse-pre-migration:snap"

function Test-VolumeHasData {
    param([string]$Volume)
    # Returns $true if the named volume exists AND has at least one entry in
    # /var/lib/clickhouse/data (where table data lives).
    $exists = docker volume inspect $Volume 2>$null
    if ($LASTEXITCODE -ne 0) { return $false }
    $listing = docker run --rm -v "${Volume}:/v" alpine:3.19 sh -c "ls -A /v/data 2>/dev/null || true" 2>$null
    return -not [string]::IsNullOrWhiteSpace($listing)
}

function Test-ContainerUsesVolume {
    param([string]$Container, [string]$Volume)
    $inspect = docker inspect -f "{{range .Mounts}}{{.Name}}|{{end}}" $Container 2>$null
    if ($LASTEXITCODE -ne 0) { return $false }
    return ($inspect -split "\|") -contains $Volume
}

$containerExists = $null -ne (docker ps -a --filter "name=^/$container$" --format "{{.Names}}")

if (-not $containerExists) {
    Write-Host "ClickHouse container does not exist. Starting fresh with the new volume..."
    docker compose up -d clickhouse
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed." }
    Write-Host "Done. The new container mounts volume '$volName'."
    exit 0
}

$alreadyUsingVolume = Test-ContainerUsesVolume -Container $container -Volume $volName
$volumeHasData = Test-VolumeHasData -Volume $volName

if ($alreadyUsingVolume -and $volumeHasData) {
    Write-Host "Migration appears already done: '$container' already mounts '$volName' and the volume has data."
    Write-Host "No action needed."
    exit 0
}

Write-Host "=== ClickHouse volume migration ==="
Write-Host "container         : $container"
Write-Host "target volume     : $volName"
Write-Host "already on volume : $alreadyUsingVolume"
Write-Host "volume has data   : $volumeHasData"
Write-Host ""
$resp = Read-Host "This will stop the ClickHouse container, snapshot it, recreate it with the named volume, and copy data into the volume. Proceed? (y/N)"
if ($resp -notmatch '^(y|Y|yes)$') {
    Write-Host "Aborted by user."
    exit 0
}

Write-Host ""
Write-Host "Step 1/6 : snapshot current container as '$snapImage'"
docker commit $container $snapImage
if ($LASTEXITCODE -ne 0) { throw "docker commit failed." }

Write-Host "Step 2/6 : docker compose down (stops clickhouse, keeps volumes)"
docker compose rm -sf clickhouse
if ($LASTEXITCODE -ne 0) { throw "docker compose rm failed." }

Write-Host "Step 3/6 : ensure named volume '$volName' exists"
docker volume create $volName | Out-Null

Write-Host "Step 4/6 : copy data from snapshot -> volume (preserving owner/perms)"
$copyCmd = "mkdir -p /dst && cp -a /src/var/lib/clickhouse/. /dst/ && echo copy-ok"
docker run --rm `
    -v "${volName}:/dst" `
    --entrypoint "/bin/sh" `
    $snapImage `
    -c $copyCmd
if ($LASTEXITCODE -ne 0) { throw "snapshot -> volume copy failed." }

# Optional — do the same for logs volume so past logs are preserved.
$logsVol = "gba_clickhouse_logs"
docker volume create $logsVol | Out-Null
docker run --rm `
    -v "${logsVol}:/dst" `
    --entrypoint "/bin/sh" `
    $snapImage `
    -c "mkdir -p /dst && cp -a /src/var/log/clickhouse-server/. /dst/ 2>/dev/null || true" | Out-Null

Write-Host "Step 5/6 : boot new ClickHouse container with the populated volume"
docker compose up -d clickhouse
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed." }

Write-Host "Step 6/6 : verify tables are visible"
Start-Sleep -Seconds 5
$rowCount = docker exec gba-clickhouse clickhouse-client -u analytics --password analytics -d github_analytics --query "SELECT count() FROM system.tables WHERE database='github_analytics'" 2>$null
if (-not $rowCount) {
    Write-Warning "Could not query ClickHouse yet. Give it a few seconds and run:"
    Write-Warning "  docker exec gba-clickhouse clickhouse-client -u analytics --password analytics -d github_analytics --query 'SHOW TABLES'"
}
else {
    Write-Host "github_analytics has $rowCount tables — migration OK."
}

Write-Host ""
Write-Host "=== Migration complete ==="
Write-Host "The snapshot image '$snapImage' is kept as a rollback point."
Write-Host "Once you've verified everything works, you can delete it with:"
Write-Host "  docker image rm $snapImage"
