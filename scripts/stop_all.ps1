# Safely stop all project containers WITHOUT removing them and WITHOUT
# touching volumes. Data in ClickHouse persists.
#
# Use this instead of `docker compose down`.

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "Stopping containers (data is preserved)..."
docker compose stop
if ($LASTEXITCODE -ne 0) { throw "docker compose stop failed." }

# Also stop the Next.js dev server if it's running
$webRoot = Join-Path $projectRoot "apps\web"
$lockPath = Join-Path $webRoot ".next\dev\lock"
if (Test-Path $lockPath) {
    try {
        $lock = Get-Content $lockPath -Raw | ConvertFrom-Json
        if ($lock -and $lock.pid) {
            $p = Get-Process -Id $lock.pid -ErrorAction SilentlyContinue
            if ($p) {
                Write-Host "Stopping Next.js dev server (pid=$($lock.pid))..."
                Stop-Process -Id $lock.pid -Force -ErrorAction SilentlyContinue
            }
            Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
        }
    } catch { }
}

Write-Host ""
Write-Host "All stopped. Data (ClickHouse tables, Kafka topics) is preserved."
Write-Host "Next time, just run: scripts\start_web.ps1 (or scripts\resume.ps1)"
