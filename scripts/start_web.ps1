param(
    [int]$Port = 3000,
    [switch]$ForceRestart
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Get-FreePort {
    param(
        [Parameter(Mandatory = $true)][int]$StartPort,
        [int]$MaxTries = 20
    )

    for ($p = $StartPort; $p -lt ($StartPort + $MaxTries); $p++) {
        $listener = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $listener) {
            return $p
        }
    }

    throw "No free port found in range $StartPort..$($StartPort + $MaxTries - 1)."
}

function Get-RunningNodeListener {
    param(
        [Parameter(Mandatory = $true)][int]$StartPort,
        [int]$MaxTries = 20
    )

    for ($p = $StartPort; $p -lt ($StartPort + $MaxTries); $p++) {
        $listener = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $listener) {
            continue
        }

        $proc = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -ieq "node") {
            return [PSCustomObject]@{ pid = $listener.OwningProcess; port = $p }
        }
    }

    return $null
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker command not found. Please install/start Docker Desktop."
}

Write-Host "Step 1: start ClickHouse (backend)"
docker compose up -d clickhouse
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed with exit code $LASTEXITCODE"
}

Write-Host "Step 2: start Next.js dev server (frontend)"

$webRoot = Join-Path $projectRoot "apps\web"
$lockPath = Join-Path $webRoot ".next\dev\lock"

if (Test-Path $lockPath) {
    $lock = $null
    $lockReadError = $false
    try {
        $lock = (Get-Content $lockPath -Raw | ConvertFrom-Json)
    }
    catch {
        $lock = $null
        $lockReadError = $true
    }

    if ($lock -and $lock.pid) {
        $existing = Get-Process -Id $lock.pid -ErrorAction SilentlyContinue
        # Verify the claimed port is actually LISTENING (and owned by this pid).
        # If pid is alive but the port is not listening, the dev server has
        # become a zombie (HMR/compiler crash) — kill it and launch a fresh one.
        $portAlive = $false
        if ($existing -and $lock.port) {
            $listener = Get-NetTCPConnection -LocalPort ([int]$lock.port) -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($listener -and $listener.OwningProcess -eq $lock.pid) { $portAlive = $true }
        }
        if ($existing -and -not $portAlive) {
            Write-Host "Lockfile points at pid=$($lock.pid) but port $($lock.port) is not listening." -ForegroundColor Yellow
            Write-Host "Killing zombie dev server..." -ForegroundColor Yellow
            Stop-Process -Id $lock.pid -Force -ErrorAction SilentlyContinue
            Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
            $existing = $null
        }
        if ($existing) {
            if ($ForceRestart) {
                Write-Host "Stopping existing Next.js dev server (pid=$($lock.pid), port=$($lock.port))"
                Stop-Process -Id $lock.pid -Force -ErrorAction SilentlyContinue
                Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
            }
            else {
                $url = if ($lock.appUrl) { $lock.appUrl } else { "http://localhost:$($lock.port)" }
                Write-Host "Detected running Next.js dev server (pid=$($lock.pid))"
                Write-Host "Opening: $url/offline/overview"
                return
            }
        }
        else {
            Write-Host "Found stale Next.js dev lockfile; removing it."
            Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
        }
    }
    elseif ($lockReadError) {
        # On Windows, the lock file can be exclusively locked by the running Next.js process.
        # Fall back to detecting an existing node listener on common dev ports.
        $running = Get-RunningNodeListener -StartPort $Port -MaxTries 20
        if (-not $running) {
            $running = Get-RunningNodeListener -StartPort 3000 -MaxTries 20
        }

        if ($running) {
            if ($ForceRestart) {
                Write-Host "Stopping existing dev server (pid=$($running.pid), port=$($running.port))"
                Stop-Process -Id $running.pid -Force -ErrorAction SilentlyContinue
                Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
            }
            else {
                Write-Host "Detected running dev server (pid=$($running.pid))"
                Write-Host "Opening: http://localhost:$($running.port)/offline/overview"
                return
            }
        }
    }
}

$selectedPort = Get-FreePort -StartPort $Port
if ($selectedPort -ne $Port) {
    Write-Host "Port $Port is in use; using $selectedPort instead."
}

Write-Host "Opening: http://localhost:$selectedPort/offline/overview"

# Next.js accepts -p/--port; we also set PORT for compatibility
$env:PORT = "$selectedPort"
npm run dev --prefix apps/web -- -p $selectedPort
