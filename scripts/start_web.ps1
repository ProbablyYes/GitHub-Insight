param(
    [int]$Port = 3000,
    [switch]$ForceRestart,
    [switch]$NoBrowser
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

function Test-FrontendHealthy {
    param(
        [Parameter(Mandatory = $true)][int]$TargetPort,
        [int]$TimeoutSec = 3
    )

    try {
        $response = Invoke-WebRequest -Uri "http://localhost:$TargetPort/" -UseBasicParsing -TimeoutSec $TimeoutSec
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500)
    }
    catch {
        return $false
    }
}

function Open-FrontendUrl {
    param(
        [Parameter(Mandatory = $true)][int]$TargetPort
    )

    if ($NoBrowser) {
        return
    }

    Start-Process "http://localhost:$TargetPort/offline/overview" | Out-Null
}

function Stop-DevServer {
    param(
        [Parameter(Mandatory = $true)][int]$ProcessId,
        [Parameter(Mandatory = $true)][string]$Reason
    )

    Write-Host $Reason -ForegroundColor Yellow
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
    Remove-Item -Force $lockPath -ErrorAction SilentlyContinue
}

Write-Host "Step 1: inspect existing Next.js dev server"

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
        if ($existing) {
            $frontendHealthy = $false
            if ($portAlive -and $lock.port) {
                $frontendHealthy = Test-FrontendHealthy -TargetPort ([int]$lock.port)
            }

            if ($ForceRestart) {
                Stop-DevServer -ProcessId $lock.pid -Reason "ForceRestart set: stopping existing Next.js dev server (pid=$($lock.pid), port=$($lock.port))."
            }
            elseif ($portAlive -and $frontendHealthy) {
                $url = if ($lock.appUrl) { $lock.appUrl } else { "http://localhost:$($lock.port)" }
                Write-Host "Detected healthy Next.js dev server (pid=$($lock.pid), port=$($lock.port))"
                Write-Host "Opening: $url/offline/overview"
                Open-FrontendUrl -TargetPort ([int]$lock.port)
                return
            }
            else {
                Stop-DevServer -ProcessId $lock.pid -Reason "Detected stale Next.js dev server (pid=$($lock.pid), port=$($lock.port)); restarting it."
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
            $frontendHealthy = Test-FrontendHealthy -TargetPort ([int]$running.port)

            if ($ForceRestart) {
                Stop-DevServer -ProcessId $running.pid -Reason "ForceRestart set: stopping existing Next.js dev server (pid=$($running.pid), port=$($running.port))."
            }
            elseif ($frontendHealthy) {
                Write-Host "Detected running dev server (pid=$($running.pid))"
                Write-Host "Opening: http://localhost:$($running.port)/offline/overview"
                Open-FrontendUrl -TargetPort ([int]$running.port)
                return
            }
            else {
                Stop-DevServer -ProcessId $running.pid -Reason "Detected unresponsive Next.js dev server (pid=$($running.pid), port=$($running.port)); restarting it."
            }
        }
    }
}

$selectedPort = Get-FreePort -StartPort $Port
if ($selectedPort -ne $Port) {
    Write-Host "Port $Port is in use; using $selectedPort instead."
}

Write-Host "Step 2: start Next.js dev server (frontend)"
Write-Host "Opening: http://localhost:$selectedPort/offline/overview"
Open-FrontendUrl -TargetPort $selectedPort

# Next.js accepts -p/--port; we also set PORT for compatibility
$env:PORT = "$selectedPort"
npm run dev --prefix apps/web -- -p $selectedPort
