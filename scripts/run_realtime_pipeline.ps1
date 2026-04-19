param(
    [string]$InputFile = "data/raw_single/2024-01-03-05.json",
    [int]$Speedup = 1000,
    [switch]$VerifyOnly
)

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "Realtime input file: $InputFile"

if (-not $VerifyOnly) {
    Write-Host "Step 1: replay fixed one-hour file into Kafka"
    python jobs/replay/replay_gharchive_to_kafka.py --input $InputFile --topic github_events --speedup $Speedup
}

Write-Host "Step 2: verify realtime ClickHouse result tables"
python scripts/verify_realtime_clickhouse.py
