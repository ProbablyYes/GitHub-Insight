param(
    [string]$SourceFile = "data/sample/sample_events.json",
    [string]$TargetDir = "data/raw"
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$sourcePath = Join-Path $projectRoot $SourceFile
$targetDirectory = Join-Path $projectRoot $TargetDir
$targetPath = Join-Path $targetDirectory "2024-01-01-00.json"

if (-not (Test-Path $targetDirectory)) {
    New-Item -ItemType Directory -Path $targetDirectory | Out-Null
}

Copy-Item -Path $sourcePath -Destination $targetPath -Force
Write-Host "Copied sample events to $targetPath"
