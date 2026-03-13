param(
    [switch]$WhatIf
)

# Sync scripts and tokenizer assets from scripts/ to release/app/scripts/.
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\scripts\sync_to_release.ps1
#   powershell -ExecutionPolicy Bypass -File .\scripts\sync_to_release.ps1 -WhatIf

$ErrorActionPreference = "Stop"

# Resolve source/destination relative to this script so it works from any cwd.
$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$srcScripts = $PSScriptRoot
$dstScripts = Join-Path $repoRoot "release\app\scripts"

if (-not (Test-Path -Path $dstScripts)) {
    New-Item -ItemType Directory -Path $dstScripts -Force | Out-Null
}

function Invoke-RobocopySync {
    param(
        [Parameter(Mandatory = $true)] [string]$Source,
        [Parameter(Mandatory = $true)] [string]$Destination,
        [Parameter(Mandatory = $true)] [string[]]$Files,
        [string[]]$ExtraArgs = @()
    )

    $args = @($Source, $Destination) + $Files + $ExtraArgs

    if ($WhatIf) {
        Write-Host "[WhatIf] robocopy $($args -join ' ')"
        return
    }

    & robocopy @args | Out-Host
    $code = $LASTEXITCODE
    # Robocopy uses non-standard exit codes: 0-7 are success/warning, >7 is failure.
    if ($code -gt 7) {
        throw "robocopy failed with exit code $code"
    }
}

Write-Host "Syncing Python scripts to release..."
Invoke-RobocopySync -Source $srcScripts -Destination $dstScripts -Files @("*.py") -ExtraArgs @(
    "/E",
    "/XO",
    "/R:1",
    "/W:1",
    "/XD", "__pycache__", ".pyi_build",
    "/XF", "*.pyc"
)

$srcTokenizer = Join-Path $srcScripts "deepseek_v3_tokenizer"
$dstTokenizer = Join-Path $dstScripts "deepseek_v3_tokenizer"
if (Test-Path -Path $srcTokenizer) {
    Write-Host "Syncing tokenizer assets..."
    Invoke-RobocopySync -Source $srcTokenizer -Destination $dstTokenizer -Files @("*.*") -ExtraArgs @(
        "/E",
        "/XO",
        "/R:1",
        "/W:1",
        "/XD", "__pycache__",
        "/XF", "*.pyc"
    )
}

Write-Host "Sync complete: scripts -> release/app/scripts"
