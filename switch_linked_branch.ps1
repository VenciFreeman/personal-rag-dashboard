param(
    [string]$Branch = ""
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if ([string]::IsNullOrWhiteSpace($Branch)) {
    $Branch = (git branch --show-current).Trim()
}
if ([string]::IsNullOrWhiteSpace($Branch)) {
    throw "Cannot determine target branch. Pass -Branch explicitly."
}

Write-Host "[super] target branch: $Branch"
$superHasBranch = @(git branch --list $Branch) -ne $null -and (git branch --list $Branch).Count -gt 0
if (-not $superHasBranch) {
    git checkout -b $Branch
} else {
    git checkout $Branch
}

$submodulePaths = @(
    git config -f .gitmodules --get-regexp "^submodule\..*\.path$" |
    ForEach-Object { ($_ -split "\s+", 2)[1] }
)

foreach ($path in $submodulePaths) {
    if ([string]::IsNullOrWhiteSpace($path)) { continue }
    Write-Host "[submodule] $path -> $Branch"

    git -C $path fetch origin --prune

    $hasRemoteBranch = (git -C $path ls-remote --heads origin $Branch)
    if (-not [string]::IsNullOrWhiteSpace($hasRemoteBranch)) {
        $hasLocalBranch = @(git -C $path branch --list $Branch) -ne $null -and (git -C $path branch --list $Branch).Count -gt 0
        if (-not $hasLocalBranch) {
            git -C $path checkout -b $Branch --track origin/$Branch
        } else {
            git -C $path checkout $Branch
            git -C $path pull --ff-only origin $Branch
        }
    } else {
        Write-Host "  [warn] origin/$Branch not found for $path, keeping local HEAD"
    }
}

Write-Host "[done] branch linkage completed"
