# One-click launcher for Windows — sets up a local venv and runs the deploy script.
# Usage: .\deploy\deploy.ps1 deploy\clients\acme.json [--skip-infra] [--yes]

param(
    [Parameter(Mandatory = $true)][string]$ClientConfig,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$VenvDir = Join-Path $RepoRoot ".deploy_venv"

$py = Get-Command python -ErrorAction SilentlyContinue
if (-not $py) {
    Write-Error "python not found on PATH — install Python 3.9+ first (https://www.python.org/downloads/)."
    exit 1
}

if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating virtual environment at $VenvDir ..."
    python -m venv $VenvDir
}

$venvPython = Join-Path $VenvDir "Scripts\python.exe"

Write-Host "Installing dependencies ..."
& $venvPython -m pip install --quiet --upgrade pip
& $venvPython -m pip install --quiet -r (Join-Path $RepoRoot "requirements.txt")

Write-Host "Running one-click deploy ..."
& $venvPython (Join-Path $ScriptDir "one_click_deploy.py") --client-config $ClientConfig @ExtraArgs
