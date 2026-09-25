param(
    [string]$ApiKey = 'whitewater-local-development-admin-key',
    [string]$Endpoints = '127.0.0.1:7071;127.0.0.1:7072;127.0.0.1:7073'
)

$ErrorActionPreference = 'Stop'
$installDirectory = Join-Path $env:USERPROFILE '.local\bin'
New-Item -ItemType Directory -Force -Path $installDirectory | Out-Null
Copy-Item -Force -Path (Join-Path $PSScriptRoot 'wcl-cli.ps1') -Destination (Join-Path $installDirectory 'wcl-cli.ps1')
@'
@echo off
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0wcl-cli.ps1" %*
'@ | Set-Content -Encoding Ascii -Path (Join-Path $installDirectory 'wcl-cli.cmd')

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$pathEntries = @($userPath -split ';' | Where-Object { $_ })
if ($installDirectory -notin $pathEntries) {
    [Environment]::SetEnvironmentVariable('Path', (($pathEntries + $installDirectory) -join ';'), 'User')
}
[Environment]::SetEnvironmentVariable('WHITEWATER_API_KEY', $ApiKey, 'User')
[Environment]::SetEnvironmentVariable('WHITEWATER_ENDPOINTS', $Endpoints, 'User')

Write-Host "Installed wcl-cli in $installDirectory"
Write-Host 'Open a new terminal, then run: wcl-cli'
