param(
    [Parameter(Position = 0)]
    [string]$Endpoints = $env:WHITEWATER_ENDPOINTS,
    [string]$ApiKey = $env:WHITEWATER_API_KEY,
    [string]$Execute,
    [string]$File,
    [string]$RequestId,
    [switch]$Json,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'

function Show-Usage {
    Write-Host @'
wcl-cli - Whitewater Control Language shell

Usage:
  wcl-cli
  wcl-cli "host1:7070;host2:7070"
  wcl-cli -Execute "SHOW FEEDS;"
  wcl-cli -Execute "CREATE SPACE orders;" -RequestId <uuid>
  wcl-cli -File commands.wcl
  wcl-cli -Json

Environment:
  WHITEWATER_API_KEY    Admin API Bearer key
  WHITEWATER_ENDPOINTS  Semicolon-separated Admin API endpoints
'@
}

function ConvertFrom-SecureValue {
    param([Security.SecureString]$SecureValue)
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureValue)
    try {
        [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Normalize-Endpoint {
    param([string]$Endpoint)
    $value = $Endpoint.Trim().TrimEnd('/')
    if (-not $value) { return $null }
    if ($value -notmatch '^https?://') { $value = "http://$value" }
    $value
}

function Get-EndpointList {
    param([string]$Value)
    if (-not $Value) { $Value = '127.0.0.1:7071' }
    $result = @($Value -split '[;,]' | ForEach-Object { Normalize-Endpoint $_ } | Where-Object { $_ })
    if ($result.Count -eq 0) { throw 'No valid Whitewater endpoints were provided.' }
    $result
}

function Test-StatementComplete {
    param([string]$Statement)
    $quoted = $false
    $lastUnquoted = $null
    for ($index = 0; $index -lt $Statement.Length; $index++) {
        $character = $Statement[$index]
        if ($character -eq "'") {
            if ($quoted -and $index + 1 -lt $Statement.Length -and $Statement[$index + 1] -eq "'") {
                $index++
            } else {
                $quoted = -not $quoted
            }
        } elseif (-not $quoted -and -not [char]::IsWhiteSpace($character)) {
            $lastUnquoted = $character
        }
    }
    -not $quoted -and $lastUnquoted -eq ';'
}

function Invoke-WhitewaterWcl {
    param(
        [string]$Statement,
        [string[]]$EndpointList,
        [string]$Credential,
        [string]$RequestedId
    )
    $headers = @{ Authorization = "Bearer $Credential" }
    $requestId = if ($RequestedId) { $RequestedId } else { [guid]::NewGuid().ToString() }
    $body = @{ request_id = $requestId; script = $Statement } | ConvertTo-Json -Compress
    $failures = @()
    foreach ($endpoint in $EndpointList) {
        try {
            $response = Invoke-RestMethod -Method Post -Uri "$endpoint/v1/admin/wcl" -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 5
            $script:LastEndpoint = $endpoint
            return $response
        } catch {
            $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            if ($status -eq 401) { throw "Authentication failed at $endpoint. Check WHITEWATER_API_KEY." }
            if ($status -eq 400) {
                $message = if ($_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
                throw "Whitewater rejected the WCL statement: $message"
            }
            $failures += "${endpoint}: $($_.Exception.Message)"
        }
    }
    throw "No Whitewater Admin API endpoint completed request $requestId. Retry with care; the outcome may be unknown.`n$($failures -join "`n")"
}

function Show-Execution {
    param($Execution, [bool]$AsJson)
    if ($AsJson) {
        $Execution | ConvertTo-Json -Depth 30
        return
    }
    foreach ($result in @($Execution.results)) {
        Write-Host "$($result.statement): $($result.message)" -ForegroundColor Green
        if ($null -ne $result.data) {
            $result.data | ConvertTo-Json -Depth 30
        }
    }
    Write-Host "revision: $($Execution.revision) ($($Execution.authority)) via $script:LastEndpoint" -ForegroundColor DarkGray
    if ($Execution.warning) { Write-Warning $Execution.warning }
}

function Show-ShellHelp {
    Write-Host @'
Enter WCL ending with a semicolon. Multi-line statements are supported.
  \g          Execute the buffered statement without a semicolon
  \clear      Clear the buffered statement
  \json       Toggle complete JSON output
  \endpoints  Show configured endpoints and the last successful endpoint
  \help       Show this help
  \quit       Exit
'@
}

if ($Help) {
    Show-Usage
    exit 0
}

$endpointList = Get-EndpointList $Endpoints
if (-not $ApiKey) {
    $secureKey = Read-Host 'Whitewater API key' -AsSecureString
    $ApiKey = ConvertFrom-SecureValue $secureKey
}
if (-not $ApiKey) { throw 'An Admin API key is required.' }

if ($File) {
    $Execute = Get-Content -Raw -Path $File
}

if ($Execute) {
    Show-Execution (Invoke-WhitewaterWcl $Execute $endpointList $ApiKey $RequestId) $Json.IsPresent
    exit 0
}

Write-Host 'Whitewater Control Language shell' -ForegroundColor Cyan
Write-Host "Endpoints: $($endpointList -join ', ')"
Write-Host 'All commands use the authenticated Admin API. Type \help for help.'
$buffer = ''
$jsonOutput = $Json.IsPresent
while ($true) {
    $prompt = if ($buffer) { '       ...> ' } else { 'whitewater> ' }
    $line = Read-Host $prompt
    $trimmed = $line.Trim()
    if (-not $buffer -and $trimmed.StartsWith('\')) {
        switch ($trimmed) {
            { $_ -in '\q', '\quit', '\exit' } { exit 0 }
            { $_ -in '\h', '\help' } { Show-ShellHelp; continue }
            '\json' { $jsonOutput = -not $jsonOutput; Write-Host "JSON output $(if ($jsonOutput) { 'enabled' } else { 'disabled' })"; continue }
            '\clear' { $buffer = ''; continue }
            '\endpoints' { Write-Host "Configured: $($endpointList -join ', ')"; Write-Host "Last successful: $script:LastEndpoint"; continue }
            '\g' { Write-Host 'No buffered statement.'; continue }
            default { Write-Warning "Unknown shell command: $trimmed"; continue }
        }
    }
    if ($trimmed -eq '\g') {
        if (-not $buffer) { Write-Host 'No buffered statement.'; continue }
    } else {
        $buffer += "$line`n"
    }
    if ($trimmed -ne '\g' -and -not (Test-StatementComplete $buffer)) { continue }
    try {
        Show-Execution (Invoke-WhitewaterWcl $buffer.Trim() $endpointList $ApiKey $null) $jsonOutput
    } catch {
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    }
    $buffer = ''
}
