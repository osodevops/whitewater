param(
    [ValidateSet('up', 'scale', 'autoscale', 'status', 'down', 'smoke')]
    [string]$Action = 'up',
    [ValidateRange(3, 100)]
    [int]$Nodes = 3,
    [ValidateRange(3, 100)]
    [int]$MinNodes = 3,
    [ValidateRange(1, 100)]
    [int]$MaxNodes = 20,
    [ValidateRange(2, 3600)]
    [int]$SampleSeconds = 10,
    [double]$ScaleOutThreshold = 0.75,
    [double]$ScaleInThreshold = 0.20,
    [ValidateRange(1, 360)]
    [int]$ScaleOutSamples = 6,
    [ValidateRange(1, 3600)]
    [int]$ScaleInSamples = 30,
    [ValidateRange(0, 3600)]
    [int]$CooldownSamples = 12,
    [double]$TargetRequestsPerSecond = 2000,
    [double]$TargetAppendsPerSecond = 1000,
    [double]$TargetAppendBytesPerSecond = 67108864,
    [double]$TargetInFlightPerNode = 100
)

$ErrorActionPreference = 'Stop'
$ComposeFile = Join-Path $PSScriptRoot '..\compose.cluster.yml'

function Get-NodeSnapshots {
    $snapshots = @()
    foreach ($id in @(docker compose -f $ComposeFile ps -q node)) {
        $name = (docker inspect $id --format '{{.Name}}').TrimStart('/')
        $mapping = @(docker port $id 7070/tcp)[0]
        if (-not $mapping) { continue }
        $port = [int]($mapping -replace '^.*:', '')
        $baseUrl = "http://127.0.0.1:$port"
        try {
            $metrics = Invoke-RestMethod -Uri "$baseUrl/v1/node/metrics" -TimeoutSec 3
            $snapshots += [pscustomobject]@{ Id = $id; Name = $name; BaseUrl = $baseUrl; Metrics = $metrics }
        } catch {
            Write-Warning "Unable to sample $name at ${baseUrl}: $($_.Exception.Message)"
        }
    }
    return @($snapshots)
}

function Start-Autoscaler {
    if ($MinNodes -gt $MaxNodes) { throw 'MinNodes cannot exceed MaxNodes.' }
    $previous = @{}
    Write-Host "Autoscaling every ${SampleSeconds}s: nodes $MinNodes..$MaxNodes, out >= $ScaleOutThreshold for $ScaleOutSamples samples, in <= $ScaleInThreshold for $ScaleInSamples samples."
    Write-Host 'Scale-in remains blocked unless the highest-index node is empty and safe to remove.'

    while ($true) {
        $now = Get-Date
        $snapshots = @(Get-NodeSnapshots)
        if ($snapshots.Count -eq 0) {
            Start-Sleep -Seconds $SampleSeconds
            continue
        }

        $requestRate = 0.0
        $appendRate = 0.0
        $appendByteRate = 0.0
        $inFlight = 0.0
        $sampledDeltas = 0
        foreach ($snapshot in $snapshots) {
            $metrics = $snapshot.Metrics
            $inFlight += [double]$metrics.demand.requests_in_flight
            if ($previous.ContainsKey($metrics.node_id)) {
                $prior = $previous[$metrics.node_id]
                $elapsed = [Math]::Max(0.001, ($now - $prior.Time).TotalSeconds)
                $requestRate += [Math]::Max(0, [double]$metrics.demand.requests_total - [double]$prior.Metrics.demand.requests_total) / $elapsed
                $appendRate += [Math]::Max(0, [double]$metrics.demand.appends_total - [double]$prior.Metrics.demand.appends_total) / $elapsed
                $appendByteRate += [Math]::Max(0, [double]$metrics.demand.append_bytes_total - [double]$prior.Metrics.demand.append_bytes_total) / $elapsed
                $sampledDeltas++
            }
            $previous[$metrics.node_id] = [pscustomobject]@{ Time = $now; Metrics = $metrics }
        }

        $activeIds = @($snapshots | ForEach-Object { $_.Metrics.node_id })
        foreach ($nodeId in @($previous.Keys)) {
            if ($nodeId -notin $activeIds) { $previous.Remove($nodeId) }
        }
        if ($sampledDeltas -eq 0) {
            Start-Sleep -Seconds $SampleSeconds
            continue
        }

        $nodeCount = $snapshots.Count
        $requestPressure = [double]$requestRate / [Math]::Max(1, $nodeCount * $TargetRequestsPerSecond)
        $appendPressure = [double]$appendRate / [Math]::Max(1, $nodeCount * $TargetAppendsPerSecond)
        $bytePressure = [double]$appendByteRate / [Math]::Max(1, $nodeCount * $TargetAppendBytesPerSecond)
        $inFlightPressure = [double]$inFlight / [Math]::Max(1, $nodeCount * $TargetInFlightPerNode)
        $pressure = [Math]::Max([Math]::Max($requestPressure, $appendPressure), [Math]::Max($bytePressure, $inFlightPressure))

        $highest = $snapshots | Sort-Object { [int](($_.Name -split '-')[-1]) } -Descending | Select-Object -First 1
        $removableNodes = if ($highest.Metrics.storage.safe_to_remove) { 1 } else { 0 }
        $policy = @{
            min_nodes = $MinNodes
            max_nodes = $MaxNodes
            scale_out_threshold = $ScaleOutThreshold
            scale_in_threshold = $ScaleInThreshold
            scale_out_samples = $ScaleOutSamples
            scale_in_samples = $ScaleInSamples
            cooldown_samples = $CooldownSamples
        }
        $recommendation = Invoke-RestMethod -Method Post -Uri "$($snapshots[0].BaseUrl)/v1/cluster/autoscale/recommend" -ContentType 'application/json' -Body (@{
            policy = $policy
            pressure = $pressure
            removable_nodes = $removableNodes
        } | ConvertTo-Json -Depth 5 -Compress) -TimeoutSec 3
        $actionName = $recommendation.decision.action
        Write-Host ('{0:u} nodes={1} pressure={2:N3} requests/s={3:N1} appends/s={4:N1} MiB/s={5:N2} decision={6}' -f $now, $nodeCount, $pressure, $requestRate, $appendRate, ($appendByteRate / 1MB), $actionName)

        if ($actionName -eq 'scale_out' -or $actionName -eq 'scale_in') {
            $target = [int]$recommendation.decision.target_nodes
            docker compose -f $ComposeFile up -d --scale node=$target --remove-orphans
            $previous = @{}
        }
        Start-Sleep -Seconds $SampleSeconds
    }
}

switch ($Action) {
    'up' {
        docker compose -f $ComposeFile up -d --build --scale node=$Nodes
    }
    'scale' {
        docker compose -f $ComposeFile up -d --scale node=$Nodes --remove-orphans
    }
    'autoscale' {
        Start-Autoscaler
    }
    'status' {
        docker compose -f $ComposeFile ps
    }
    'down' {
        docker compose -f $ComposeFile down -v --remove-orphans
    }
    'smoke' {
        $ids = @(docker compose -f $ComposeFile ps -q node)
        if ($ids.Count -ne $Nodes) {
            throw "Expected $Nodes nodes but Docker reports $($ids.Count)."
        }
        foreach ($id in $ids) {
            $members = docker exec $id curl --fail --silent http://127.0.0.1:7070/v1/cluster/members | ConvertFrom-Json
            if (@($members).Count -ne $Nodes) {
                throw "Node $id sees $(@($members).Count) members; expected $Nodes."
            }
        }
        Write-Host "All $Nodes nodes report complete membership."
    }
}
