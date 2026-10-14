param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"
$repo = $PSScriptRoot
$pythonExe = Join-Path (Split-Path -Parent $repo) ".venv\Scripts\python.exe"
$bridgeScript = Join-Path $repo "frontend\datashield_bridge.py"
$runtimeDir = Join-Path $repo ".runtime"
$bridgeHealth = "http://127.0.0.1:8099/health"

function Get-BridgeHealth {
    $response = & curl.exe --fail --silent --max-time 5 $bridgeHealth
    if ($LASTEXITCODE -ne 0 -or -not $response) { return $null }
    try { return ($response | ConvertFrom-Json) } catch { return $null }
}

Push-Location $repo
try {
    if ($Build) {
        docker compose up -d --build
    } else {
        docker compose up -d
    }
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }

    # docker-entrypoint-initdb.d runs only on an empty Postgres volume. Apply
    # the idempotent order-attribute migration on every launch so upgrades to
    # an existing local volume get the same columns as a fresh install.
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U samar -d datashield -f /docker-entrypoint-initdb.d/007_order_attributes.sql
    if ($LASTEXITCODE -ne 0) {
        throw "Postgres order-attribute migration failed with exit code $LASTEXITCODE"
    }

    $bridgeReady = $false
    $bridgeAvailable = $false
    try {
        $health = Get-BridgeHealth
        if (-not $health) { throw "bridge not responding" }
        $bridgeAvailable = $true
        $bridgeReady = $health.kafka_connected -eq $true
    } catch {
        # The host bridge is not running yet.
    }

    $bridgePortOpen = Test-NetConnection -ComputerName localhost -Port 8099 -InformationLevel Quiet -WarningAction SilentlyContinue
    if (-not $bridgeAvailable -and -not $bridgePortOpen) {
        if (-not (Test-Path $pythonExe)) {
            $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
            if (-not $pythonCommand) {
                throw "Python was not found. Install Python or create $pythonExe."
            }
            $pythonExe = $pythonCommand.Source
        }

        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        Start-Process -FilePath $pythonExe `
            -ArgumentList @($bridgeScript) `
            -WorkingDirectory $repo `
            -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $runtimeDir "bridge.stdout.log") `
            -RedirectStandardError (Join-Path $runtimeDir "bridge.stderr.log") | Out-Null
    }

    $deadline = (Get-Date).AddSeconds(120)
    do {
        Start-Sleep -Seconds 2
        try {
            $health = Get-BridgeHealth
            if ($health) {
                $bridgeAvailable = $true
                $bridgeReady = $health.kafka_connected -eq $true
                if ($bridgeReady) { break }
            }
        } catch {
            # Keep waiting while Docker and Kafka finish starting.
        }
    } while ((Get-Date) -lt $deadline)

    if (-not $bridgeReady) {
        throw "Dashboard bridge did not connect to Kafka. Check .runtime\bridge.stderr.log and docker compose logs."
    }

    Write-Host "E-commerce real-time sales pipeline guard is running. Kafka bridge is connected."
    $emdash = [char]0x2014
    $observatoryPage = Join-Path $repo "frontend\DataShield AI $emdash Pipeline Observatory.html"
    Write-Host "Open: $observatoryPage"
    Write-Host "Services: http://localhost:8099/services"
} finally {
    Pop-Location
}
