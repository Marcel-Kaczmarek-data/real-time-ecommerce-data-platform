<#
.SYNOPSIS
  Windows equivalent of the Makefile (same targets).
.EXAMPLE
  ./make.ps1 demo
  ./make.ps1 seed -Scale full
#>
param(
    [Parameter(Position = 0)][string]$Target = "help",
    [string]$Scale = "lite",
    [int]$HistoryDays = 120,
    [int]$StreamDays = 7,
    [int]$Events = 100000
)
$ErrorActionPreference = "Stop"
$Run = @("compose", "run", "--rm", "--no-deps", "platform")

function Invoke-Platform([string[]]$Cmd) {
    & docker @Run @Cmd
    if ($LASTEXITCODE -ne 0) { throw "command failed: $($Cmd -join ' ')" }
}
function Invoke-Compose([string[]]$CmdArgs) {
    & docker compose @CmdArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose $($CmdArgs -join ' ') failed" }
}

$targets = [ordered]@{
    "setup"     = { if (-not (Test-Path .env)) { Copy-Item .env.example .env }; Invoke-Compose @("build", "platform") }
    "up"        = { Invoke-Compose @("--profile", "dashboard", "--profile", "monitoring", "up", "-d") }
    "up-all"    = { Invoke-Compose @("--profile", "dashboard", "--profile", "monitoring", "--profile", "processing", "--profile", "orchestration", "up", "-d") }
    "airflow"   = { Invoke-Compose @("--profile", "orchestration", "up", "-d") }
    "down"      = { Invoke-Compose @("--profile", "*", "down") }
    "reset"     = {
        $ans = Read-Host "This deletes Kafka, Postgres, object-storage and checkpoint volumes. Type 'yes'"
        if ($ans -eq "yes") { Invoke-Compose @("--profile", "*", "down", "-v") } else { "aborted" }
    }
    "seed"      = {
        Invoke-Platform @("python", "-m", "data_generator.cli", "--scale", $Scale, "seed", "--history-days", "$HistoryDays", "--stream-days", "$StreamDays")
        Invoke-Platform @("python", "-m", "ingestion.kafka_producers.admin", "bootstrap")
    }
    "stream"    = {
        Invoke-Platform @("python", "-m", "ingestion.kafka_producers.run", "catchup", "--sync-ops-db", "--metrics-port", "0")
        Invoke-Platform @("python", "-m", "pipelines.bronze.stream_kafka_to_bronze", "--once")
    }
    "batch"     = { Invoke-Platform @("python", "-m", "ingestion.batch.run", "--all") }
    "silver"    = { Invoke-Platform @("python", "-m", "pipelines.silver.run") }
    "gold"      = { Invoke-Platform @("python", "-m", "pipelines.gold.run") }
    "quality"   = { Invoke-Platform @("python", "-m", "data_quality.run", "--fail-on", "never") }
    "reconcile" = { Invoke-Platform @("python", "-m", "pipelines.gold.reconciliation") }
    "publish"   = { Invoke-Platform @("python", "-m", "pipelines.gold.publish_serving") }
    "transform" = { foreach ($t in "silver", "gold", "quality", "reconcile") { & $targets[$t] } }
    "test"      = { Invoke-Platform @("python", "-m", "pytest", "tests/unit", "tests/data_quality", "-q", "-p", "no:cacheprovider", "--cov", "--cov-report=term-missing:skip-covered") }
    "test-integration" = { Invoke-Compose @("run", "--rm", "platform", "python", "-m", "pytest", "tests/integration", "-m", "integration", "-q", "-p", "no:cacheprovider") }
    "test-e2e"  = { Invoke-Compose @("run", "--rm", "platform", "python", "-m", "pytest", "tests/end_to_end", "-m", "e2e", "-q", "-p", "no:cacheprovider") }
    "lint"      = {
        Invoke-Platform @("ruff", "check", ".")
        Invoke-Platform @("ruff", "format", "--check", ".")
        Invoke-Platform @("mypy", "data_generator", "ingestion", "pipelines", "data_quality", "serving")
    }
    "benchmark" = { Invoke-Compose @("run", "--rm", "platform", "python", "-m", "benchmarks.run", "--events", "$Events") }
    "dashboard" = { Invoke-Compose @("--profile", "dashboard", "up", "-d", "streamlit") }
    "status"    = { Invoke-Platform @("python", "scripts/lakehouse_status.py") }
    "demo"      = {
        & $targets["setup"]
        Invoke-Compose @("up", "-d")
        foreach ($t in "seed", "stream", "batch", "transform", "publish", "dashboard") { & $targets[$t] }
        "Dashboard: http://localhost:8501   Grafana: http://localhost:3001   Airflow: http://localhost:8088"
    }
}

if ($Target -eq "help" -or -not $targets.Contains($Target)) {
    "Targets: " + ($targets.Keys -join ", ")
    exit 0
}
& $targets[$Target]
