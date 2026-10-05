# Copies the fixed files into your repo, backing up whatever they replace. Safe to re-run.
param([string]$Repo = "C:\python\datashield-ai")
$ErrorActionPreference = "Stop"
$here  = Split-Path -Parent $MyInvocation.MyCommand.Path
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backup = Join-Path $Repo "_backup_before_fixes_$stamp"
if (-not (Test-Path $Repo)) { throw "Repo not found: $Repo" }

$files = @(
  "services/circuit_breaker/main.py",
  "services/circuit_breaker/breaker/config.py",
  "services/circuit_breaker/breaker/state_machine.py",
  "services/circuit_breaker/breaker/kafka_consumer.py",
  "services/circuit_breaker/breaker/admission.py",
  "services/drift_monitor/main.py",
  "services/drift_monitor/drift_detector/config.py",
  "services/drift_monitor/drift_detector/publish_gate.py",
  "services/drift_monitor/config/drift_detection_config.yaml",
  "services/ml_isolation_forest/main.py",
  "tests/test_edge_triggering_and_admission.py",
  "frontend/datashield_bridge.py",
  "scripts/verify_live_stack.py"
)
function Install-One($srcRel, $dstRel) {
  $src = Join-Path $here $srcRel
  $dst = Join-Path $Repo $dstRel
  if (Test-Path $dst) {
    $b = Join-Path $backup $dstRel
    New-Item -ItemType Directory -Force -Path (Split-Path $b) | Out-Null
    Copy-Item $dst $b -Force
  }
  New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
  Copy-Item $src $dst -Force
  Write-Host "  installed $dstRel"
}
foreach ($f in $files) { Install-One $f $f }

# The page keeps your existing filename (it contains an em dash, which is why it is renamed in this zip).
$emdash = [char]0x2014
Install-One "frontend/pipeline-observatory.html" "frontend/DataShield AI $emdash Pipeline Observatory.html"

Write-Host "`nDone. Backups (if any) are in $backup"
Write-Host "Next: follow RUNBOOK.md (rebuild circuit_breaker, drift_monitor, ml-isolation-forest)."
