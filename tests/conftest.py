import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for service_dir in ["drift_monitor", "circuit_breaker", "rca_copilot"]:
    service_path = REPO_ROOT / "services" / service_dir
    if service_path.exists() and str(service_path) not in sys.path:
        sys.path.insert(0, str(service_path))
