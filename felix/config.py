import os
import json
from pathlib import Path
from typing import List, Set

def _load_env_files() -> None:
    """
    Load environment variables from .env (and .env.local if present).
    - .env is the primary runtime file (checked in .gitignore)
    - .env.local is a template/example; also read if present to ease local dev
    Values already present in the process environment are not overridden.
    Supported format: KEY=VALUE with optional quotes; lines starting with '#' are ignored.
    """
    for fname in (".env", ".env.local"):
        p = Path(fname)
        if not p.exists():
            continue
        try:
            for raw in p.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
        except Exception:
            # Fail-closed: ignore malformed lines but continue loading others
            pass


# Load .env/.env.local before reading configuration values
_load_env_files()

# Core configuration (env-driven; no secrets in source)
# Accept both OCI_TENANCY_OCID and TENANCY_OCID for convenience; provide an obvious dummy default.
TENANCY_OCID = os.getenv("OCI_TENANCY_OCID") or os.getenv("TENANCY_OCID") or "ocid1.compartment.DUMMY"

PROCESSED_TAG = os.getenv("PROCESSED_TAG", "maintenance_processed")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = Path(os.getenv("LOG_FILE", "maint_orchestrator.log"))

DRAIN_POLL_SEC = int(os.getenv("DRAIN_POLL_SEC", "30"))
MAINT_POLL_SEC = int(os.getenv("MAINT_POLL_SEC", "86400"))  # 24 hours

# Override flag: Skip drain state check and allow immediate scheduling
# When True, maintenance can be scheduled without waiting for node to reach IDLE+DRAIN state
SKIP_DRAIN_CHECK = os.getenv("SKIP_DRAIN_CHECK", "false").lower() in ("true", "1", "yes")

# Paths for external configuration files
APPROVED_FAULT_CODES_FILE = Path(os.getenv("APPROVED_FAULT_CODES_FILE", "config/approved_fault_codes.json"))
EXCLUDED_HOSTS_FILE = Path(os.getenv("EXCLUDED_HOSTS_FILE", "config/excluded_hosts.json"))
EVENTS_LOG_FILE = Path(os.getenv("EVENTS_LOG_FILE", "logs/events.jsonl"))

def _read_json_list(path: Path) -> List[str]:
    import logging
    try:
        if path.exists():
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return [str(x) for x in data]
                else:
                    logging.warning(f"File {path} does not contain a JSON array")
        else:
            logging.warning(f"File {path} does not exist")
    except json.JSONDecodeError as e:
        logging.error(f"JSON parse error in {path}: {e}")
    except Exception as e:
        logging.error(f"Error reading {path}: {e}")
    return []


# OCI auth/region
region = os.getenv("REGION", "us-ashburn-1")

# NTR configuration
APPROVED_FAULT_CODES_ENV = os.getenv("APPROVED_FAULT_CODES", "")
APPROVED_FAULT_CODES: Set[str] = {c.strip() for c in APPROVED_FAULT_CODES_ENV.split(",") if c.strip()}


# Map normalized -> canonical (as provided in config)

# Guardrails and loop settings
DAILY_SCHEDULE_CAP = int(os.getenv("DAILY_SCHEDULE_CAP", "10"))
LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "900"))  # 15 minutes

# Exclusions: global list of hostnames excluded from automation (drain/schedule/etc.)
# Loaded from EXCLUDED_HOSTS_FILE (JSON array of hostnames). Example:
#   ["GPU-9", "GPU-332"]
def get_excluded_hosts() -> set[str]:
    return set(_read_json_list(EXCLUDED_HOSTS_FILE))

def is_host_excluded(hostname: str) -> bool:
    return hostname in get_excluded_hosts()

def get_approved_faults() -> set[str]:
    import logging
    arr = _read_json_list(APPROVED_FAULT_CODES_FILE)
    if arr:
        result = {s.strip() for s in arr if isinstance(s, str) and s.strip()}
        logging.debug(f"Loaded approved faults from {APPROVED_FAULT_CODES_FILE}: {result}")
        return result
    # Fallback to env if file empty or missing
    logging.debug(f"Using approved faults from env fallback: {APPROVED_FAULT_CODES}")
    return set(APPROVED_FAULT_CODES)

def is_fault_approved(fault_ids: list[str]) -> str | None:
    """
    Return the approved fault code by exact (raw) match only.
    Normalization is intentionally not used per operator request.
    """
    approved = get_approved_faults()
    for fid in fault_ids or []:
        if fid in approved:
            return fid
    return None
