import os
import re
from pathlib import Path

TENANCY_OCID     = os.getenv("OCI_TENANCY_OCID")      # optional override
PROCESSED_TAG    = os.getenv("PROCESSED_TAG", "maintenance_processed")
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", 8))

LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE         = Path(os.getenv("LOG_FILE", "maint_orchestrator.log"))

DRAIN_POLL_SEC   = int(os.getenv("DRAIN_POLL_SEC", 30))
MAINT_POLL_SEC   = int(os.getenv("MAINT_POLL_SEC", 86400))  # 24 hours

SLURM_DRAIN_REASON = os.getenv("SLURM_DRAIN_REASON", "OCiMaintenance")
SLURM_DRAIN_COMMAND = f"sudo scontrol update NodeName=%s Reason='{SLURM_DRAIN_REASON}' State=DRAIN"
SLURM_RESUME_COMMAND = "sudo scontrol update NodeName=%s State=RESUME"

tenancy_ocid = 'ocid1.compartment.oc1..aaaaaaaan5ouwmczcchigfas4xuzw5mh5xpqhnymull6y4g7gxc73wmgammq'
TENANCY_OCID = tenancy_ocid
# if this is set, attempt to use the local instance principal for auth
use_instance_principal = True
region = "us-ashburn-1"

# NTR configuration
APPROVED_FAULT_CODES_ENV = os.getenv("APPROVED_FAULT_CODES", "HPCGPU-0001-01,HPCRDMA-0002-02")
APPROVED_FAULT_CODES = {c.strip() for c in APPROVED_FAULT_CODES_ENV.split(",") if c.strip()}

def normalize_fault_code(code: str) -> str:
    """
    Normalize a fault code for matching:
    - Uppercase
    - Remove all non-alphanumeric characters (drop spaces, dashes, underscores, etc.)
    """
    return re.sub(r"[^A-Z0-9]", "", (code or "").upper())

# Map normalized -> canonical (as provided in config)
APPROVED_FAULT_CODES_NORM = {normalize_fault_code(c): c for c in APPROVED_FAULT_CODES}

# Guardrails and loop settings
DAILY_SCHEDULE_CAP = int(os.getenv("DAILY_SCHEDULE_CAP", 10))
LOOP_INTERVAL_SEC  = int(os.getenv("LOOP_INTERVAL_SEC", 900))  # 15 minutes

def get_approved_faults() -> set[str]:
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
