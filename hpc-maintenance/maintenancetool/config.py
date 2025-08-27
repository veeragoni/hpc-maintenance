import os
from pathlib import Path

TENANCY_OCID     = os.getenv("OCI_TENANCY_OCID")      # optional override
PROCESSED_TAG    = os.getenv("PROCESSED_TAG", "maintenance_processed")
MAX_WORKERS      = int(os.getenv("MAX_WORKERS", 8))

LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE         = Path(os.getenv("LOG_FILE", "maint_orchestrator.log"))

DRAIN_POLL_SEC   = int(os.getenv("DRAIN_POLL_SEC", 30))
MAINT_POLL_SEC   = int(os.getenv("MAINT_POLL_SEC", 60))

SLURM_DRAIN_REASON = os.getenv("SLURM_DRAIN_REASON", "OCiMaintenance")
SLURM_DRAIN_COMMAND = f"sudo scontrol update NodeName=%s Reason='{SLURM_DRAIN_REASON}' State=DRAIN"
SLURM_RESUME_COMMAND = "sudo scontrol update NodeName=%s State=RESUME"

tenancy_ocid = 'ocid1.compartment.oc1..aaaaaaaan5ouwmczcchigfas4xuzw5mh5xpqhnymull6y4g7gxc73wmgammq'
TENANCY_OCID = tenancy_ocid
# if this is set, attempt to use the local instance principal for auth
use_instance_principal = True
region = "us-phoenix-1"

