from ..models import MaintenanceJob
from ..slrum_utils import drain, wait_drained_empty
from ..eventlog import log_event

def execute(job: MaintenanceJob) -> None:
    # Use the approved fault code if available; otherwise fall back to discovered fault string
    reason = job.approved_fault or job.fault_str
    log_event({"phase": "drain", "action": "requested", "host": job.hostname, "reason": reason})
    drain(job.hostname, reason)
    # Wait until node transitions through DRAINING and becomes DRAIN (empty: IDLE+DRAIN)
    wait_drained_empty(job.hostname)
    log_event({"phase": "drain", "action": "drained_empty", "host": job.hostname})
