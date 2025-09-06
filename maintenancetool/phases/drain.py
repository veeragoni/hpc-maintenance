from ..models import MaintenanceJob
from ..slrum_utils import drain, wait_drained_empty

def execute(job: MaintenanceJob) -> None:
    # Use the approved fault code if available; otherwise fall back to discovered fault string
    reason = job.approved_fault or job.fault_str
    drain(job.hostname, reason)
    # Wait until node transitions through DRAINING and becomes DRAIN (empty: IDLE+DRAIN)
    wait_drained_empty(job.hostname)
