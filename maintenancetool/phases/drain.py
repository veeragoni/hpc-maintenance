from ..models import MaintenanceJob
from ..slrum_utils import drain, wait_state

def execute(job: MaintenanceJob) -> None:
    drain(job.hostname, job.fault_str)
    wait_state(job.hostname, "drain")