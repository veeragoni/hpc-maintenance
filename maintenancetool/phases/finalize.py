from ..models import MaintenanceJob
from ..slrum_utils import resume, mark_ntr

def execute(job: MaintenanceJob) -> None:
    (resume if job.health_ok else mark_ntr)(job.hostname)