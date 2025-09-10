from ..models import MaintenanceJob
from ..eventlog import log_event
import logging
log = logging.getLogger(__name__)

def execute(job: MaintenanceJob) -> None:
    # TODO: Finalize placeholder (no-op). Implement resume/mark_ntr, MGMT updates when APIs and flow are finalized.
    decision = "resume" if getattr(job, "health_ok", True) else "ntr"
    log.info("[TODO] Finalize placeholder: would %s node %s (health_ok=%s)", decision, job.hostname, getattr(job, "health_ok", True))
    try:
        log_event({
            "phase": "finalize",
            "action": "placeholder",
            "host": job.hostname,
            "decision": decision
        })
    except Exception:
        pass
