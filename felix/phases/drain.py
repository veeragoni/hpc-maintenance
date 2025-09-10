from ..models import MaintenanceJob
from ..slrum_utils import drain, wait_drained_empty
from ..eventlog import log_event

def execute(job: MaintenanceJob) -> None:
    """
    Drain only when maintenance event indicates downtime host maintenance, and never for TERMINATE actions.
    """
    ev = job.event
    ev_type = getattr(ev, "display_name", "") or ""
    action = getattr(ev, "instance_action", "") or ""

    # Guard: skip drain unless it's a downtime host maintenance, and block TERMINATE actions
    if action == "TERMINATE" or ev_type != "DOWNTIME_HOST_MAINTENANCE":
        log_event({
            "phase": "drain",
            "action": "skipped",
            "host": job.hostname,
            "reason": "not_eligible_for_drain",
            "maintenance_event_type": ev_type,
            "instance_action": action,
        })
        return

    # Use the approved fault code if available; otherwise fall back to discovered fault string
    reason = job.approved_fault or job.fault_str
    log_event({"phase": "drain", "action": "requested", "host": job.hostname, "reason": reason})
    drain(job.hostname, reason)
    # Wait until node transitions through DRAINING and becomes DRAIN (empty: IDLE+DRAIN)
    wait_drained_empty(job.hostname)
    log_event({"phase": "drain", "action": "drained_empty", "host": job.hostname})
