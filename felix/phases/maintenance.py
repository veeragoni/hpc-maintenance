from ..models import MaintenanceJob
from ..config import PROCESSED_TAG
from ..oci_utils import trigger_update, is_event_complete
from ..slrum_utils import set_reason
from ..mgmt_utils import mgmt_update_node_status, mgmt_reconfigure_compute
import time, logging
from ..config import MAINT_POLL_SEC
from ..eventlog import log_event

log = logging.getLogger(__name__)

def execute(job: MaintenanceJob) -> None:
    if job.event.id is None:
        log.error("MaintenanceJob event ID is None, cannot proceed with maintenance")
        return

    state = getattr(job.event, "lifecycle_state", None)
    log.info(f"MaintenanceJob event {job.event.id} on {job.hostname} with state {state}")

    # Only handle SCHEDULED: trigger maintenance with time_window_start = now + 5 min
    if state == "SCHEDULED":
        import datetime as dt
        scheduled_time = (dt.datetime.utcnow() + dt.timedelta(minutes=5)).replace(microsecond=0).isoformat() + "Z"
        log.info(f"Starting maintenance for event {job.event.id} with time_window_start={scheduled_time} (SCHEDULED state).")
        log_event({
            "phase": "maintenance",
            "action": "schedule_request",
            "host": job.hostname,
            "event_id": job.event.id,
            "instance_id": getattr(job.event, "instance_id", None),
            "window_start": scheduled_time
        })
        job.work_request = trigger_update(
            event_id=job.event.id,
            time_window_start=scheduled_time
        )

        if job.work_request:
            # Only update reason/MGMT if scheduling was accepted
            log_event({
                "phase": "maintenance",
                "action": "schedule_accepted",
                "host": job.hostname,
                "event_id": job.event.id,
                "instance_id": getattr(job.event, "instance_id", None),
                "work_request": getattr(job, "work_request", None)
            })
            reason_fault = job.approved_fault or job.fault_str
            if reason_fault:
                try:
                    set_reason(job.hostname, f"NTR scheduled: {reason_fault}")
                except Exception as e:
                    log.warning("Failed to set Slurm reason for %s: %s", job.hostname, e)
            try:
                mgmt_update_node_status(
                    job.hostname,
                    "NTR scheduled",
                    {
                        "fault_code": reason_fault,
                        "event_id": job.event.id,
                        "instance_id": getattr(job.event, "instance_id", None),
                    },
                )
            except Exception as e:
                log.warning("Failed to update MGMT status for %s: %s", job.hostname, e)

            # Trigger MGMT reconfigure compute (rerun cloud-init) using instance OCID when available
            inst_id = getattr(job.event, "instance_id", None)
            nodes_list = [inst_id] if inst_id else [job.hostname]
            try:
                log_event({
                    "phase": "maintenance",
                    "action": "mgmt_reconfigure_request",
                    "host": job.hostname,
                    "event_id": job.event.id,
                    "instance_id": inst_id
                })
                ok = mgmt_reconfigure_compute(nodes_list)
                log_event({
                    "phase": "maintenance",
                    "action": "mgmt_reconfigure_ok" if ok else "mgmt_reconfigure_failed",
                    "host": job.hostname,
                    "event_id": job.event.id,
                    "instance_id": inst_id
                })
            except Exception as e:
                log.warning("MGMT reconfigure compute failed for %s: %s", job.hostname, e)
                try:
                    log_event({
                        "phase": "maintenance",
                        "action": "mgmt_reconfigure_error",
                        "host": job.hostname,
                        "event_id": job.event.id,
                        "instance_id": inst_id,
                        "error": str(e)
                    })
                except Exception:
                    pass

            # Wait for the maintenance to move to a new state, then complete
            while not is_event_complete(job.event.id):
                time.sleep(MAINT_POLL_SEC)
            log_event({
                "phase": "maintenance",
                "action": "event_complete",
                "host": job.hostname,
                "event_id": job.event.id,
                "instance_id": getattr(job.event, "instance_id", None)
            })
            job.done = True
        else:
            log.warning("Scheduling not permitted for event %s; skipping reason/MGMT updates.", job.event.id)
            log_event({
                "phase": "maintenance",
                "action": "schedule_denied",
                "host": job.hostname,
                "event_id": job.event.id,
                "instance_id": getattr(job.event, "instance_id", None)
            })
        return

    # For all other states: log and return; nothing to be done.
    log.info(f"Event {job.event.id} is in terminal or in-progress state ({state}). No action taken.")
    try:
        log_event({
            "phase": "maintenance",
            "action": "skipped_state",
            "host": job.hostname,
            "event_id": job.event.id,
            "instance_id": getattr(job.event, "instance_id", None),
            "state": state
        })
    except Exception:
        pass
    return
