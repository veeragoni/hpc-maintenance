from ..models import MaintenanceJob
from ..config import PROCESSED_TAG
from ..oci_utils import trigger_update, is_event_complete
import time, logging
from ..config import MAINT_POLL_SEC

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
        job.work_request = trigger_update(
            event_id=job.event.id,
            time_window_start=scheduled_time
        )
        # Wait for the maintenance to move to a new state, then complete
        if job.work_request:
            while not is_event_complete(job.event.id):
                time.sleep(MAINT_POLL_SEC)
        job.done = True
        return

    # For all other states: log and return; nothing to be done.
    log.info(f"Event {job.event.id} is in terminal or in-progress state ({state}). No action taken.")
    return
