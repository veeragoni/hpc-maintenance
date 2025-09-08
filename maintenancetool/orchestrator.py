import concurrent.futures as cf, logging, time
from .models import MaintenanceJob
from .config import MAX_WORKERS, DAILY_SCHEDULE_CAP, LOOP_INTERVAL_SEC, is_fault_approved, is_host_excluded
from .logging_util import setup_logging
from .mgmt_utils import mgmt_update_node_status, mgmt_reconfigure_compute

from .phases import (
    discovery,  # type: ignore  (import package)
    drain,
    maintenance,
    health,
    finalize,
)

def _process(job: MaintenanceJob) -> None:
    try:
        state = getattr(job.event, "lifecycle_state", None)

        # If maintenance already completed, resume workflow from health/finalize
        if state in ("SUCCEEDED", "COMPLETED"):
            logging.info("Event %s on %s is %s; skipping drain/schedule; running health/finalize.",
                         getattr(job.event, "id", None), job.hostname, state)
            health.execute(job)
            finalize.execute(job)
            return

        # If maintenance is in progress/processing, do not schedule again; ensure node is drained only
        if state in ("IN_PROGRESS", "PROCESSING"):
            logging.info("Event %s on %s is %s; draining only and skipping schedule.",
                         getattr(job.event, "id", None), job.hostname, state)
            drain.execute(job)
            return

        # Normal path: drain -> schedule -> health -> finalize
        drain.execute(job)
        maintenance.execute(job)
        health.execute(job)
        finalize.execute(job)
    except Exception as exc:
        logging.exception("Workflow failed for %s: %s", job.hostname, exc)

def _process_stage(job: MaintenanceJob) -> None:
    """
    Stage-only processing: Discovery (already done by caller) -> Drain -> Schedule (maintenance)
    Skips health and finalize phases.
    """
    try:
        drain.execute(job)
        maintenance.execute(job)
    except Exception as exc:
        logging.exception("[STAGE] Workflow failed for %s: %s", job.hostname, exc)

def run_once(dry_run: bool = False) -> None:
    all_jobs = discovery.discover()
    if not all_jobs:
        logging.info("No maintenance events to process.")
        return

    # Filter by approved fault codes and annotate job.approved_fault
    approved: list[MaintenanceJob] = []
    for j in all_jobs:
        approved_fault = is_fault_approved(j.fault_ids)
        if approved_fault:
            if is_host_excluded(j.hostname):
                logging.info("Excluding %s by config (approved fault %s); skipping.", j.hostname, approved_fault)
                continue
            j.approved_fault = approved_fault
            approved.append(j)
        else:
            logging.info("Skipping %s: faults not in whitelist %s", j.hostname, j.fault_ids)

    if not approved:
        logging.info("No jobs matched approved fault codes.")
        return

    # Guardrail: cap number of jobs processed in this run
    capped = approved[:DAILY_SCHEDULE_CAP]
    if len(approved) > len(capped):
        logging.warning("Guardrail: limiting run to %d jobs out of %d approved", len(capped), len(approved))

    if dry_run:
        logging.info("[DRY RUN] Would process %d jobs …", len(capped))
        for j in capped:
            state = getattr(j.event, "lifecycle_state", None)
            inst_id = getattr(j.event, "instance_id", None)
            logging.info("[DRY RUN] %s: event=%s instance=%s state=%s fault=%s", j.hostname, j.event.id, inst_id, state, j.approved_fault)
            logging.info("[DRY RUN] Would DRAIN %s with reason '%s'", j.hostname, j.approved_fault)
            if state == "SCHEDULED":
                import datetime as dt
                scheduled_time = (dt.datetime.utcnow() + dt.timedelta(minutes=5)).replace(microsecond=0).isoformat() + "Z"
                logging.info("[DRY RUN] Would schedule maintenance for event %s on instance %s at %s", j.event.id, inst_id, scheduled_time)
                logging.info("[DRY RUN] Would set Slurm reason to 'NTR scheduled: %s' and update MGMT", j.approved_fault)
            logging.info("[DRY RUN] Would run health checks and finalize (resume on pass, keep drained on fail)")
        return

    logging.info("Processing %d jobs …", len(capped))
    with cf.ThreadPoolExecutor(max_workers=min(len(capped), MAX_WORKERS)) as pool:
        list(pool.map(_process, capped))   # waits for completion

def run_stage(dry_run: bool = False) -> None:
    """
    Run a single pass that performs: discover -> filter (approved/excluded) -> drain -> schedule
    Health and finalize are intentionally skipped.
    """
    all_jobs = discovery.discover()
    if not all_jobs:
        logging.info("[STAGE] No maintenance events to process.")
        return

    # Filter by approved fault codes and annotate job.approved_fault
    approved: list[MaintenanceJob] = []
    for j in all_jobs:
        approved_fault = is_fault_approved(j.fault_ids)
        if approved_fault:
            if is_host_excluded(j.hostname):
                logging.info("[STAGE] Excluding %s by config (approved fault %s); skipping.", j.hostname, approved_fault)
                continue
            j.approved_fault = approved_fault
            approved.append(j)
        else:
            logging.info("[STAGE] Skipping %s: faults not in whitelist %s", j.hostname, j.fault_ids)

    if not approved:
        logging.info("[STAGE] No jobs matched approved fault codes.")
        return

    # Guardrail: cap number of jobs processed in this run
    capped = approved[:DAILY_SCHEDULE_CAP]
    if len(approved) > len(capped):
        logging.warning("[STAGE] Guardrail: limiting run to %d jobs out of %d approved", len(capped), len(approved))

    if dry_run:
        logging.info("[STAGE][DRY RUN] Would process %d jobs …", len(capped))
        for j in capped:
            state = getattr(j.event, "lifecycle_state", None)
            inst_id = getattr(j.event, "instance_id", None)
            logging.info("[STAGE][DRY RUN] %s: event=%s instance=%s state=%s fault=%s", j.hostname, j.event.id, inst_id, state, j.approved_fault)
            logging.info("[STAGE][DRY RUN] Would DRAIN %s with reason '%s'", j.hostname, j.approved_fault)
            # In stage dry-run, simulate scheduling even if current state != SCHEDULED
            import datetime as dt
            scheduled_time = (dt.datetime.utcnow() + dt.timedelta(minutes=5)).replace(microsecond=0).isoformat() + "Z"
            logging.info("[STAGE][DRY RUN] Treating event state as SCHEDULED for preview")
            logging.info("[STAGE][DRY RUN] Would schedule maintenance for event %s on instance %s at %s", j.event.id, inst_id, scheduled_time)
            logging.info("[STAGE][DRY RUN] Would set Slurm reason to 'NTR scheduled: %s' and update MGMT", j.approved_fault)
        return

    logging.info("[STAGE] Processing %d jobs …", len(capped))
    with cf.ThreadPoolExecutor(max_workers=min(len(capped), MAX_WORKERS)) as pool:
        list(pool.map(_process_stage, capped))   # waits for completion

def run_loop(dry_run: bool = False) -> None:
    """Periodic orchestrator loop."""
    logging.info("Starting maintenance loop with interval=%ss%s", LOOP_INTERVAL_SEC, " [DRY RUN]" if dry_run else "")
    while True:
        try:
            run_once(dry_run=dry_run)
        except Exception as exc:
            logging.exception("Loop iteration failed: %s", exc)
        time.sleep(LOOP_INTERVAL_SEC)

def run_catchup(dry_run: bool = False, host: str | None = None) -> None:
    """
    One-shot reconciliation for nodes where maintenance was already triggered:
    - SUCCEEDED/COMPLETED: do NOT drain/schedule; run health -> finalize and update MGMT to running.
    - IN_PROGRESS/PROCESSING: do NOT drain/schedule; ensure MGMT is updated to NTR scheduled and reconfigure compute is triggered.
    - All other states: no-op.

    Respects excluded hosts and approved fault whitelist.
    """
    all_jobs = discovery.discover()
    if not all_jobs:
        logging.info("[CATCHUP] No maintenance events to process.")
        return

    if host:
        all_jobs = [j for j in all_jobs if j.hostname == host]
        if not all_jobs:
            logging.info("[CATCHUP] No maintenance event found for host %s.", host)
            return

    # Respect exclusions and approved fault whitelist
    jobs = [j for j in all_jobs if not is_host_excluded(j.hostname)]
    if not jobs:
        logging.info("[CATCHUP] No non-excluded jobs to process.")
        return

    approved_jobs: list[MaintenanceJob] = []
    for j in jobs:
        approved_fault = is_fault_approved(j.fault_ids)
        if approved_fault:
            j.approved_fault = approved_fault
            approved_jobs.append(j)
        else:
            logging.info("[CATCHUP] Skipping %s: faults not in whitelist %s", j.hostname, j.fault_ids)

    if not approved_jobs:
        logging.info("[CATCHUP] No jobs matched approved fault codes.")
        return

    for j in approved_jobs:
        state = getattr(j.event, "lifecycle_state", None)
        inst_id = getattr(j.event, "instance_id", None)
        eid = getattr(j.event, "id", None)
        fault = getattr(j, "approved_fault", None) or getattr(j, "fault_str", None)
        logging.info("[CATCHUP] %s: event=%s instance=%s state=%s fault=%s", j.hostname, eid, inst_id, state, fault)

        if state in ("SUCCEEDED", "COMPLETED"):
            logging.info("[CATCHUP][TODO] Health/finalize placeholders: would run health and finalize on %s", j.hostname)
            logging.info("[CATCHUP][TODO] MGMT update placeholder: would set status 'running' (or 'NTR scheduled' if still drained) for instance %s", inst_id or j.hostname)
            continue

        if state in ("IN_PROGRESS", "PROCESSING"):
            logging.info("[CATCHUP][TODO] MGMT placeholders: would set 'NTR scheduled' for %s and trigger reconfigure compute for %s", j.hostname, inst_id or j.hostname)
            continue

        logging.info("[CATCHUP] %s: state %s not applicable; skipping.", j.hostname, state)
