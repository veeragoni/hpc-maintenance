import concurrent.futures as cf, logging, time
from .models import MaintenanceJob
from .config import MAX_WORKERS, DAILY_SCHEDULE_CAP, LOOP_INTERVAL_SEC, is_fault_approved
from .logging_util import setup_logging

from .phases import (
    discovery,  # type: ignore  (import package)
    drain,
    maintenance,
    health,
    finalize,
)

def _process(job: MaintenanceJob) -> None:
    try:
        drain.execute(job)
        maintenance.execute(job)
        health.execute(job)
        finalize.execute(job)
    except Exception as exc:
        logging.exception("Workflow failed for %s: %s", job.hostname, exc)

def run_once() -> None:
    all_jobs = discovery.discover()
    if not all_jobs:
        logging.info("No maintenance events to process.")
        return

    # Filter by approved fault codes and annotate job.approved_fault
    approved: list[MaintenanceJob] = []
    for j in all_jobs:
        approved_fault = is_fault_approved(j.fault_ids)
        if approved_fault:
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

    logging.info("Processing %d jobs …", len(capped))
    with cf.ThreadPoolExecutor(max_workers=min(len(capped), MAX_WORKERS)) as pool:
        list(pool.map(_process, capped))   # waits for completion

def run_loop() -> None:
    """Periodic orchestrator loop."""
    logging.info("Starting maintenance loop with interval=%ss", LOOP_INTERVAL_SEC)
    while True:
        try:
            run_once()
        except Exception as exc:
            logging.exception("Loop iteration failed: %s", exc)
        time.sleep(LOOP_INTERVAL_SEC)
