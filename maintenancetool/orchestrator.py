import concurrent.futures as cf, logging
from .models import MaintenanceJob
from .config import MAX_WORKERS
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
    jobs = discovery.discover()
    if not jobs:
        logging.info("No maintenance events to process.")
        return

    logging.info("Processing %d jobs …", len(jobs))
    with cf.ThreadPoolExecutor(max_workers=min(len(jobs), MAX_WORKERS)) as pool:
        list(pool.map(_process, jobs))   # waits for completion