import logging
import json
from ..utils import run_cmd
from ..models import MaintenanceJob
from ..config import PROCESSED_TAG
from ..oci_utils import compute_client, list_compartments
from ..utils import paginated

log = logging.getLogger(__name__)

def _host_map() -> dict[str, str]:
    log.info("Retrieving host map")
    njson = run_cmd([
        "/config/venv/Ubuntu_22.04_x86_64/bin/python3",
        "/config/mgmt/manage.py", "nodes", "list", "json"
    ])
    host_map = {n["ocid"]: n["hostname"] for n in json.loads(njson)}
    log.info("Host map contains %d entries", len(host_map))
    log.debug("Host map: %s", host_map)
    return host_map

def discover() -> list[MaintenanceJob]:
    hmap = _host_map()
    jobs: list[MaintenanceJob] = []

    for comp in list_compartments():
        for ev_sum in paginated(
                compute_client.list_instance_maintenance_events,
                compartment_id=comp):
            if ev_sum.lifecycle_state not in ("SCHEDULED", "STARTED", "PROCESSING"):
                continue
            event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
            if event_response is None:
                log.warning("Failed to retrieve maintenance event %s", ev_sum.id)
                continue
            ev = event_response.data
            if ev.freeform_tags.get(PROCESSED_TAG):
                log.debug("Skipping processed event %s", ev.id)
                continue

            faults = "_".join(
                f"{d['fault_id']}_{d['component']}"
                for d in (ev.additional_details or {}).get('fault_details', [])
            )
            log.debug("Processing event %s for instance %s", ev.id, ev.instance_id)
            host = hmap.get(ev.instance_id)
            if host:
                log.debug("Found hostname %s for OCID %s", host, ev.instance_id)
                jobs.append(MaintenanceJob(ev, host, faults))
            else:
                log.warning("No hostname for OCID %s", ev.instance_id)
    log.info("Discovered %d maintenance jobs", len(jobs))
    for job in jobs:
        log.info("Maintenance job: %s - Event OCID: %s - Instance OCID: %s", job.hostname, job.event.id, job.event.instance_id)
    return jobs
