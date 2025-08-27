import datetime as dt
import logging
import oci
from oci.work_requests.models import WorkRequest
from .common import make_compute_client, make_identity_client, make_workrequests_client, paginated_api_to_generator, get_additional_details, is_workrequest_terminal
from .config import PROCESSED_TAG, TENANCY_OCID
from .utils import paginated
from typing import Optional, List, Dict
import subprocess
import json

log = logging.getLogger(__name__)

compute_client = make_compute_client()
identity_client = make_identity_client()
wr_client = make_workrequests_client()

def list_instance_maintenance_events(compartment_id: str) -> List:
    events = []
    for event in paginated(compute_client.list_instance_maintenance_events,
                           compartment_id=compartment_id):
        events.append(event)
    return events

def list_compartments() -> List[str]:
    ids = [TENANCY_OCID]
    for comp in paginated(identity_client.list_compartments,
                          compartment_id=TENANCY_OCID):
        ids.append(comp.id)
    return ids

from typing import Optional

def trigger_update(
    event_id: str,
    freeform_tags: Optional[dict] = None,
    defined_tags: Optional[dict] = None,
    time_window_start: Optional[str] = None,
) -> Optional[str]:
    log.info(f"trigger_update: event_id type={type(event_id)} value={event_id}")
    log.info(f"trigger_update: freeform_tags type={type(freeform_tags)} value={freeform_tags}")
    log.info(f"trigger_update: defined_tags type={type(defined_tags)} value={defined_tags}")
    log.info(f"trigger_update: time_window_start type={type(time_window_start)} value={time_window_start}")

    details_kwargs = {}
    if freeform_tags:
        details_kwargs["freeform_tags"] = freeform_tags
    if defined_tags:
        details_kwargs["defined_tags"] = defined_tags
    if time_window_start:
        details_kwargs["time_window_start"] = time_window_start

    log.info(f"Building UpdateInstanceMaintenanceEventDetails with kwargs: {details_kwargs}")

    try:
        details = oci.core.models.UpdateInstanceMaintenanceEventDetails(**details_kwargs)
        log.info(f"Constructed UpdateInstanceMaintenanceEventDetails: {details}")

        resp = compute_client.update_instance_maintenance_event(
            instance_maintenance_event_id=event_id,
            update_instance_maintenance_event_details=details,
        )
        log.info(f"update_instance_maintenance_event API called with event_id={event_id} details={details}")
        if resp is not None and hasattr(resp, 'headers') and "opc-work-request-id" in resp.headers:
            wr_id = resp.headers["opc-work-request-id"]
            log.info("Triggered maintenance WR=%s", wr_id)
            _wait_work_request(wr_id)
            return wr_id
        else:
            log.error("Failed to trigger maintenance update: Invalid response. Response: %s", resp)
            return None
    except Exception as e:
        log.error("Error triggering maintenance update: %s", e, exc_info=True)
        return None

def _wait_work_request(wr_id: str) -> None:
    try:
        lifecycle_states = ["SUCCEEDED", "FAILED", "CANCELED", "COMPLETED"]
        # Only supply evaluate_response, and no property/attribute args.
        # Import sentinel for proper check
        from oci.util import WAIT_RESOURCE_NOT_FOUND

        # Use property/state pattern per OCI docs for work requests
        waiter_result = oci.wait_until(
            wr_client,
            wr_client.get_work_request(wr_id),
            property="status",
            state=lifecycle_states
        )
        if waiter_result is WAIT_RESOURCE_NOT_FOUND:
            log.error(f"Work request {wr_id} not found - reached WAIT_RESOURCE_NOT_FOUND sentinel.")
        elif hasattr(waiter_result, "data"):
            log.info(f"Work request {wr_id} reached a terminal state: {waiter_result.data.status}")
        else:
            log.info(f"Work request {wr_id} waiter result has unexpected type: {type(waiter_result)}")
    except Exception as e:
        log.error(f"Error waiting for work request: {e}")

def is_event_complete(event_id: str) -> bool:
    try:
        response = compute_client.get_instance_maintenance_event(event_id)
        if response is not None and response.data is not None:
            ev = response.data
            return ev.lifecycle_state in ("SUCCEEDED", "FAILED", "CANCELED", "COMPLETED")
        else:
            log.error("Event data is None")
            return False
    except Exception as e:
        log.error(f"Error checking event completion: {e}")
        return False

def get_gpus_for_ocid(ocid: str) -> List[str]:
    mgmt_cmd = ["/config/venv/Ubuntu_22.04_x86_64/bin/python3", "/config/mgmt/manage.py"]
    raw_json = subprocess.check_output(mgmt_cmd + ["nodes", "list", "json"], text=True)
    nodes = json.loads(raw_json)
    ocid_to_host = {n["ocid"]: n.get("hostname", "") for n in nodes}
    return [ocid_to_host.get(ocid, "")]

def get_gpu_ocid_dict(ocids: List[str]) -> Dict[str, List[str]]:
    gpu_ocid_dict = {}
    for ocid in ocids:
        gpus_for_ocid = get_gpus_for_ocid(ocid)
        gpu_ocid_dict[ocid] = gpus_for_ocid
    return gpu_ocid_dict
