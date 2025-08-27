import logging

import datetime
import oci
import pprint

import maintenancetool.config as config
from maintenancetool.common import setup_logging, paginated_api_to_generator, is_workrequest_terminal, get_additional_details
from maintenancetool.common import make_compute_client, make_identity_client, make_workrequests_client
import subprocess
import json


def get_fault_details(maintenance):
    assert isinstance(maintenance, oci.core.models.InstanceMaintenanceEvent)
    _, _, fault_details = get_additional_details(maintenance.additional_details)
    return fault_details

def log_event_info(event):
    logger = logging.getLogger(__name__)
    logger.debug(pprint.pformat(event))


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    compute_client = make_compute_client()
    assert isinstance(compute_client, oci.core.ComputeClient)
    identity_client = make_identity_client()
    assert isinstance(identity_client, oci.identity.IdentityClient)

    # First, we list all compartments in the tenancy
    compartments = list_compartments(identity_client, config.tenancy_ocid)
    # Next, we retrieve all MaintenanceEvents for all compartments we found above
    all_events = list_all_maintenance_events(compute_client, compartments)
    
    mgmt_cmd = ["/config/venv/Ubuntu_22.04_x86_64/bin/python3", "/config/mgmt/manage.py"]
    raw_json = subprocess.check_output(mgmt_cmd + ["nodes", "list", "json"], text=True)
    nodes = json.loads(raw_json)
    ocid_to_host = {n["ocid"]: n["hostname"] for n in nodes}

    # for each, print out the affected instance and the list of faults
    for inst_maintenance in all_events:
        assert isinstance(inst_maintenance, oci.core.models.InstanceMaintenanceEvent)
        faults = get_fault_details(inst_maintenance)
        faults = [(fault.fault_id, fault.component) for fault in faults]
        print(f"Maintenance scheduled against {inst_maintenance.instance_id} with faults: {faults}")
        fault = "_".join([item for fault_tuple in faults for item in fault_tuple])
        hostname = ocid_to_host.get(inst_maintenance.instance_id, "UNKNOWN")
        print(f"Host name for {inst_maintenance.instance_id}: {hostname}")
        print(f"Fault: {fault}")
        #log_event_info(inst_maintenance)
        if hostname != "UNKNOWN":
            cmd = [
                "sudo",
                "scontrol",
                "update",
                f"NODENAME={hostname}",
                f'REASON="{fault}"',
                "STATE=DRAIN",
            ]
           
            try:
                # subprocess.run(cmd, check=True) # Uncomment this line to actually run the command
                logger.info("Draining the instance of maintenance to SLURM. Command ran: %s", " ".join(cmd))
            except subprocess.CalledProcessError as exc:
                logger.error("Failed to drain node %s: %s", hostname, exc)

def list_all_maintenance_events(compute_client, compartments):
    """List all MaintenanceEvents across the list of compartments
    The API requires that a compartmentId be provided for each request, so we cannot just retrieve everything
    from the tenancy level recursively.

    In our case, we need the detail object, so we perform a list and then retrieve each event individually. We could
    also filter first and only do this for the events we care about.

    :returns list of oci.core.models.InstanceMaintenanceEvent
    """
    log = logging.getLogger(__name__)
    events = list()
    for compartment in compartments:
        for maintenance_event in paginated_api_to_generator(
                compute_client.list_instance_maintenance_events, compartment_id=compartment):

            #Let's look at only the events that are newly scheduled, and not any that are started or completed
            if maintenance_event.lifecycle_state not in ['SCHEDULED', 'STARTED', 'PROCESSING']:
                log.info("Filtering event %s because it is not in a scheduled state.", maintenance_event.id)
                continue

            log_event_info(maintenance_event)
            event_id = maintenance_event.id
            # The details of the event are not included in the oci.core.models.InstanceMaintenanceEventSummary object,
            # we need to issue a GET to retrieve the details
            response = compute_client.get_instance_maintenance_event(instance_maintenance_event_id=event_id)
            event = response.data
            assert isinstance(event, oci.core.models.InstanceMaintenanceEvent)
            events.append(event)
    return events

def map_instances_to_hosts(compute_client):
    assert isinstance(compute_client, oci.core.ComputeClient)

def schedule_maintenance_immediately(compute_client, maintenance_event):
    """Schedule MaintenanceEvent to occur immediately"""
    assert isinstance(compute_client, oci.core.ComputeClient)

    tags = maintenance_event.freeform_tags
    tags[config.processed_tag] = True

    schedule_time = datetime.datetime.now()
    update = oci.core.models.UpdateInstanceMaintenanceEventDetails(
        time_window_start=schedule_time.isoformat(), freeform_tags=tags
    )
    response = compute_client.update_instance_maintenance_event(maintenance_event.id, update)
    work_request_id = response.headers.get('opc-work-request-id')

    workrequests_client = make_workrequests_client()
    assert isinstance(workrequests_client, oci.work_requests.WorkRequestClient)

    # Wait for the update to complete
    workrequest_status = workrequests_client.get_work_request(work_request_id=work_request_id)
    oci.waiter.wait_until(workrequests_client, workrequest_status, evaluate_response=is_workrequest_terminal)

    if workrequest_status != 'SUCCESSFUL':
        raise Exception("Scheduling of event=%s failed." % maintenance_event.id)


def list_compartments(identity_client, tenant_id):
    """A generator for compartments that are under tenant_id
    Note: Includes the tenant_id in the generator
    """
    compartments = []

    for compartment in paginated_api_to_generator(identity_client.list_compartments, compartment_id=tenant_id):
        compartments.append(compartment.id)
    compartments.append(tenant_id)

    return compartments


# Only call if this file is invoked directly, not imported as a module
if __name__ == '__main__':
    main()
