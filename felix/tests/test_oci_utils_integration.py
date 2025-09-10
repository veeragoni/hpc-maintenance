import pytest
from felix.oci_utils import list_compartments, list_instance_maintenance_events, get_gpu_ocid_dict, get_gpus_for_ocid
from felix.config import TENANCY_OCID
import json
from unittest.mock import patch

def test_list_compartments_integration():
    print(f"TENANCY_OCID: {TENANCY_OCID}")
    compartments = list_compartments()
    assert isinstance(compartments, list)
    for comp in compartments:
        print(f"Compartment ID: {comp}")
    assert all(isinstance(comp, str) for comp in compartments)
    assert len(compartments) > 0

def test_list_instance_maintenance_events_integration():
    events = list_instance_maintenance_events(TENANCY_OCID)
    print(f"Found {len(events)} maintenance events")
    for event in events:
        print(f"Event ID: {event.id}, Compartment ID: {event.compartment_id}, State: {event.lifecycle_state}")
    assert isinstance(events, list)

@patch('felix.oci_utils.subprocess.check_output')
def test_get_gpu_ocid_dict(mock_check_output):
    mock_check_output.return_value = '[{"ocid": "ocid1.instance.oc1.iad.example", "hostname": "GPU1"}]'
    ocids = ['ocid1.instance.oc1.iad.example']
    result = get_gpu_ocid_dict(ocids)
    assert result == {'ocid1.instance.oc1.iad.example': ['GPU1']}


def test_instance_ocid_gpu_ids_integration():
    events = list_instance_maintenance_events(TENANCY_OCID)
    instance_ocids = [event.instance_id for event in events if event.instance_id is not None]
    for ocid in instance_ocids:
        gpus = get_gpus_for_ocid(ocid)
        print(f"Instance OCID: {ocid}, GPU IDs: {gpus}")

if __name__ == '__main__':
    pytest.main([__file__])
