"""
tests/test_oci_utils.py
Unit-tests for maintenancetool.oci_utils

These tests do NOT call Oracle Cloud Infrastructure.  They patch the OCI
SDK clients inside maintenancetool.oci_utils, inject fake data, and verify
that our wrapper functions behave correctly.
"""

from unittest.mock import MagicMock, patch
import datetime as dt

import pytest

# Patch oci.config.from_file before importing maintenancetool.oci_utils
with patch("oci.config.from_file") as mock_config:
    mock_config.return_value = {"tenancy": "fake_tenancy"}
    # import after monkey-patching ↓
    MODULE_PATH = "maintenancetool.oci_utils"


# ---------------------------------------------------------------------------
# helpers to build fake OCI objects without importing the real SDK everywhere
# ---------------------------------------------------------------------------
class _FakeResp:
    def __init__(self, data, next_page=None):
        self.data = data
        self.next_page = next_page


class _Obj:
    def __init__(self, **kw):
        self.__dict__.update(kw)


# ---------------------------------------------------------------------------
# list_compartments
# ---------------------------------------------------------------------------
def test_list_compartments(monkeypatch):
    fake_compartments = [_Obj(id="ocid1.compartment.aaa"), _Obj(id="ocid1.compartment.bbb")]

    # patch identity_client inside the target module BEFORE importing it
    with patch(f"{MODULE_PATH}.identity_client") as fake_id:
        fake_id.list_compartments.side_effect = [
            _FakeResp(fake_compartments)  # first / only page
        ]

        # now we can import the module
        from maintenancetool.oci_utils import list_compartments, TENANCY_OCID

        result = list_compartments()

        expected = [TENANCY_OCID, "ocid1.compartment.aaa", "ocid1.compartment.bbb"]
        assert result == expected
        fake_id.list_compartments.assert_called_once_with(compartment_id=TENANCY_OCID)


# ---------------------------------------------------------------------------
# trigger_update + _wait_work_request
# ---------------------------------------------------------------------------
@pytest.fixture
def _patch_clients(monkeypatch):
    """
    Patch compute_client & wr_client in oci_utils and yield their mocks.
    """
    with patch(f"{MODULE_PATH}.compute_client") as fake_compute, patch(
        f"{MODULE_PATH}.wr_client"
    ) as fake_wr:
        yield fake_compute, fake_wr


def test_trigger_update_success(_patch_clients):
    fake_compute, fake_wr = _patch_clients

    # --- fake UPDATE call response ----------------------------------------
    fake_response = _Obj(
        headers={"opc-work-request-id": "wr123"}
    )
    fake_compute.update_instance_maintenance_event.return_value = fake_response

    # --- fake GET work request & waiter -----------------------------------
    fake_wr_obj = _Obj(status="SUCCEEDED")
    fake_wr.get_work_request.return_value = _Obj(data=fake_wr_obj)

    # patch oci.wait_until so it doesn't actually sleep
    with patch("maintenancetool.oci_utils.oci.wait_until") as fake_wait:
        from maintenancetool.oci_utils import trigger_update

        wr_id = trigger_update(
            event_id="ev123",
            tags={"maintenance_processed": "true"},
        )

        # assertions
        assert wr_id == "wr123"
        fake_compute.update_instance_maintenance_event.assert_called_once()
        fake_wr.get_work_request.assert_called_once_with(work_request_id="wr123")
        fake_wait.assert_called_once()


# ---------------------------------------------------------------------------
# is_event_complete
# ---------------------------------------------------------------------------
def test_is_event_complete_true(_patch_clients):
    fake_compute, _ = _patch_clients
    fake_compute.get_instance_maintenance_event.return_value = _Obj(
        data=_Obj(lifecycle_state="COMPLETED")
    )

    from maintenancetool.oci_utils import is_event_complete

    assert is_event_complete("fake_event")
    fake_compute.get_instance_maintenance_event.assert_called_once_with("fake_event")


def test_is_event_complete_false(_patch_clients):
    fake_compute, _ = _patch_clients
    fake_compute.get_instance_maintenance_event.return_value = _Obj(
        data=_Obj(lifecycle_state="PROCESSING")
    )

    from maintenancetool.oci_utils import is_event_complete

    assert is_event_complete("fake_event") is False
