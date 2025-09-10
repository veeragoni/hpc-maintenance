from dataclasses import dataclass, field
from typing import Optional
from oci.core.models import InstanceMaintenanceEvent

@dataclass
class MaintenanceJob:
    event:        InstanceMaintenanceEvent
    hostname:     str
    fault_str:    str
    # List of individual fault IDs extracted during discovery (for whitelist checks)
    fault_ids:    list[str] = field(default_factory=list)
    # Selected approved fault ID (filled by orchestrator when matched to whitelist)
    approved_fault: Optional[str] = None

    work_request: Optional[str] = None
    done:         bool = False
    health_ok:    Optional[bool] = None
