from dataclasses import dataclass
from typing import Optional
from oci.core.models import InstanceMaintenanceEvent

@dataclass
class MaintenanceJob:
    event:        InstanceMaintenanceEvent
    hostname:     str
    fault_str:    str
    work_request: Optional[str] = None
    done:         bool = False
    health_ok:    Optional[bool] = None