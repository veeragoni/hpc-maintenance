import logging
from typing import Optional, Dict, Any

log = logging.getLogger(__name__)

def mgmt_update_node_status(hostname: str, status: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Placeholder/stub for MGMT integration.
    In production: call MGMT DB/API to reflect node status transitions (e.g., 'NTR scheduled').
    """
    log.info("MGMT update: host=%s status=%s details=%s", hostname, status, details or {})
