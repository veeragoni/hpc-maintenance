import logging
from typing import Dict, List
from .phases import discovery
from .config import get_approved_faults

log = logging.getLogger(__name__)

def gather_faults() -> Dict[str, List[str]]:
    """
    Returns a mapping of fault_id -> list of hostnames reporting that fault_id.
    Uses raw fault_ids exactly as discovered (no normalization).
    """
    jobs = discovery.discover()
    faults: Dict[str, List[str]] = {}
    for j in jobs:
        # If an event has no fault_ids, record a placeholder for visibility
        if not j.fault_ids:
            faults.setdefault("(none)", []).append(j.hostname)
            continue
        for fid in j.fault_ids:
            faults.setdefault(fid, []).append(j.hostname)
    return faults

def print_faults_summary() -> None:
    faults = gather_faults()
    approved = get_approved_faults()
    if not faults:
        print("No maintenance events found.")
        return

    print("Approved fault codes (from config):", ", ".join(sorted(approved)) or "(none)")
    print("Discovered fault codes summary (raw values):")
    for fid in sorted(faults.keys()):
        hosts = sorted(faults[fid])
        approved_mark = " [APPROVED]" if fid in approved else ""
        print(f"  - {fid}{approved_mark}: {len(hosts)} node(s) -> {', '.join(hosts)}")

    print("\nPer-node view:")
    # Re-run discovery to show per-node list exactly
    jobs = discovery.discover()
    for j in sorted(jobs, key=lambda x: x.hostname):
        print(f"  {j.hostname}: fault_ids={j.fault_ids} fault_str={j.fault_str}")
