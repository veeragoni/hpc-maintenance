import json
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set

from .oci_utils import compute_client, list_compartments
from .utils import paginated, run_cmd
from .slrum_utils import slurm_node_status_map

log = logging.getLogger(__name__)

# Shared helpers and row builder for discovery/reporting views.
# Keep both discovery.py and reporting.py thin by delegating to this module.

# State ordering priority for sorting/grouping
STATE_ORDER = {
    "SCHEDULED": 1,
    "PROCESSING": 2,
    "IN_PROGRESS": 2,
    "STARTED": 2,
    "SUCCEEDED": 3,
    "FAILED": 5,
    "CANCELED": 5,
}

def _mgmt_host_map() -> Dict[str, str]:
    """
    Retrieve a mapping of instance OCID -> hostname from the management system.
    """
    njson = run_cmd([
        ".venv/bin/python3",
        "/config/mgmt/manage.py", "nodes", "list", "--format", "json"
    ])
    try:
        data = json.loads(njson)
    except Exception:
        logging.getLogger(__name__).warning("Failed to parse MGMT nodes JSON")
        return {}
    return {n.get("ocid"): n.get("hostname") for n in data if n.get("ocid") and n.get("hostname")}

def _as_aware(dt):
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        try:
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return dt
    return dt

def _seconds_between(start, end) -> float | None:
    if start is None or end is None:
        return None
    try:
        return max(0.0, (end - start).total_seconds())
    except Exception:
        return None

def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    sec = int(round(seconds))
    # If >= 24 hours, show days and hours only (e.g., "2 d 5 h")
    if sec >= 86400:
        d = sec // 86400
        h = (sec % 86400) // 3600
        return f"{d} d {h} h"
    # Otherwise show hours/minutes/seconds (e.g., "1 h 02 m" or "14 m 32 s")
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    parts: List[str] = []
    if h > 0:
        parts.append(f"{h} h")
        parts.append(f"{m:02d} m")
    else:
        if m > 0:
            parts.append(f"{m} m")
        parts.append(f"{s:02d} s")
    return " ".join(parts)

def fmt_ts(dt) -> str:
    dt = _as_aware(dt)
    if dt is None:
        return "—"
    try:
        ts = dt.astimezone(timezone.utc)
        month = ts.strftime("%b")  # e.g., "Aug"
        d = ts.day                 # no leading zero
        # ordinal suffix (1st, 2nd, 3rd, 4th, …)
        if 10 <= (d % 100) <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(d % 10, "th")
        tstr = ts.strftime("%I:%M %p").lstrip("0").lower()  # e.g., "10:00 pm"
        return f"{month} {d}{suffix} {ts.year} {tstr} UTC"
    except Exception:
        return str(dt)

def color_event_type(name: str | None) -> str:
    n = (name or "").upper()
    if n == "TERMINATE":
        return "[red]TERMINATE[/red]"
    if n == "DOWNTIME_HOST_MAINTENANCE":
        return "[magenta]DOWNTIME_HOST_MAINTENANCE[/magenta]"
    return name or ""

def pick_created_or_started(state_upper: str, ev):
    """
    Return the timestamp to display in the Created/Started column:
    - SCHEDULED -> time_created
    - Otherwise -> time_started if available, else time_created
    """
    return getattr(ev, "time_created", None) if state_upper == "SCHEDULED" else (getattr(ev, "time_started", None) or getattr(ev, "time_created", None))

def build_event_rows(
    progress_cb=None,
    filter_states: Optional[Set[str]] = None,
) -> List[dict]:
    """
    Build normalized event rows with consistent field names and formatting for both discovery and reporting.

    Returns list of dict with keys:
      - state: raw lifecycle_state
      - hostname: resolved hostname or "(unknown)"
      - slurm_state: slurm node state if available
      - instance_ocid: instance OCID
      - event_ocid: maintenance event OCID
      - display_name: maintenance event type (colorized for Rich)
      - fault_ids: list[str]
      - time_in_state: formatted duration string based on state rules
      - created: formatted Created/Started timestamp string per rules
      - total_processing: formatted processing duration (finished-started) or "—"
    """
    hmap = _mgmt_host_map()
    s_map = slurm_node_status_map()
    rows: List[dict] = []
    now = datetime.now(timezone.utc)

    filt = {s.upper() for s in (filter_states or set())} if filter_states else None

    for comp in list_compartments():
        if progress_cb:
            try:
                progress_cb({"phase": "compartment", "compartment": comp})
            except Exception:
                pass

        for ev_sum in paginated(
                compute_client.list_instance_maintenance_events,
                compartment_id=comp):
            raw_state = getattr(ev_sum, "lifecycle_state", None) or ""
            if filt and raw_state.upper() not in filt:
                continue

            event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
            if event_response is None:
                logging.getLogger(__name__).warning("Failed to retrieve maintenance event %s", ev_sum.id)
                continue
            ev = event_response.data

            additional = getattr(ev, "additional_details", None) or {}
            fault_details = additional.get("fault_details") or additional.get("faultDetails") or []
            if isinstance(fault_details, str):
                try:
                    fault_details = json.loads(fault_details)
                except Exception:
                    logging.getLogger(__name__).warning("fault_details not JSON-decodable for event %s: %s", ev.id, fault_details)
                    fault_details = []

            fault_ids: List[str] = []
            for d in fault_details:
                fid = d.get("fault_id") or d.get("faultId")
                if fid:
                    fault_ids.append(fid)

            hostname = hmap.get(ev.instance_id) or "(unknown)"

            # Time-aware columns
            state_upper = (raw_state or "").upper()
            t_started = _as_aware(getattr(ev, "time_started", None))
            t_finished = _as_aware(getattr(ev, "time_finished", None))

            # Time in current state (frozen for terminal states)
            if state_upper == "SCHEDULED":
                created_dt = _as_aware(getattr(ev, "time_created", None))
                tin_state = _seconds_between(created_dt, now)
            elif state_upper in ("PROCESSING", "IN_PROGRESS", "STARTED"):
                tin_state = _seconds_between(t_started, now)
            elif state_upper in ("SUCCEEDED", "FAILED", "CANCELED"):
                # Terminal: finished - started if both present
                tin_state = _seconds_between(t_started, t_finished)
            else:
                # Fallback: since creation
                tin_state = _seconds_between(_as_aware(getattr(ev, "time_created", None)), now)

            total_proc = _seconds_between(t_started, t_finished)

            rows.append({
                "state": raw_state,
                "hostname": hostname,
                "slurm_state": (s_map.get(hostname, {}) or {}).get("state", ""),
                "instance_ocid": getattr(ev, "instance_id", "") or "",
                "event_ocid": ev.id,
                "display_name": color_event_type(getattr(ev, "display_name", None)),
                "fault_ids": fault_ids,
                "time_in_state": fmt_duration(tin_state),
                "created": fmt_ts(pick_created_or_started(state_upper, ev)),
                "total_processing": fmt_duration(total_proc) if total_proc is not None else "—",
            })

            if progress_cb and (len(rows) % 20 == 0):
                try:
                    progress_cb({"phase": "progress", "count": len(rows)})
                except Exception:
                    pass

    return rows
