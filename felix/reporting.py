import logging
import json
import time
import itertools, random
from typing import Dict, List
from datetime import datetime, timezone
from .phases import discovery
from .config import get_approved_faults
from .oci_utils import compute_client, list_compartments
from .utils import paginated, run_cmd
from .slrum_utils import slurm_node_status_map
from .formatting import print_json_data
from .events_common import build_event_rows, STATE_ORDER as COMMON_STATE_ORDER

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


# Fun, short status messages paired with themed Rich spinners for OCI maintenance
SPIN_MSGS = [
    {"text": "Finding humming GPUs in OCI…", "spinner": "earth"},
    {"text": "Herding switches and cables, one port at a time…", "spinner": "line"},
    {"text": "Chasing down NVRMs so AI can dream bigger…", "spinner": "runner"},
    {"text": "Tuning accelerators; tomorrow runs faster than today…", "spinner": "simpleDotsScrolling"},
    {"text": "Mapping instances so models don’t miss a beat…", "spinner": "earth"},
    {"text": "Oracle Cloud checkup: cables snug, GPUs snugger…", "spinner": "weather"},
    {"text": "Sharpening the cluster—one maintenance at a time…", "spinner": "pong"},
    {"text": "Because the future deserves fewer flaky links…", "spinner": "line"},
    {"text": "Less downtime, more training time—OCI style…", "spinner": "aesthetic"},
    {"text": "Your gradients thank this maintenance sweep…", "spinner": "dots"},
]
# How long each loading message stays visible (seconds)
MESSAGE_CYCLE_SECONDS = 1.6

# SYNC NOTE:
# The column order, time-in-state logic, Created/Started formatting, and coloring in this file
# are intended to stay in lockstep with felix/phases/discovery.py.
# When you change one, mirror in the other or extract shared helpers to avoid drift.

# Event discovery and table rendering (reusable)
# State ordering priority for table sorting.
# Lower numbers sort earlier in the table.
# IMPORTANT: SCHEDULED must appear first; PROCESSING/IN_PROGRESS/STARTED next; SUCCEEDED next; FAILED/CANCELED last.
# Do not change without stakeholder approval to avoid reshuffling in downstream UIs.
_STATE_ORDER = COMMON_STATE_ORDER
# Default filter for the events table (maintain this set to change defaults)
DEFAULT_STATE_EXCLUDE = {"CANCELED"}

def _mgmt_host_map() -> Dict[str, str]:
    """
    Retrieve a mapping of instance OCID -> hostname from the management system.
    Duplicated here for reuse without importing a private helper from discovery.
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
    # Assume SDK provides tz-aware UTC; if naive, coerce to UTC
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

def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    sec = int(round(seconds))
    # If >= 24 hours, show days and hours only (e.g., "58 d 10 h")
    if sec >= 86400:
        d = sec // 86400
        h = (sec % 86400) // 3600
        return f"{d} d {h} h"
    # Otherwise show hours/minutes/seconds
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    parts = []
    if h > 0:
        parts.append(f"{h} h")
        parts.append(f"{m:02d} m")
    else:
        if m > 0:
            parts.append(f"{m} m")
        parts.append(f"{s:02d} s")
    return " ".join(parts)

def _fmt_ts(dt):
    dt = _as_aware(dt)
    if dt is None:
        return "—"
    try:
        ts = dt.astimezone(timezone.utc)
        month = ts.strftime("%b")
        d = ts.day
        if 10 <= (d % 100) <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(d % 10, "th")
        tstr = ts.strftime("%I:%M %p").lstrip("0").lower()
        return f"{month} {d}{suffix} {ts.year} {tstr} UTC"
    except Exception:
        return str(dt)

def _color_event_type(name: str | None) -> str:
    n = (name or "").upper()
    if n == "TERMINATE":
        return "[red]TERMINATE[/red]"
    if n == "DOWNTIME_HOST_MAINTENANCE":
        return "[magenta]DOWNTIME_HOST_MAINTENANCE[/magenta]"
    return name or ""

def _pick_created_or_started(state_upper: str, ev):
    dt = getattr(ev, "time_created", None) if state_upper == "SCHEDULED" else (getattr(ev, "time_started", None) or getattr(ev, "time_created", None))
    return _fmt_ts(dt)

def list_all_events(progress_cb=None) -> list[dict]:
    """
    Return a list of all instance maintenance events across compartments with raw fields.
    Each row contains:
      - state (raw lifecycle_state from provider)
      - hostname (resolved via MGMT, or '(unknown)')
      - instance_ocid
      - event_ocid
      - display_name
      - fault_ids (list[str])
    """
    hmap = _mgmt_host_map()
    s_map = slurm_node_status_map()
    rows: list[dict] = []
    now = datetime.now(timezone.utc)
    comp_idx = 0
    event_count = 0

    for comp in list_compartments():
        comp_idx += 1
        if progress_cb:
            try:
                progress_cb({"phase": "compartment", "index": comp_idx})
            except Exception:
                pass
        for ev_sum in paginated(
                compute_client.list_instance_maintenance_events,
                compartment_id=comp):
            event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
            if event_response is None:
                logging.getLogger(__name__).warning("Failed to retrieve maintenance event %s", ev_sum.id)
                continue
            ev = event_response.data

            additional = ev.additional_details or {}
            fault_details = additional.get('fault_details') or additional.get('faultDetails') or []
            if isinstance(fault_details, str):
                try:
                    fault_details = json.loads(fault_details)
                except Exception:
                    logging.getLogger(__name__).warning("fault_details not JSON-decodable for event %s: %s", ev.id, fault_details)
                    fault_details = []

            fault_ids = []
            for d in fault_details:
                fid = (d.get('fault_id') or d.get('faultId'))
                if fid:
                    fault_ids.append(fid)


            hostname = hmap.get(ev.instance_id) or "(unknown)"
            raw_state = getattr(ev_sum, "lifecycle_state", None) or getattr(ev, "lifecycle_state", None) or ""

            # Time-aware columns
            state_upper = (raw_state or "").upper()
            t_started = _as_aware(getattr(ev, "time_started", None))
            t_finished = _as_aware(getattr(ev, "time_finished", None))
            t_sched = _as_aware(getattr(ev, "time_window_start", None)) or _as_aware(getattr(ev, "time_created", None)) or t_started

            # Time in current state
            if state_upper in ("PROCESSING", "IN_PROGRESS", "STARTED"):
                tin_state = _seconds_between(t_started, now)
            elif state_upper == "SCHEDULED":
                created = _as_aware(getattr(ev, "time_created", None))
                tin_state = _seconds_between(created, now)
            elif state_upper in ("SUCCEEDED", "FAILED", "CANCELED"):
                tin_state = _seconds_between(t_started, t_finished)
            else:
                tin_state = _seconds_between(_as_aware(getattr(ev, "time_created", None)), now)

            # Total processing time (wall-clock), shown once terminal
            total_proc = _seconds_between(t_started, t_finished)

            rows.append({
                "state": raw_state,
                "hostname": hostname,
                "slurm_state": (s_map.get(hostname, {}) or {}).get("state", ""),
                "instance_ocid": ev.instance_id,
                "event_ocid": ev.id,
                "display_name": _color_event_type(getattr(ev, "display_name", None)),
                "fault_ids": fault_ids,
                "time_in_state": _fmt_duration(tin_state),
                "created": _pick_created_or_started(state_upper, ev),
                "total_processing": _fmt_duration(total_proc) if total_proc is not None else "—",
            })
            if progress_cb and (len(rows) % 20 == 0):
                try:
                    progress_cb({"phase": "progress", "count": len(rows)})
                except Exception:
                    pass
    return rows

def print_events_table(exclude: list[str] | None = None, include_canceled: bool = False, output_json: str | None = None) -> None:
    """
    Print a nice table of instance maintenance events grouped by states:
    CANCELED, FAILED, PROCESSING, SCHEDULED, STARTED, SUCCEEDED (raw, unnormalized).
    """
    try:
        from rich.console import Console
        from rich.table import Table
    except Exception:
        Console = None
        Table = None

    rows = []
    if Console:
        console = Console()
        # Cycle themed messages + spinners; slower cadence so messages can be read
        spin_msgs = SPIN_MSGS if SPIN_MSGS else [{"text": "Working…", "spinner": "dots"}]
        cycle_msgs = itertools.cycle(random.sample(spin_msgs, len(spin_msgs)))
        first = next(cycle_msgs)
        try:
            ctx_status = console.status(first["text"], spinner=first.get("spinner", "dots"))
        except Exception:
            ctx_status = console.status(first["text"], spinner="dots")
        with ctx_status as status:
            last = {"t": 0.0}
            def _cb(_info=None):
                try:
                    now = time.monotonic()
                    if now - last["t"] >= MESSAGE_CYCLE_SECONDS:
                        m = next(cycle_msgs)
                        # Try to update spinner to match the message; fallback if unsupported
                        try:
                            status.update(m["text"], spinner=m.get("spinner", "dots"))
                        except Exception:
                            status.update(m["text"])
                        last["t"] = now
                except Exception:
                    pass
            rows = build_event_rows(progress_cb=_cb)
    else:
        # Plain text fallback progress
        texts = [m["text"] for m in SPIN_MSGS] if 'SPIN_MSGS' in globals() else []
        messages = itertools.cycle(random.sample(texts, len(texts)) if texts else ["Loading events…"])
        first = next(messages)
        print(first)
        last = {"t": 0.0}
        def _plain_cb(_info=None):
            now = time.monotonic()
            if now - last["t"] >= MESSAGE_CYCLE_SECONDS:
                print(next(messages))
                last["t"] = now
        rows = build_event_rows(progress_cb=_plain_cb)

    # Apply state filtering
    excluded_states = set(DEFAULT_STATE_EXCLUDE)
    if include_canceled:
        excluded_states.discard("CANCELED")
    if exclude:
        excluded_states.update(s.upper() for s in exclude)
    rows = [r for r in rows if (r.get("state") or "").upper() not in excluded_states]

    def _sort_key(r: dict):
        # Group by hostname first so related events stay adjacent, then by state order.
        state_weight = _STATE_ORDER.get(r.get("state", ""), 999)
        hostname = r.get("hostname") or ""
        unknown_host = 1 if hostname == "(unknown)" else 0
        return (unknown_host, hostname, state_weight, r.get("instance_ocid") or "")

    # JSON output path (kept in sync with discovery)
    if output_json is not None:
        rows_out = sorted(rows, key=_sort_key)
        print_json_data(rows_out, output_json)
        return

    if Console and Table:
        console = Console()
        if not rows:
            console.print("No maintenance events found.")
            return

        table = Table(title="Instance Maintenance Events", show_lines=False)
        table.add_column("Hostname", no_wrap=True)
        table.add_column("Maintenance Event Type")
        table.add_column("State", no_wrap=True)
        table.add_column("Slurm State", no_wrap=True)
        table.add_column("Time in State", no_wrap=True)
        table.add_column("Created/Started", no_wrap=True)
        table.add_column("Fault IDs")
        table.add_column("Instance OCID", no_wrap=True)

        style_map = {
            "SCHEDULED": "cyan",
            "STARTED": "yellow",
            "PROCESSING": "bright_yellow",
            "IN_PROGRESS": "bright_yellow",
            "SUCCEEDED": "green",
            "FAILED": "red",
            "CANCELED": "magenta",
        }

        for r in sorted(rows, key=_sort_key):
            st = r.get("state") or "(unknown)"
            style = style_map.get(st)
            st_disp = f"[{style}]{st}[/{style}]" if style else st
            table.add_row(
                r.get("hostname") or "",
                r.get("display_name") or "",
                st_disp,
                r.get("slurm_state") or "",
                r.get("time_in_state") or "—",
                r.get("created") or "—",
                ", ".join(r.get("fault_ids") or []) if r.get("fault_ids") else "(none)",
                r.get("instance_ocid") or "",
            )

        console.print(table)
    else:
        print("Instance Maintenance Events")
        if not rows:
            print("  (none)")
            return
        for r in sorted(rows, key=_sort_key):
            print(
                f"host={r.get('hostname') or ''} | "
                f"type={r.get('display_name') or ''} | "
                f"state={r.get('state') or '(unknown)'} | "
                f"slurm={r.get('slurm_state') or ''} | "
                f"state_time={r.get('time_in_state') or '—'} | "
                f"created={r.get('created') or '—'} | "
                f"fault_ids={', '.join(r.get('fault_ids') or []) if r.get('fault_ids') else '(none)'} | "
                f"inst={r.get('instance_ocid') or ''}"
            )
