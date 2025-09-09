import logging
import json
import time
import itertools, random
from typing import Dict, List
from .phases import discovery
from .config import get_approved_faults
from .oci_utils import compute_client, list_compartments
from .utils import paginated, run_cmd

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

# Event discovery and table rendering (reusable)
# State ordering priority for table sorting.
# Lower numbers sort earlier in the table.
# IMPORTANT: SCHEDULED must appear first; PROCESSING/IN_PROGRESS/STARTED next; SUCCEEDED next; FAILED/CANCELED last.
# Do not change without stakeholder approval to avoid reshuffling in downstream UIs.
_STATE_ORDER = {
    "SCHEDULED": 1,
    "PROCESSING": 2,
    "IN_PROGRESS": 2,
    "STARTED": 2,
    "SUCCEEDED": 3,
    "FAILED": 5,
    "CANCELED": 5,
}
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

def list_all_events(progress_cb=None) -> list[dict]:
    """
    Return a list of all instance maintenance events across compartments with raw fields.
    Each row contains:
      - state (raw lifecycle_state from provider)
      - hostname (resolved via MGMT, or '(unknown)')
      - instance_ocid
      - event_ocid
      - fault_ids (list[str])
    """
    hmap = _mgmt_host_map()
    rows: list[dict] = []
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

            rows.append({
                "state": raw_state,
                "hostname": hostname,
                "instance_ocid": ev.instance_id,
                "event_ocid": ev.id,
                "fault_ids": fault_ids,
            })
            if progress_cb and (len(rows) % 20 == 0):
                try:
                    progress_cb({"phase": "progress", "count": len(rows)})
                except Exception:
                    pass
    return rows

def print_events_table(exclude: list[str] | None = None, include_canceled: bool = False) -> None:
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
            rows = list_all_events(progress_cb=_cb)
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
        rows = list_all_events(progress_cb=_plain_cb)

    # Apply state filtering
    excluded_states = set(DEFAULT_STATE_EXCLUDE)
    if include_canceled:
        excluded_states.discard("CANCELED")
    if exclude:
        excluded_states.update(s.upper() for s in exclude)
    rows = [r for r in rows if (r.get("state") or "").upper() not in excluded_states]

    def _sort_key(r: dict):
        # Use preferred order if present; unknown states at the end.
        # Within each state, place hosts with "(unknown)" at the bottom.
        state_weight = _STATE_ORDER.get(r.get("state", ""), 999)
        hostname = r.get("hostname") or ""
        unknown_host = 1 if hostname == "(unknown)" else 0
        return (state_weight, unknown_host, hostname, r.get("instance_ocid") or "")

    if Console and Table:
        console = Console()
        if not rows:
            console.print("No maintenance events found.")
            return

        table = Table(title="Instance Maintenance Events", show_lines=False)
        table.add_column("State", no_wrap=True)
        table.add_column("Hostname", no_wrap=True)
        table.add_column("Instance OCID", no_wrap=True)
        table.add_column("Fault IDs")

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
                st_disp,
                r.get("hostname") or "",
                r.get("instance_ocid") or "",
                ", ".join(r.get("fault_ids") or []) if r.get("fault_ids") else "(none)",
            )

        console.print(table)
    else:
        print("Instance Maintenance Events")
        if not rows:
            print("  (none)")
            return
        for r in sorted(rows, key=_sort_key):
            print(f"- {r.get('state') or '(unknown)':>10} | {r.get('hostname') or '':<20} | inst={r.get('instance_ocid') or ''} | fault_ids={', '.join(r.get('fault_ids') or []) if r.get('fault_ids') else '(none)'}")
