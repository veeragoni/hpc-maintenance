import logging
import json
import sys
import os
from datetime import datetime, timezone
from typing import Any, Dict, cast
from ..utils import run_cmd
from ..models import MaintenanceJob
from ..config import PROCESSED_TAG
from ..oci_utils import compute_client, list_compartments
from ..utils import paginated
from oci.util import to_dict
from ..formatting import run_with_status, print_json_data, print_table
from ..slrum_utils import slurm_node_status_map
from ..events_common import build_event_rows, fmt_duration, fmt_ts, color_event_type, pick_created_or_started, STATE_ORDER as COMMON_STATE_ORDER

log = logging.getLogger(__name__)

# SYNC NOTE:
# The column order, state coloring, time-in-state rules, and Created/Started formatting
# must remain consistent with maintenancetool/reporting.py. If you modify one, mirror the change
# in the other or extract shared helpers in formatting/common modules to avoid drift.

def _host_map() -> dict[str, str]:
    log.debug("Retrieving host map")
    njson = run_cmd([
        ".venv/bin/python3",
        "/config/mgmt/manage.py", "nodes", "list", "--format", "json"
    ])
    host_map = {n["ocid"]: n["hostname"] for n in json.loads(njson)}
    log.debug("Host map contains %d entries", len(host_map))
    log.debug("Host map: %s", host_map)
    return host_map

def discover() -> list[MaintenanceJob]:
    hmap = _host_map()
    jobs: list[MaintenanceJob] = []

    for comp in list_compartments():
        for ev_sum in paginated(
                compute_client.list_instance_maintenance_events,
                compartment_id=comp):
            if ev_sum.lifecycle_state not in ("SCHEDULED",):
                continue
            event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
            if event_response is None:
                log.warning("Failed to retrieve maintenance event %s", ev_sum.id)
                continue
            ev = event_response.data
            if ev.freeform_tags.get(PROCESSED_TAG):
                log.debug("Skipping processed event %s", ev.id)
                continue

            additional = ev.additional_details or {}
            fault_details = additional.get('fault_details') or additional.get('faultDetails') or []
            # Some providers return fault_details as a JSON string; parse if needed
            if isinstance(fault_details, str):
                try:
                    fault_details = json.loads(fault_details)
                except Exception:
                    log.warning("fault_details not JSON-decodable for event %s: %s", ev.id, fault_details)
                    fault_details = []

            fault_ids = []
            for d in fault_details:
                fid = (d.get('fault_id') or d.get('faultId'))
                if fid:
                    fault_ids.append(fid)

            faults = "_".join(
                f"{(d.get('fault_id') or d.get('faultId'))}_{(d.get('component') or d.get('faultComponent'))}"
                for d in fault_details
            )
            log.debug("Processing event %s for instance %s", ev.id, ev.instance_id)
            host = hmap.get(ev.instance_id)
            if host:
                log.debug("Found hostname %s for OCID %s", host, ev.instance_id)
                jobs.append(MaintenanceJob(ev, host, faults, fault_ids=fault_ids))
    log.debug("Discovered %d maintenance jobs", len(jobs))
    return jobs

def discover_json() -> list[dict]:
    """
    Discover SCHEDULED maintenance events and return a list of JSON-serializable dicts
    representing the full InstanceMaintenanceEvent model plus:
      - hostname: resolved via MGMT (or None if not found)
      - fault_ids: list[str] extracted from additional_details.fault_details
      - faults: short summary joined from fault_id and component
    This function is intended for consumers that want raw event data instead of MaintenanceJob objects.
    """
    hmap = _host_map()
    s_map = slurm_node_status_map()
    results: list[dict] = []

    for comp in list_compartments():
        for ev_sum in paginated(
                compute_client.list_instance_maintenance_events,
                compartment_id=comp):
            if ev_sum.lifecycle_state not in ("SCHEDULED",):
                continue
            event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
            if event_response is None:
                log.warning("Failed to retrieve maintenance event %s", ev_sum.id)
                continue
            ev = event_response.data
            # Skip already-processed events, mirroring discover()
            if getattr(ev, "freeform_tags", {}) and ev.freeform_tags.get(PROCESSED_TAG):
                continue

            additional = getattr(ev, "additional_details", None) or {}
            fault_details = additional.get('fault_details') or additional.get('faultDetails') or []
            if isinstance(fault_details, str):
                try:
                    fault_details = json.loads(fault_details)
                except Exception:
                    log.warning("fault_details not JSON-decodable for event %s: %s", ev.id, fault_details)
                    fault_details = []

            fault_ids: list[str] = []
            for d in fault_details:
                fid = (d.get('fault_id') or d.get('faultId'))
                if fid:
                    fault_ids.append(fid)

            faults = "_".join(
                f"{(d.get('fault_id') or d.get('faultId'))}_{(d.get('component') or d.get('faultComponent'))}"
                for d in fault_details
            )

            # Convert full OCI model to dict (coerce to dict defensively)
            ev_raw = to_dict(ev)
            if isinstance(ev_raw, str):
                try:
                    ev_dict = cast(Dict[str, Any], json.loads(ev_raw))
                except Exception:
                    ev_dict = {}
            else:
                ev_dict = cast(Dict[str, Any], ev_raw)  # expected to already be a dict

            # Attach convenience fields
            host = hmap.get(ev.instance_id)
            ev_dict["hostname"] = host
            ev_dict["fault_ids"] = fault_ids
            ev_dict["faults"] = faults
            # Slurm enrichment (if hostname present)
            sl = (s_map.get(host or "") or {})
            ev_dict["slurm_state"] = sl.get("state")
            ev_dict["slurm_reason"] = sl.get("reason")
            ev_dict["slurm_reason_user"] = sl.get("reason_user")
            ev_dict["slurm_reason_timestamp"] = sl.get("reason_timestamp")
            results.append(ev_dict)

    log.debug("Discovered %d maintenance jobs (JSON)", len(results))
    return results


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





def _print_jobs_table(jobs: list[MaintenanceJob]) -> None:
    """
    Pretty-print SCHEDULED discovery results as a table.
    Columns: State, Hostname, Slurm State, Instance OCID, Maintenance Event Type, Fault IDs
    """
    # Sort: place "(unknown)" hostnames (if any) at the bottom, then by hostname, then instance OCID
    def _sort_key(j: MaintenanceJob):
        hostname = j.hostname or ""
        unknown_host = 1 if hostname == "(unknown)" else 0
        instance = getattr(j.event, "instance_id", "") or ""
        return (unknown_host, hostname, instance)

    s_map = slurm_node_status_map()
    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    for j in sorted(jobs, key=_sort_key):
        ev = j.event
        raw_state = getattr(ev, "lifecycle_state", None) or "(unknown)"
        state_upper = (raw_state or "").upper()
        t_started = _as_aware(getattr(ev, "time_started", None))
        t_finished = _as_aware(getattr(ev, "time_finished", None))
        t_sched = _as_aware(getattr(ev, "time_window_start", None)) or _as_aware(getattr(ev, "time_created", None)) or t_started

        # Time in current state (frozen for terminal states)
        if state_upper == "SCHEDULED":
            created = _as_aware(getattr(ev, "time_created", None))
            tin_state = _seconds_between(created, now)
        elif state_upper in ("PROCESSING", "IN_PROGRESS", "STARTED"):
            tin_state = _seconds_between(t_started, now)
        elif state_upper in ("SUCCEEDED", "FAILED", "CANCELED"):
            # Freeze at terminal: show processing duration if we have both bounds
            if t_started and t_finished:
                tin_state = _seconds_between(t_started, t_finished)
            else:
                tin_state = None
        else:
            # Fallback: since creation
            tin_state = _seconds_between(_as_aware(getattr(ev, "time_created", None)), now)


        rows.append({
            "state": raw_state,
            "time_in_state": fmt_duration(tin_state),
            "created": fmt_ts(pick_created_or_started(state_upper, ev)),
            "hostname": j.hostname or "",
            "slurm_state": ((s_map.get(j.hostname or "") or {}).get("state", "")),
            "instance_ocid": (getattr(ev, "instance_id", "") or ""),
            "display_name": color_event_type(getattr(ev, "display_name", None)),
            "fault_ids": (j.fault_ids or []),
        })

    columns = [
        {"header": "Hostname", "key": "hostname", "no_wrap": True},
        {"header": "Maintenance Event Type", "key": "display_name"},
        {"header": "State", "key": "state", "no_wrap": True},
        {"header": "Slurm State", "key": "slurm_state", "no_wrap": True},
        {"header": "Time in State", "key": "time_in_state", "no_wrap": True},
        {"header": "Created/Started", "key": "created", "no_wrap": True},
        {"header": "Instance OCID", "key": "instance_ocid", "no_wrap": True},
    ]
    style_map = {"SCHEDULED": "cyan"}
    print_table("Discovery: SCHEDULED Maintenance Events", columns, rows, style_map=style_map, state_key="state")


def run_cli(output_json: str | None = None) -> None:
    """
    CLI entrypoint for discovery:
    - Runs discovery() to gather SCHEDULED events
    - Displays a concise Rich table of the results (unless --json is supplied)
    - If output_json is provided, writes full JSON model to that file instead of printing a table
    """
    # Ensure discovery never triggers writes to OCI (read-only guard)
    try:
        os.environ["MAINT_READONLY"] = os.environ.get("MAINT_READONLY", "1") or "1"
    except Exception:
        pass

    if output_json is not None:
        # JSON mode: non-blocking status while discovering, then print or write JSON.
        data = run_with_status("Discovering SCHEDULED maintenance events…", discover_json)
        if output_json == "-" or output_json == "":
            print_json_data(data, None)
            return
        try:
            print_json_data(data, output_json)
            print(f"Wrote discovery JSON ({len(data)} event(s)) to: {output_json}")
        except Exception as e:
            print(f"Error writing JSON to {output_json}: {e}")
        return

    # Table (interactive) mode

    def _build_rows_for_cli():
        allowed = {"PROCESSING", "SCHEDULED", "STARTED", "SUCCEEDED"}
        return build_event_rows(filter_states=allowed)
        for comp in list_compartments():
            for ev_sum in paginated(
                    compute_client.list_instance_maintenance_events,
                    compartment_id=comp):
                raw_state = getattr(ev_sum, "lifecycle_state", None) or ""
                if raw_state not in allowed:
                    continue
                event_response = compute_client.get_instance_maintenance_event(ev_sum.id)
                if event_response is None:
                    continue
                ev = event_response.data
                # Skip already-processed events (same as other discovery paths)
                if getattr(ev, "freeform_tags", {}) and ev.freeform_tags.get(PROCESSED_TAG):
                    continue

                # Fault IDs extraction
                additional = getattr(ev, "additional_details", None) or {}
                fault_details = additional.get("fault_details") or additional.get("faultDetails") or []
                if isinstance(fault_details, str):
                    try:
                        fault_details = json.loads(fault_details)
                    except Exception:
                        fault_details = []
                fault_ids: list[str] = []
                for d in fault_details:
                    fid = d.get("fault_id") or d.get("faultId")
                    if fid:
                        fault_ids.append(fid)

                host = hmap.get(ev.instance_id) or "(unknown)"

                # Time-aware: time in current state
                state_upper = raw_state.upper()
                t_started = _as_aware(getattr(ev, "time_started", None))
                t_finished = _as_aware(getattr(ev, "time_finished", None))
                t_sched = _as_aware(getattr(ev, "time_window_start", None)) or _as_aware(getattr(ev, "time_created", None)) or t_started

                if state_upper == "SCHEDULED":
                    created = _as_aware(getattr(ev, "time_created", None))
                    tin_state = _seconds_between(created, now)
                elif state_upper in ("PROCESSING", "IN_PROGRESS", "STARTED"):
                    tin_state = _seconds_between(t_started, now)
                elif state_upper in ("SUCCEEDED",):
                    if t_started and t_finished:
                        tin_state = _seconds_between(t_started, t_finished)
                    else:
                        tin_state = None
                else:
                    tin_state = _seconds_between(_as_aware(getattr(ev, "time_created", None)), now)

                sl = (s_map.get(host) or {})
                rows.append({
                    "state": raw_state,
                    "time_in_state": _fmt_duration(tin_state),
                    "created": _fmt_ts(_pick_created_or_started(ev, state_upper)),
                    "hostname": host,
                    "slurm_state": sl.get("state", ""),
                    "instance_ocid": getattr(ev, "instance_id", "") or "",
                    "display_name": _color_event_type(getattr(ev, "display_name", None)),
                    "fault_ids": fault_ids,
                })

        # Sort/group by hostname (unknown last), then by state within each host, then instance OCID
        state_rank = {"PROCESSING": 0, "STARTED": 1, "SCHEDULED": 2, "SUCCEEDED": 3}
        rows.sort(
            key=lambda r: (
                1 if (r.get("hostname") == "(unknown)") else 0,
                r.get("hostname") or "",
                state_rank.get((r.get("state") or "").upper(), 99),
                r.get("instance_ocid") or "",
            )
        )
        return rows

    rows = run_with_status("Discovering maintenance events…", _build_rows_for_cli)

    # Ensure unknown hostnames sort to the bottom, mirroring reporting.py
    def _sort_key(r: dict):
        state_weight = COMMON_STATE_ORDER.get(r.get("state", ""), 999)
        hostname = r.get("hostname") or ""
        unknown_host = 1 if hostname == "(unknown)" else 0
        return (unknown_host, hostname, state_weight, r.get("instance_ocid") or "")
    rows = sorted(rows, key=_sort_key)

    columns = [
        {"header": "Hostname", "key": "hostname", "no_wrap": True},
        {"header": "Maintenance Event Type", "key": "display_name"},
        {"header": "State", "key": "state", "no_wrap": True},
        {"header": "Slurm State", "key": "slurm_state", "no_wrap": True},
        {"header": "Time in State", "key": "time_in_state", "no_wrap": True},
        {"header": "Created/Started", "key": "created", "no_wrap": True},
        {"header": "Instance OCID", "key": "instance_ocid", "no_wrap": True},
    ]
    style_map = {
        "SCHEDULED": "cyan",
        "PROCESSING": "bright_yellow",
        "STARTED": "yellow",
        "SUCCEEDED": "green",
    }
    # Summary (top): counts per state and read-only note
    try:
        from rich.console import Console
        console = Console()
        c_sched = sum(1 for r in rows if (r.get("state") or "").upper() == "SCHEDULED")
        c_proc = sum(1 for r in rows if (r.get("state") or "").upper() == "PROCESSING")
        c_started = sum(1 for r in rows if (r.get("state") or "").upper() == "STARTED")
        c_succ = sum(1 for r in rows if (r.get("state") or "").upper() == "SUCCEEDED")
        console.print("")
        console.print(f"[bold]Summary[/bold]: [cyan]SCHEDULED[/cyan]={c_sched}  [bright_yellow]PROCESSING[/bright_yellow]={c_proc}  [yellow]STARTED[/yellow]={c_started}  [green]SUCCEEDED[/green]={c_succ}")
        console.print("")
    except Exception:
        c_sched = sum(1 for r in rows if (r.get("state") or "").upper() == "SCHEDULED")
        c_proc = sum(1 for r in rows if (r.get("state") or "").upper() == "PROCESSING")
        c_started = sum(1 for r in rows if (r.get("state") or "").upper() == "STARTED")
        c_succ = sum(1 for r in rows if (r.get("state") or "").upper() == "SUCCEEDED")
        print("")
        print(f"Summary: SCHEDULED={c_sched} PROCESSING={c_proc} STARTED={c_started} SUCCEEDED={c_succ}")
        print("Discovery is read-only; only SCHEDULED events will be considered for subsequent phases.")
        print("")

    print_table("Discovery: Maintenance Events (Processing, Scheduled, Started, Succeeded)", columns, rows, style_map=style_map, state_key="state")
