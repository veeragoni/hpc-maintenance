import logging, os, subprocess, sys
from typing import Optional, Dict, Any
from pathlib import Path

log = logging.getLogger(__name__)

def _find_manage_py() -> Optional[Path]:
    """
    Try to locate the external MGMT manage.py CLI.
    Preference order:
      1) MGMT_MANAGE_PATH env var (absolute or relative)
      2) Common relative locations near this repo
    """
    env_path = os.getenv("MGMT_MANAGE_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p.resolve()

    # Prefer absolute bind-mounted path if present (as used by discovery.py)
    abs_bind = Path("/config/mgmt/manage.py")
    if abs_bind.exists():
        return abs_bind.resolve()

    candidates = [
        Path("config/mgmt/manage.py"),
        Path("../config/mgmt/manage.py"),
        Path("../../config/mgmt/manage.py"),
        Path("../../../config/mgmt/manage.py"),
        Path("../../../../config/mgmt/manage.py"),
    ]
    for p in candidates:
        if p.exists():
            return p.resolve()
    return None

def _venv_python() -> Path:
    """
    Prefer the project-local virtualenv interpreter to run manage.py,
    falling back to the current Python executable.
    """
    v = Path(".venv/bin/python3")
    return v if v.exists() else Path(sys.executable)

# New helpers to query MGMT inventory and map OCID -> hostname
def _mgmt_nodes_list_json() -> Optional[str]:
    manage_py = _find_manage_py()
    if not manage_py:
        return None
    cmd = [str(_venv_python()), str(manage_py), "nodes", "list", "--format", "json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode == 0 and proc.stdout:
            return proc.stdout
    except Exception:
        pass
    return None

def _hostname_for_ocid(instance_ocid: str) -> Optional[str]:
    try:
        njson = _mgmt_nodes_list_json()
        if not njson:
            return None
        import json as _json
        arr = _json.loads(njson)
        for n in arr:
            if n.get("ocid") == instance_ocid:
                return n.get("hostname")
    except Exception:
        pass
    return None

def mgmt_update_node_status(hostname: str, status: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Attempt to update MGMT to reflect node status transitions (e.g., 'NTR scheduled').
    Uses external CLI:
      manage.py configurations update --name "<host>" --fields 'status="NTR scheduled",compute_status="ntr"'
    Configuration:
      - MGMT_MANAGE_PATH: optional path to manage.py (absolute or relative)
    Notes:
      - If the external CLI is not found, this will log and return without raising.
      - We prefer updating by hostname; if unavailable, we will try instance OCID (if MGMT CLI accepts it).
    """
    details = details or {}
    manage_py = _find_manage_py()
    inst_ocid = details.get("instance_id") or details.get("ocid")

    # Fallback to logging only if MGMT CLI not found
    if not manage_py:
        log.info("MGMT update (CLI not found): host=%s status=%s details=%s", hostname, status, details)
        return

    # Determine identifier to pass to --name
    # Prefer instance OCID per operator request; fallback to hostname.
    target_name = (inst_ocid or hostname or "")

    if not target_name:
        log.warning("MGMT update skipped: neither hostname nor instance OCID available. details=%s", details)
        return

    # Build fields string. We set both status and compute_status where possible.
    # status uses the requested value; compute_status is a lowercased hint for MGMT UIs.
    compute_status = "ntr" if status.lower().startswith("ntr") else status.lower()
    fields_value = f'status="{status}",compute_status="{compute_status}"'

    # Try with instance OCID first, then with hostname resolved from MGMT, then raw hostname
    attempt_names: list[str] = []
    if inst_ocid:
        attempt_names.append(inst_ocid)
        hn = _hostname_for_ocid(inst_ocid)
        if hn:
            attempt_names.append(hn)
    if hostname and hostname not in attempt_names:
        attempt_names.append(hostname)

    for name in attempt_names:
        cmd = [
            str(_venv_python()),
            str(manage_py),
            "configurations",
            "update",
            "--name",
            name,
            "--fields",
            fields_value,
        ]
        try:
            log.debug("Executing MGMT CLI: %s", " ".join(cmd))
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.returncode == 0:
                log.info("MGMT updated: name=%s fields=%s", name, fields_value)
                if proc.stdout:
                    log.debug("MGMT stdout: %s", proc.stdout.strip())
                break
            else:
                log.warning("MGMT update failed (rc=%s): name=%s fields=%s stderr=%s",
                            proc.returncode, name, fields_value, (proc.stderr or "").strip())
        except Exception as e:
            log.warning("MGMT update raised exception for name=%s: %s", name, e)
            continue

def mgmt_reconfigure_compute(nodes: list[str], fields: Optional[str] = None) -> bool:
    """
    Trigger compute-side reconfigure (rerun cloud-init) for one or more nodes using MGMT CLI:

      manage.py nodes reconfigure compute --nodes "<n1,n2,...>" [--fields "<filters>"]

    Notes:
      - --nodes accepts IPs, hostnames, OCIDs, serials or oci names.
      - Prefer passing instance OCIDs for precision when available.
      - --fields is optional (acts as a filter in MGMT); omitted by default.
      - Returns True on rc=0, False otherwise.
    """
    manage_py = _find_manage_py()
    if not manage_py:
        log.info("MGMT reconfigure compute (CLI not found): nodes=%s fields=%s", nodes, fields)
        return False

    nodes_arg = ",".join([n for n in nodes if n])
    if not nodes_arg:
        log.warning("MGMT reconfigure compute skipped: empty nodes list.")
        return False

    cmd = [
        str(_venv_python()),
        str(manage_py),
        "nodes",
        "reconfigure",
        "compute",
        "--nodes",
        nodes_arg,
    ]
    if fields:
        cmd += ["--fields", fields]

    try:
        log.debug("Executing MGMT reconfigure compute CLI: %s", " ".join(cmd))
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode == 0:
            log.info("MGMT reconfigure compute OK: nodes=%s", nodes_arg)
            if proc.stdout:
                log.debug("MGMT reconfigure compute stdout: %s", proc.stdout.strip())
            return True
        else:
            log.warning("MGMT reconfigure compute failed (rc=%s): nodes=%s stderr=%s",
                        proc.returncode, nodes_arg, (proc.stderr or "").strip())
            return False
    except Exception as e:
        log.warning("MGMT reconfigure compute raised exception: %s", e)
        return False
