import time, logging, re, json
from typing import Any, Dict, List, Optional
from .utils import run_cmd
from .config import DRAIN_POLL_SEC
from .formatting import print_json_data

log = logging.getLogger(__name__)

def drain(host: str, reason: str) -> None:
    run_cmd([
        "sudo", "scontrol", "update",
        f"NODENAME={host}",
        f'REASON="{reason}"', "STATE=DRAIN"
    ])
    log.info("Requested DRAIN for %s", host)

def wait_state(host: str, target: str = "drain") -> None:
    while True:
        state = get_state(host)
        if target in state:
            log.info("Node %s reached state containing %s (state=%s)", host, target, state)
            return
        time.sleep(DRAIN_POLL_SEC)

def wait_drained_empty(host: str) -> None:
    """Wait until node is drained AND empty (IDLE+DRAIN)."""
    while True:
        state = get_state(host)
        if ("drain" in state) and ("idle" in state):
            log.info("Node %s is drained and idle (state=%s)", host, state)
            return
        time.sleep(DRAIN_POLL_SEC)

def resume(host: str) -> None:
    run_cmd(["sudo", "scontrol", "update",
             f"NODENAME={host}", "STATE=RESUME",
             'REASON="Maintenance_OK"'])

def mark_ntr(host: str) -> None:
    run_cmd(["sudo", "scontrol", "update",
             f"NODENAME={host}", "STATE=DRAIN",
             'REASON="PostMaint_Failure"', "FEATURES+=NTR"])

def set_reason(host: str, reason: str) -> None:
    """Update only the Slurm reason without changing state."""
    run_cmd([
        "sudo", "scontrol", "update",
        f"NODENAME={host}",
        f'REASON="{reason}"'
    ])
    log.info('Updated reason for %s -> "%s"', host, reason)

def get_state(host: str) -> str:
    """Return the current Slurm state token (lowercased). Preserves flags like '+drain'."""
    out = run_cmd(["scontrol", "show", "node", host])
    state_token = ""
    for token in out.replace("\n", " ").split():
        if token.startswith("State="):
            state_token = token.split("=", 1)[1]
            # Strip any trailing punctuation (commas) while preserving flags like '+DRAIN'
            state_token = state_token.strip().rstrip(",")
            break
    state = state_token.strip().lower()
    return state


# -----------------------------
# Additional Slurm query helpers
# -----------------------------

def _parse_cpus_field(c: str) -> Dict[str, int] | Dict[str, str]:
    """
    Parse Slurm %C field formatted as 'alloc/idle/other/total' into a dict.
    If parsing fails, return {'raw': original}.
    """
    parts = c.split("/")
    if len(parts) == 4:
        try:
            return {
                "alloc": int(parts[0]),
                "idle": int(parts[1]),
                "other": int(parts[2]),
                "total": int(parts[3]),
            }
        except Exception:
            pass
    return {"raw": c}


def expand_hostlist(expr: str) -> List[str]:
    """
    Expand a Slurm hostlist expression (e.g., GPU-[1-3,8]) to concrete node names.
    Uses: scontrol show hostlist <expr>
    """
    try:
        out = run_cmd(["scontrol", "show", "hostlist", expr]).strip()
        if not out:
            return []
        return [h.strip() for h in out.replace("\n", "").split(",") if h.strip()]
    except Exception:
        return []


def sinfo_reasons() -> List[Dict[str, str]]:
    """
    Return reasons from 'sinfo -R' as a list of dicts:
    {node, user, timestamp, reason}
    Implementation notes:
      - Uses '-h' to suppress headers.
      - Parses last three tokens as user + timestamp + node; the remainder is the reason (may contain spaces).
    """
    out = run_cmd(["sinfo", "-R", "-h"]).splitlines()
    rows: List[Dict[str, str]] = []
    for line in out:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        node = parts[-1]
        timestamp = parts[-2]
        user = parts[-3] if len(parts) >= 3 else ""
        reason = " ".join(parts[:-3]).strip()
        rows.append({"node": node, "user": user, "timestamp": timestamp, "reason": reason})
    return rows


def sinfo_nodes() -> List[Dict[str, object]]:
    """
    Return node-level info using 'sinfo -N -o %N|%t|%C' as:
    {node, state, cpus: {'alloc','idle','other','total'} or {'raw': str}}
    """
    out = run_cmd(["sinfo", "-N", "-h", "-o", "%N|%t|%C"]).splitlines()
    rows: List[Dict[str, object]] = []
    for line in out:
        line = line.strip()
        if not line:
            continue
        fields = line.split("|")
        if len(fields) < 3:
            continue
        node, state, cpus = fields[0].strip(), fields[1].strip(), fields[2].strip()
        rows.append({"node": node, "state": state, "cpus": _parse_cpus_field(cpus)})
    return rows


def sinfo_partitions() -> List[Dict[str, str]]:
    """
    Partition summary using 'sinfo -s -o %P|%a|%l|%F|%D|%C'
    Returns dicts with keys: partition, availability, timelimit, features, nodes, cpus
    """
    out = run_cmd(["sinfo", "-s", "-h", "-o", "%P|%a|%l|%F|%D|%C"]).splitlines()
    rows: List[Dict[str, str]] = []
    for line in out:
        line = line.strip()
        if not line:
            continue
        fields = line.split("|")
        if len(fields) < 6:
            continue
        rows.append({
            "partition": fields[0].strip(),
            "availability": fields[1].strip(),
            "timelimit": fields[2].strip(),
            "features": fields[3].strip(),
            "nodes": fields[4].strip(),
            "cpus": fields[5].strip(),
        })
    return rows


def sinfo_all() -> List[Dict[str, object]]:
    """
    Combined partition/node view using 'sinfo -o %P|%N|%t|%C'
    Returns dicts with: partition, node, state, cpus
    """
    out = run_cmd(["sinfo", "-h", "-o", "%P|%N|%t|%C"]).splitlines()
    rows: List[Dict[str, object]] = []
    for line in out:
        line = line.strip()
        if not line:
            continue
        fields = line.split("|")
        if len(fields) < 4:
            continue
        rows.append({
            "partition": fields[0].strip(),
            "node": fields[1].strip(),
            "state": fields[2].strip(),
            "cpus": _parse_cpus_field(fields[3].strip()),
        })
    return rows


def scontrol_show_node(host: str) -> Dict[str, str]:
    """
    Return 'scontrol show node <host>' as a dict of key/value pairs, preserving raw string in 'raw'.
    Keys include at least NodeName, State, Reason, Features if present. Convenience lower-case aliases are added.
    """
    out = run_cmd(["scontrol", "show", "node", host])
    info: Dict[str, str] = {"raw": out}
    flat = out.replace("\n", " ")
    for m in re.finditer(r"(\w+)=([^\s]+)", flat):
        k = m.group(1)
        v = m.group(2).rstrip(",")
        info[k] = v
    # Convenience lower/alternate keys
    if "NodeName" in info:
        info["node"] = info["NodeName"]
    if "State" in info:
        info["state"] = info["State"]
    if "Reason" in info:
        info["reason"] = info["Reason"]
    if "Features" in info:
        info["features"] = info["Features"]
    return info


def slurm_node_status_map() -> Dict[str, Dict[str, object]]:
    """
    Build a quick map of node -> {state, cpus, reason?, reason_user?, reason_timestamp?}
    Uses sinfo_nodes() for state/cpus and sinfo_reasons() for reason lines.
    """
    nodes: Dict[str, Dict[str, object]] = {}
    for row in sinfo_nodes():
        node = str(row.get("node", ""))
        if not node:
            continue
        state = str(row.get("state", ""))
        nodes[node] = {"state": state, "cpus": row.get("cpus")}
    for row in sinfo_reasons():
        node = row["node"]
        if node in nodes:
            nodes[node]["reason"] = row["reason"]
            nodes[node]["reason_user"] = row["user"]
            nodes[node]["reason_timestamp"] = row["timestamp"]
        else:
            nodes[node] = {
                "state": "unknown",
                "reason": row["reason"],
                "reason_user": row["user"],
                "reason_timestamp": row["timestamp"],
            }
    return nodes


# -----------------------------
# JSON convenience wrappers
# -----------------------------

def _to_json_output(data: Any, output: Optional[str] = None):
    """
    If output is None or empty, return a JSON string.
    If output is "-", write to stdout.
    Otherwise write to the given file path. Returns None in write modes.
    """
    if output is None or output == "":
        return json.dumps(data, indent=2, sort_keys=False, default=str)
    if output == "-":
        print_json_data(data, None)
        return None
    print_json_data(data, output)
    return None


def sinfo_reasons_json(output: Optional[str] = None):
    data = sinfo_reasons()
    return _to_json_output(data, output)


def sinfo_nodes_json(output: Optional[str] = None):
    data = sinfo_nodes()
    return _to_json_output(data, output)


def sinfo_partitions_json(output: Optional[str] = None):
    data = sinfo_partitions()
    return _to_json_output(data, output)


def sinfo_all_json(output: Optional[str] = None):
    data = sinfo_all()
    return _to_json_output(data, output)


def scontrol_show_node_json(host: str, output: Optional[str] = None):
    data = scontrol_show_node(host)
    return _to_json_output(data, output)


def slurm_node_status_map_json(output: Optional[str] = None):
    data = slurm_node_status_map()
    return _to_json_output(data, output)
