import time, logging
from .utils import run_cmd
from .config import DRAIN_POLL_SEC

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
