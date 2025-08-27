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
        out = run_cmd(["sinfo", "-h", "-N", "-o", "%N %t"])
        for line in out.splitlines():
            n, state = line.split(None, 1)
            if n == host and state.lower().startswith(target):
                log.info("Node %s reached state %s", host, target)
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
