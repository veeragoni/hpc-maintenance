import subprocess
import logging
from typing import Dict, Iterator, Any, Callable

def run_cmd(cmd: list[str], *, check: bool = True) -> str:
    log = logging.getLogger(__name__)
    log.debug("Running command: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd, text=True, capture_output=True
    )
    if check and proc.returncode:
        log.error("Command failed (%s): %s", proc.returncode, proc.stderr.strip())
        raise RuntimeError(proc.stderr.strip())
    return proc.stdout.strip()

def paginated(gen_fn: Callable[..., Any], **kwargs) -> Iterator[Any]:
    """Generic OCI pagination helper."""
    while True:
        resp = gen_fn(**kwargs)
        yield from resp.data
        if resp.next_page is None:
            break
        kwargs["page"] = resp.next_page