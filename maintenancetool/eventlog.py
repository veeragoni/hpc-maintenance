import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from .config import EVENTS_LOG_FILE

_lock = threading.Lock()

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log_event(event: dict) -> None:
    """
    Append a single JSON event (one line) to EVENTS_LOG_FILE.
    Ensures parent directory exists and write is thread-safe.
    """
    try:
        data = dict(event)
        data.setdefault("ts", _now_iso())
        path: Path = EVENTS_LOG_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(data, separators=(",", ":"), sort_keys=False)
        with _lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        # Swallow logging errors; do not impact workflow
        pass
