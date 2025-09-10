from __future__ import annotations

import json
import sys
import shutil, subprocess
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, cast

# Lightweight, reusable formatting utilities for tables and JSON output.

def run_with_status(message: str, fn: Callable[..., Any], *args, spinner: str = "dots", **kwargs) -> Any:
    """
    Run a callable while showing a transient status indicator.
    - Uses Rich spinner when available; otherwise prints a one-line message that is cleared before output.
    - Does not add sleeps; non-blocking visual only.
    """
    try:
        from rich.console import Console  # type: ignore
        console = Console()
        with console.status(message, spinner=spinner):
            return fn(*args, **kwargs)
    except Exception:
        # Plain-text fallback (ephemeral)
        try:
            sys.stdout.write(message)
            sys.stdout.flush()
            result = fn(*args, **kwargs)
            # Clear the line using carriage return
            sys.stdout.write("\r")
            sys.stdout.flush()
            return result
        except Exception:
            # If something went wrong with ephemeral output, just run and return
            return fn(*args, **kwargs)


def print_json_data(data: Any, output: Optional[str] = None) -> None:
    """
    Print JSON data to stdout (if output is None, '-' or empty) or write to a file path.
    """
    target_stdout = output is None or output == "-" or output == ""
    if target_stdout:
        # If jq is available, pipe JSON through jq for pretty/colorized output; otherwise fallback to Python pretty-print.
        try:
            if shutil.which("jq"):
                json_str = json.dumps(data, indent=2, sort_keys=False, default=str)
                jq_cmd = ["jq", "."]
                # Use colorized output when writing to a TTY
                try:
                    if sys.stdout.isatty():
                        jq_cmd.insert(1, "-C")
                except Exception:
                    pass
                subprocess.run(jq_cmd, input=json_str, text=True, check=False)
            else:
                raise RuntimeError("jq not found")
        except Exception:
            json.dump(data, sys.stdout, indent=2, sort_keys=False, default=str)
            sys.stdout.write("\n")
            sys.stdout.flush()
    else:
        path = cast(str, output)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=False, default=str)


def print_table(
    title: str,
    columns: Sequence[Dict[str, Any]],
    rows: Iterable[Dict[str, Any]],
    style_map: Optional[Dict[str, str]] = None,
    state_key: Optional[str] = None,
) -> None:
    """
    Render a table given a list of column specs and row dicts.
    columns: list of dicts with keys:
      - header: str (column header)
      - key: str (field key to pull from each row)
      - no_wrap: bool (optional; default False)
    rows: iterable of dicts providing values for each key
    style_map: optional mapping of a state value -> Rich color style for that row's state cell
    state_key: key in row for state (used for colorizing the first column if it is State)
    """
    try:
        from rich.console import Console  # type: ignore
        from rich.table import Table  # type: ignore
        console = Console()
        table = Table(title=title, show_lines=False)
        for col in columns:
            table.add_column(str(col.get("header", "")), no_wrap=bool(col.get("no_wrap", False)))

        for r in rows:
            rendered: List[str] = []
            for idx, col in enumerate(columns):
                key = str(col.get("key", ""))
                val = r.get(key, "")
                if isinstance(val, list):
                    val = ", ".join(str(x) for x in val)
                val_str = "" if val is None else str(val)
                # Colorize state cell if applicable
                if state_key and key == state_key and style_map:
                    style = style_map.get(val_str)
                    if style:
                        val_str = f"[{style}]{val_str}[/{style}]"
                rendered.append(val_str)
            table.add_row(*rendered)
        console.print(table)
    except Exception:
        # Plain text fallback
        headers = [str(c.get("header", "")) for c in columns]
        print(title)
        print(" | ".join(headers))
        print("-" * (sum(len(h) for h in headers) + 3 * (len(headers) - 1)))
        for r in rows:
            vals: List[str] = []
            for c in columns:
                key = str(c.get("key", ""))
                v = r.get(key, "")
                if isinstance(v, list):
                    v = ", ".join(str(x) for x in v)
                vals.append("" if v is None else str(v))
            print(" | ".join(vals))
