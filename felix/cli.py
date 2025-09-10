import argparse, logging
from oci.core.models import InstanceMaintenanceEvent
from .logging_util import setup_logging
from .orchestrator import run_once, run_loop, run_stage, run_catchup
from . import __version__
from .phases import discovery, drain, maintenance, health, finalize
from .models import MaintenanceJob
from .reporting import print_events_table

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="felix",
        description="Automated OCI + Slurm maintenance orchestrator",
    )
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    # Subcommand for running the full maintenance workflow once
    parser_run = subparsers.add_parser("run", help="Run the full maintenance workflow once")
    parser_run.add_argument("--dry-run", "-n", action="store_true", help="Do not make changes; show what would be done")
    parser_run.set_defaults(func=lambda args: run_once(dry_run=args.dry_run))

    # Subcommand for running the periodic maintenance loop
    parser_loop = subparsers.add_parser("loop", help="Run the periodic maintenance loop (15m interval by default)")
    parser_loop.add_argument("--dry-run", "-n", action="store_true", help="Do not make changes; show what would be done each iteration")
    parser_loop.set_defaults(func=lambda args: run_loop(dry_run=args.dry_run))

    # Subcommand for staging only (discover -> drain -> schedule)
    parser_stage = subparsers.add_parser("stage", help="Discover -> drain -> schedule (if event state=SCHEDULED); skips health/finalize")
    parser_stage.add_argument("--dry-run", "-n", action="store_true", help="Do not make changes; show what would be done")
    parser_stage.set_defaults(func=lambda args: run_stage(dry_run=args.dry_run))

    # Subcommand for reporting: show maintenance events table
    parser_report = subparsers.add_parser("report", help="Show all instance maintenance events (table)")
    parser_report.add_argument("--include-canceled", action="store_true", help="Include CANCELED events in the table")
    parser_report.add_argument("-x", "--exclude", action="append", default=None, help="Exclude events in the given state (can be repeated)")
    parser_report.add_argument("--json", nargs="?", const="-", metavar="FILE", help="Output JSON to stdout (no FILE) or write to FILE; skips table")
    parser_report.set_defaults(func=lambda args: print_events_table(exclude=args.exclude, include_canceled=args.include_canceled, output_json=args.json))

    # Subcommand for discovery phase
    # Reports SCHEDULED events only; read-only (no changes applied)
    # By default shows a Rich table; --json FILE writes full JSON and skips table output
    parser_discovery = subparsers.add_parser("discover", help="Run discovery phase (reports SCHEDULED; no changes)")
    parser_discovery.add_argument("--json", nargs="?", const="-", metavar="FILE", help="Output JSON to stdout (no FILE) or write to FILE; skips table")
    parser_discovery.set_defaults(func=lambda args: discovery.run_cli(output_json=args.json))

    # Subcommand for drain phase
    parser_drain = subparsers.add_parser("drain", help="Run drain phase")
    parser_drain.add_argument("hostname", help="Hostname to drain")
    parser_drain.set_defaults(func=lambda args: _exec_phase_with_discovery(args.hostname, drain.execute))

    # Subcommand for maintenance phase
    parser_maintenance = subparsers.add_parser("maintenance", help="Run maintenance phase")
    parser_maintenance.add_argument("hostname", help="Hostname to maintain")
    parser_maintenance.set_defaults(func=lambda args: _exec_phase_with_discovery(args.hostname, maintenance.execute))

    # Subcommand for health phase
    parser_health = subparsers.add_parser("health", help="Run health phase")
    parser_health.add_argument("hostname", help="Hostname to check health")
    parser_health.set_defaults(func=lambda args: _exec_phase_with_discovery(args.hostname, health.execute))

    # Subcommand for finalize phase
    parser_finalize = subparsers.add_parser("finalize", help="Run finalize phase")
    parser_finalize.add_argument("hostname", help="Hostname to finalize")
    parser_finalize.set_defaults(func=lambda args: _exec_phase_with_discovery(args.hostname, finalize.execute))

    # Subcommand for one-shot catch-up (no drain/schedule)
    # - SUCCEEDED/COMPLETED: health -> finalize and set MGMT to "running"
    # - IN_PROGRESS/PROCESSING: set MGMT to "NTR scheduled" + reconfigure compute
    parser_catchup = subparsers.add_parser("catchup", help="One-shot reconciliation for already-triggered maintenance (no drain/schedule)")
    parser_catchup.add_argument("--host", help="Limit to a specific hostname", default=None)
    parser_catchup.add_argument("--dry-run", "-n", action="store_true", help="Preview actions without changing Slurm/OCI/MGMT")
    parser_catchup.set_defaults(func=lambda args: run_catchup(dry_run=args.dry_run, host=args.host))

    args = parser.parse_args()

    setup_logging()

    # Helper function to execute a phase after discovery
    def _exec_phase_with_discovery(hostname, phase_fn):
        from .phases.discovery import discover
        jobs = discover()
        job = next((j for j in jobs if j.hostname == hostname), None)
        if not job:
            print(f"Error: No maintenance job/event found for hostname '{hostname}'.")
            return
        return phase_fn(job)

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
