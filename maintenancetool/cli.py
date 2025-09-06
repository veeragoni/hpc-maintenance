import argparse, logging
from oci.core.models import InstanceMaintenanceEvent
from .logging_util import setup_logging
from .orchestrator import run_once, run_loop
from . import __version__
from .phases import discovery, drain, maintenance, health, finalize
from .models import MaintenanceJob
from .reporting import print_faults_summary

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="maintenancetool",
        description="Automated OCI + Slurm maintenance orchestrator",
    )
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    # Subcommand for running the full maintenance workflow once
    parser_run = subparsers.add_parser("run", help="Run the full maintenance workflow once")
    parser_run.set_defaults(func=lambda args: run_once())

    # Subcommand for running the periodic maintenance loop
    parser_loop = subparsers.add_parser("loop", help="Run the periodic maintenance loop (15m interval by default)")
    parser_loop.set_defaults(func=lambda args: run_loop())

    # Subcommand for reporting: show raw fault codes and node mapping
    parser_report = subparsers.add_parser("report", help="Print discovered fault codes and node mapping")
    parser_report.set_defaults(func=lambda args: print_faults_summary())

    # Subcommand for discovery phase
    parser_discovery = subparsers.add_parser("discover", help="Run discovery phase")
    parser_discovery.set_defaults(func=lambda args: discovery.discover())

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
