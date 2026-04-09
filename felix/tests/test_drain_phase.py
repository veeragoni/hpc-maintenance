from types import SimpleNamespace

from felix.models import MaintenanceJob
from felix.phases import drain as drain_phase
from felix import slrum_utils


def make_job(hostname: str = "gpu-2788") -> MaintenanceJob:
    event = SimpleNamespace(
        display_name="DOWNTIME_HOST_MAINTENANCE",
        instance_action="",
        id="evt-1",
    )
    return MaintenanceJob(
        event=event,
        hostname=hostname,
        fault_str="HPCRDMA-0002-02",
        approved_fault="HPCRDMA-0002-02",
    )


def test_execute_skips_drain_request_when_node_already_has_drain_flag(monkeypatch):
    job = make_job()
    calls = []
    events = []

    monkeypatch.setattr(drain_phase, "SKIP_DRAIN_CHECK", False)
    monkeypatch.setattr(drain_phase, "is_drained", lambda host: True, raising=False)
    monkeypatch.setattr(drain_phase, "drain", lambda host, reason: calls.append(("drain", host, reason)))
    monkeypatch.setattr(drain_phase, "wait_drained_empty", lambda host: calls.append(("wait", host)))
    monkeypatch.setattr(drain_phase, "log_event", lambda event: events.append(event))

    drain_phase.execute(job)

    assert calls == []
    assert events == [{"phase": "drain", "action": "already_drained", "host": "gpu-2788"}]


def test_execute_requests_drain_and_waits_when_node_is_not_already_drained(monkeypatch):
    job = make_job()
    calls = []
    events = []

    monkeypatch.setattr(drain_phase, "SKIP_DRAIN_CHECK", False)
    monkeypatch.setattr(drain_phase, "is_drained", lambda host: False, raising=False)
    monkeypatch.setattr(drain_phase, "drain", lambda host, reason: calls.append(("drain", host, reason)))
    monkeypatch.setattr(drain_phase, "wait_drained_empty", lambda host: calls.append(("wait", host)))
    monkeypatch.setattr(drain_phase, "log_event", lambda event: events.append(event))

    drain_phase.execute(job)

    assert calls == [
        ("drain", "gpu-2788", "NTR HPCRDMA-0002-02"),
        ("wait", "gpu-2788"),
    ]
    assert events == [
        {
            "phase": "drain",
            "action": "requested",
            "host": "gpu-2788",
            "reason": "NTR HPCRDMA-0002-02",
        },
        {"phase": "drain", "action": "drained_empty", "host": "gpu-2788"},
    ]


def test_state_has_flag_matches_exact_drain_flag_only():
    assert slrum_utils.state_has_flag("drain", "drain")
    assert slrum_utils.state_has_flag("idle+drain", "drain")
    assert not slrum_utils.state_has_flag("draining", "drain")
