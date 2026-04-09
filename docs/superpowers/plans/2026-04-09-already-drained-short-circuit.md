# Already Drained Short-Circuit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Felix continue immediately when a node is already in a real Slurm `DRAIN` state before the drain phase starts.

**Architecture:** Add a tiny Slurm-state helper for exact flag detection, then branch early in the drain phase. Keep the current request-and-wait behavior unchanged for nodes that are not already drained.

**Tech Stack:** Python, pytest, monkeypatch

---

### Task 1: Add failing drain-phase tests

**Files:**
- Create: `felix/tests/test_drain_phase.py`
- Modify: `felix/phases/drain.py`
- Modify: `felix/slrum_utils.py`
- Test: `felix/tests/test_drain_phase.py`

- [ ] **Step 1: Write the failing test**

```python
def test_execute_skips_drain_request_when_node_already_has_drain_flag(...):
    ...
    assert events == [{"phase": "drain", "action": "already_drained", "host": "gpu-2788"}]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest felix/tests/test_drain_phase.py -q`
Expected: FAIL because the current implementation still requests drain unconditionally.

- [ ] **Step 3: Add a second test for the normal path**

```python
def test_execute_requests_drain_and_waits_when_node_is_not_already_drained(...):
    ...
    assert calls == ["drain", "wait"]
```

- [ ] **Step 4: Run test file to verify the first test still fails for the intended reason**

Run: `pytest felix/tests/test_drain_phase.py -q`
Expected: one failing shortcut test, normal-path test may fail or pass depending on import wiring.

### Task 2: Implement the minimal shortcut

**Files:**
- Modify: `felix/slrum_utils.py`
- Modify: `felix/phases/drain.py`
- Test: `felix/tests/test_drain_phase.py`

- [ ] **Step 1: Add exact drain-flag detection helper**

```python
def state_has_flag(state: str, flag: str) -> bool:
    tokens = [token for token in re.split(r"[^a-z]+", state.lower()) if token]
    return flag.lower() in tokens

def is_drained(host: str) -> bool:
    return state_has_flag(get_state(host), "drain")
```

- [ ] **Step 2: Add early return in the drain phase**

```python
if is_drained(job.hostname):
    log_event({"phase": "drain", "action": "already_drained", "host": job.hostname})
    return
```

- [ ] **Step 3: Run the focused tests**

Run: `pytest felix/tests/test_drain_phase.py -q`
Expected: PASS

- [ ] **Step 4: Run the existing lightweight test suite**

Run: `pytest felix/tests/test_utils.py felix/tests/test_oci_utils.py -q`
Expected: PASS
