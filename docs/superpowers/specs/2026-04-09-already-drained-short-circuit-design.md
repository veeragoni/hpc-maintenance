# Already Drained Short-Circuit Design

## Problem

The drain phase always issues `STATE=DRAIN` and, unless `SKIP_DRAIN_CHECK` is enabled, waits for `IDLE+DRAIN`. When an operator has already placed a node into a real drain state before Felix runs, `felix stage` can appear stuck because the workflow does not recognize the pre-existing drain state as sufficient to continue.

## Goal

If the current Slurm node state already includes the `DRAIN` flag, Felix should skip reissuing the drain command and continue to the next step immediately.

## Non-Goals

- Do not change the normal drain path for nodes that are not already drained.
- Do not treat `DRAINING` as equivalent to `DRAIN`.
- Do not alter maintenance or finalize phase behavior.

## Design

1. Add a small Slurm helper that checks whether the current node state has an exact `drain` flag.
2. Call that helper at the start of the drain phase after the maintenance eligibility guard.
3. If the node is already drained:
   - log a distinct `already_drained` drain event
   - skip the `scontrol update ... STATE=DRAIN` call
   - skip the `IDLE+DRAIN` wait
   - return so the orchestrator can proceed to maintenance
4. If the node is not already drained, preserve the current behavior.

## Notes

- The helper must distinguish `drain` from `draining`.
- Tests should cover both the shortcut path and the existing normal path.
