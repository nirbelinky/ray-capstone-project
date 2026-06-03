"""ZoneActor — Ray remote actor owning mutable state for one pickup zone.

Each actor holds its partition of replay data and manages the per-tick
lifecycle (INACTIVE → ACTIVE → CLOSED).  All public methods return plain
dicts to avoid Ray serialization issues with frozen dataclasses.
"""

from __future__ import annotations

import logging
from typing import Any

import ray

logger = logging.getLogger(__name__)


@ray.remote
class ZoneActor:
    """Stateful actor for a single pickup zone.

    Parameters
    ----------
    zone_id : int
        Unique identifier for this zone.
    replay_data : dict[int, dict]
        Mapping from ``tick_id`` to ``{demand, hour_of_day, day_of_week}``.
    baseline : dict[tuple[int, int], dict]
        Mapping from ``(hour_of_day, day_of_week)`` to
        ``{mean_demand, std_demand}``.
    """

    # ── Construction ──────────────────────────────────────────────────────

    def __init__(
        self,
        zone_id: int,
        replay_data: dict[int, dict],
        baseline: dict[tuple[int, int], dict],
    ) -> None:
        # Identity
        self.zone_id: int = zone_id

        # Data partitions
        self.replay_data: dict[int, dict] = replay_data
        self.baseline: dict[tuple[int, int], dict] = baseline

        # Tick lifecycle
        self.tick_state: str = "INACTIVE"
        self.active_tick_id: int | None = None
        self.reported_decision: str | None = None
        self.reported_latency: float = 0.0

        # Decision history & fallback
        self.decision_history: dict[int, dict] = {}
        self.previous_decision: str = "OK"

        # Observability counters
        self.duplicate_count: int = 0
        self.late_count: int = 0
        self.fallback_count: int = 0

        logger.info(
            "ZoneActor %d initialised — %d replay ticks, %d baseline slots",
            zone_id,
            len(replay_data),
            len(baseline),
        )

    # ── Tick lifecycle ────────────────────────────────────────────────────

    def activate_tick(self, tick_id: int) -> None:
        """Mark *tick_id* as the current active tick.

        Idempotent: if *tick_id* is already in ``decision_history`` the
        call is logged and silently skipped.
        """
        if tick_id in self.decision_history:
            logger.warning(
                "Zone %d: activate_tick(%d) skipped — already in history",
                self.zone_id,
                tick_id,
            )
            return

        self.tick_state = "ACTIVE"
        self.active_tick_id = tick_id
        self.reported_decision = None
        self.reported_latency = 0.0
        logger.debug("Zone %d: tick %d activated", self.zone_id, tick_id)

    # ── Snapshot ──────────────────────────────────────────────────────────

    def get_snapshot(self, tick_id: int) -> dict[str, Any]:
        """Return a plain-dict snapshot for the current tick.

        Raises
        ------
        ValueError
            If the actor is not in ``ACTIVE`` state or *tick_id* does not
            match the currently active tick.
        """
        if self.tick_state != "ACTIVE":
            raise ValueError(
                f"Zone {self.zone_id}: get_snapshot called in state "
                f"{self.tick_state} (expected ACTIVE)"
            )
        if tick_id != self.active_tick_id:
            raise ValueError(
                f"Zone {self.zone_id}: get_snapshot for tick {tick_id} but "
                f"active tick is {self.active_tick_id}"
            )

        replay = self.replay_data.get(
            tick_id,
            {"demand": 0, "hour_of_day": 0, "day_of_week": 0},
        )
        demand: int = replay["demand"]
        hour_of_day: int = replay["hour_of_day"]
        day_of_week: int = replay["day_of_week"]

        bl = self.baseline.get(
            (hour_of_day, day_of_week),
            {"mean_demand": 0.0, "std_demand": 1.0},
        )

        return {
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "demand": demand,
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
            "baseline_mean": bl["mean_demand"],
            "baseline_std": bl["std_demand"],
        }

    # ── Decision reporting (async mode) ───────────────────────────────────

    def report_decision(
        self,
        tick_id: int,
        decision: str,
        task_latency_s: float,
    ) -> str:
        """Async mode: scoring task reports its result to the actor.

        Returns
        -------
        str
            ``"ACCEPTED"``, ``"LATE"``, or ``"DUPLICATE"``.
        """
        if self.tick_state != "ACTIVE" or tick_id != self.active_tick_id:
            self.late_count += 1
            logger.debug(
                "Zone %d: LATE report for tick %d (state=%s, active=%s)",
                self.zone_id,
                tick_id,
                self.tick_state,
                self.active_tick_id,
            )
            return "LATE"

        if self.reported_decision is not None:
            self.duplicate_count += 1
            logger.debug(
                "Zone %d: DUPLICATE report for tick %d",
                self.zone_id,
                tick_id,
            )
            return "DUPLICATE"

        self.reported_decision = decision
        self.reported_latency = task_latency_s
        logger.debug(
            "Zone %d: ACCEPTED decision '%s' for tick %d (latency=%.4fs)",
            self.zone_id,
            decision,
            tick_id,
            task_latency_s,
        )
        return "ACCEPTED"

    # ── Decision writing (blocking mode) ──────────────────────────────────

    def write_decision(
        self,
        tick_id: int,
        decision: str,
        used_fallback: bool,
        task_latency_s: float,
    ) -> None:
        """Blocking mode: driver writes the accepted decision.

        Idempotent by ``tick_id`` — duplicate writes increment the
        duplicate counter and return immediately.
        """
        if tick_id in self.decision_history:
            self.duplicate_count += 1
            logger.debug(
                "Zone %d: duplicate write_decision for tick %d",
                self.zone_id,
                tick_id,
            )
            return

        self.decision_history[tick_id] = {
            "decision": decision,
            "used_fallback": used_fallback,
            "task_latency_s": task_latency_s,
        }
        self.previous_decision = decision

        if used_fallback:
            self.fallback_count += 1

        logger.debug(
            "Zone %d: wrote decision '%s' for tick %d (fallback=%s)",
            self.zone_id,
            decision,
            tick_id,
            used_fallback,
        )

    # ── Tick finalization ─────────────────────────────────────────────────

    def close_tick(self, tick_id: int) -> dict[str, Any]:
        """Finalize the tick.

        If a decision was reported (async on-time), it is committed.
        Otherwise the previous decision is used as a fallback.

        Returns
        -------
        dict
            ``{status, decision}`` where *status* is ``"ON_TIME"``,
            ``"FALLBACK"``, ``"ALREADY_CLOSED"``, or ``"ERROR"``.
        """
        # Idempotent: if tick already finalized, return cached result
        if tick_id in self.decision_history:
            hist = self.decision_history[tick_id]
            return {
                "status": "ALREADY_CLOSED",
                "decision": hist["decision"],
                "zone_id": self.zone_id,
                "tick_id": tick_id,
                "task_latency_s": hist["task_latency_s"],
            }

        if self.tick_state != "ACTIVE" or tick_id != self.active_tick_id:
            logger.warning(
                "Zone %d: close_tick(%d) called in invalid state "
                "(state=%s, active=%s)",
                self.zone_id,
                tick_id,
                self.tick_state,
                self.active_tick_id,
            )
            return {
                "status": "ERROR",
                "decision": None,
                "zone_id": self.zone_id,
                "tick_id": tick_id,
            }

        # ── Async on-time path ────────────────────────────────────────────
        if self.reported_decision is not None:
            decision = self.reported_decision
            latency = self.reported_latency
            if tick_id not in self.decision_history:
                self.decision_history[tick_id] = {
                    "decision": decision,
                    "used_fallback": False,
                    "task_latency_s": latency,
                }
            self.previous_decision = decision
            self.tick_state = "CLOSED"
            return {
                "status": "ON_TIME",
                "decision": decision,
                "zone_id": self.zone_id,
                "tick_id": tick_id,
                "task_latency_s": latency,
            }

        # ── Fallback path ─────────────────────────────────────────────────
        fallback_decision = self.previous_decision

        self.decision_history[tick_id] = {
            "decision": fallback_decision,
            "used_fallback": True,
            "task_latency_s": 0.0,
        }
        self.previous_decision = fallback_decision
        self.fallback_count += 1
        self.tick_state = "CLOSED"

        logger.info(
            "Zone %d: FALLBACK for tick %d → '%s'",
            self.zone_id,
            tick_id,
            fallback_decision,
        )
        return {
            "status": "FALLBACK",
            "decision": fallback_decision,
            "zone_id": self.zone_id,
            "tick_id": tick_id,
            "task_latency_s": 0.0,
        }

    # ── Observability ─────────────────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        """Return current actor status as a plain dict."""
        return {
            "zone_id": self.zone_id,
            "tick_state": self.tick_state,
            "active_tick_id": self.active_tick_id,
            "reported_decision": self.reported_decision,
            "duplicate_count": self.duplicate_count,
            "late_count": self.late_count,
            "fallback_count": self.fallback_count,
        }

    def get_decision_history(self) -> dict[int, dict]:
        """Return the full decision history keyed by tick_id."""
        return dict(self.decision_history)

    def is_ready(self, tick_id: int) -> bool:
        """Check if this actor has a reported decision for the given tick."""
        return (
            self.tick_state == "ACTIVE"
            and self.active_tick_id == tick_id
            and self.reported_decision is not None
        )
