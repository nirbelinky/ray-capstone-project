"""Shared data models for the TLC-backed per-zone recommendations capstone.

Leaf module — imports nothing from the project.
Defines type aliases and structured data models. At runtime, ZoneActor
and scoring functions exchange plain dicts for simplicity; these
dataclasses are used for driver-level output aggregation and artifact
serialization.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal

# ── Type aliases ──────────────────────────────────────────────────────────────

Decision = Literal["NEED", "OK"]
FallbackPolicy = Literal["always_previous"]
TickState = Literal["INACTIVE", "ACTIVE", "CLOSED"]


# ── Immutable value objects ───────────────────────────────────────────────────


@dataclass(frozen=True)
class ZoneSnapshot:
    """Immutable snapshot sent from a ZoneActor to a ``score_zone`` task.

    Contains everything the scoring function needs to produce a
    deterministic decision for one zone in one tick.
    """

    zone_id: int
    tick_id: int
    demand: int
    hour_of_day: int
    day_of_week: int
    baseline_mean: float
    baseline_std: float
    is_slow: bool


@dataclass(frozen=True)
class ScoringResult:
    """Output of a ``score_zone`` task — returned to the driver or reported
    to the owning :class:`ZoneActor`.

    Frozen so it can be safely stored in the Ray object store and
    referenced by multiple consumers.
    """

    zone_id: int
    tick_id: int
    decision: Decision
    task_latency_s: float


@dataclass(frozen=True)
class ZoneTickLatency:
    """Per-zone, per-tick latency entry written to ``latency_log.json``.

    Frozen because each entry is an immutable fact once the tick is
    finalized.
    """

    zone_id: int
    tick_id: int
    task_latency_s: float
    used_fallback: bool
    decision: Decision


# ── Mutable aggregate ────────────────────────────────────────────────────────


@dataclass
class TickMetrics:
    """Per-tick metrics collected by the driver loop.

    *Not* frozen — fields are populated incrementally as the tick
    progresses through snapshot collection, scoring, and finalization.
    """

    tick_id: int
    tick_start_ts: float
    tick_end_ts: float
    tick_latency_s: float
    zones_total: int
    zones_completed: int
    zones_fallback: int
    mean_zone_latency_s: float
    max_zone_latency_s: float
    max_mean_ratio: float
    late_reports: int
    duplicate_reports: int

    # ── Convenience helpers ───────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Serialize to a plain dict (e.g. for CSV / JSON writing)."""
        return asdict(self)
