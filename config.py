"""Runtime configuration for the TLC-backed per-zone recommendations capstone.

Leaf module — imports nothing from the project.
Provides :class:`RunConfig` with sensible defaults, JSON serialization,
and a ``stress_overrides`` class method for the stress-test mode.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import json
from pathlib import Path


@dataclass
class RunConfig:
    """Full runtime configuration — serialized to ``run_config.json``.

    Parameters
    ----------
    mode : str
        Execution mode: ``"blocking"``, ``"async"``, or ``"stress"``.
    fallback_policy : str
        Policy applied when a zone misses its tick deadline.
        ``"previous_else_ok"`` re-uses the last accepted decision
        (defaulting to ``"OK"`` on the very first tick).
        ``"always_previous"`` behaves identically but is stricter
        on subsequent ticks.
    tick_minutes : int
        Width of each replay tick window in minutes.
    max_inflight_zones : int
        Maximum concurrent scoring tasks in async mode.
    tick_timeout_s : float
        Wall-clock budget per tick before the driver finalizes.
    completion_fraction : float
        Fraction of zones that must complete before the driver may
        finalize a tick early (async mode only).
    slow_zone_fraction : float
        Fraction of active zones designated as *slow* for skew injection.
    slow_zone_sleep_s : float
        Artificial sleep injected into scoring tasks for slow zones.
    duplicate_report_probability : float
        Probability that a scoring task sends a duplicate report to the
        actor (async mode only).  ``0.0`` disables duplicates.
    ray_address : str | None
        Ray cluster address (``None`` → local ``ray.init()``).
    prepared_dir : str
        Path to the directory containing prepared assets.
    output_dir : str
        Path to the directory where run artifacts are written.
    """

    mode: str
    fallback_policy: str = "always_previous"
    tick_minutes: int = 15
    max_inflight_zones: int = 4
    tick_timeout_s: float = 2.0
    completion_fraction: float = 0.75
    slow_zone_fraction: float = 0.25
    slow_zone_sleep_s: float = 1.0
    duplicate_report_probability: float = 0.0
    ray_address: str | None = None
    prepared_dir: str = ""
    output_dir: str = ""

    # ── Serialization ─────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Return a plain ``dict`` suitable for JSON serialization."""
        return asdict(self)

    def to_json(self, path: Path) -> None:
        """Write the config as pretty-printed JSON to *path*."""
        path.write_text(json.dumps(self.to_dict(), indent=2))

    # ── Factory helpers ───────────────────────────────────────────────────

    @classmethod
    def stress_overrides(cls, base: RunConfig | None = None) -> RunConfig:
        """Return a :class:`RunConfig` with harsher skew parameters.

        If *base* is provided, non-stress fields are copied from it;
        otherwise a fresh config with ``mode="stress"`` and default
        values is returned.

        Overridden fields
        -----------------
        * ``slow_zone_fraction`` → ``0.5``  (half the zones are slow)
        * ``slow_zone_sleep_s``  → ``3.0``  (3× the normal delay)
        * ``tick_timeout_s``     → ``1.5``  (tighter deadline)
        """
        if base is None:
            return cls(
                mode="stress",
                slow_zone_fraction=0.5,
                slow_zone_sleep_s=3.0,
                tick_timeout_s=1.5,
            )

        return cls(
            mode="stress",
            fallback_policy=base.fallback_policy,
            tick_minutes=base.tick_minutes,
            max_inflight_zones=base.max_inflight_zones,
            tick_timeout_s=1.5,
            completion_fraction=base.completion_fraction,
            slow_zone_fraction=0.5,
            slow_zone_sleep_s=3.0,
            duplicate_report_probability=base.duplicate_report_probability,
            ray_address=base.ray_address,
            prepared_dir=base.prepared_dir,
            output_dir=base.output_dir,
        )
