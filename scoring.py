"""Scoring task — Ray remote function for zone-level demand classification.

Implements a simple z-score threshold rule: if the current demand is more
than ``Z_THRESHOLD`` standard deviations above the baseline mean for the
same ``(hour_of_day, day_of_week)`` slot, the zone is classified as
``"NEED"``; otherwise ``"OK"``.

All inputs and outputs are plain dicts to avoid Ray serialization issues
with frozen dataclasses.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import ray

logger = logging.getLogger(__name__)

# ── Module-level constant ─────────────────────────────────────────────────────

Z_THRESHOLD: float = 1.5
"""Zones with a z-score above this value are classified as ``"NEED"``."""


# ── Shared z-score helper ─────────────────────────────────────────────────────


def _score_snapshot(snapshot_dict: dict[str, Any]) -> tuple[str, float]:
    """Compute z-score and decision from a snapshot dict.

    Returns (decision, z_score) where decision is "NEED" or "OK".
    """
    demand = snapshot_dict["demand"]
    baseline_mean = snapshot_dict["baseline_mean"]
    baseline_std = snapshot_dict["baseline_std"]
    z = (demand - baseline_mean) / max(baseline_std, 1.0)
    decision = "NEED" if z > Z_THRESHOLD else "OK"
    return decision, z


# ── Remote scoring function ──────────────────────────────────────────────────


@ray.remote
def score_zone(
    snapshot_dict: dict[str, Any],
    is_slow: bool,
    slow_sleep_s: float,
) -> dict[str, Any]:
    """Score a single zone for one tick.

    Parameters
    ----------
    snapshot_dict : dict
        Plain dict with keys: ``zone_id``, ``tick_id``, ``demand``,
        ``hour_of_day``, ``day_of_week``, ``baseline_mean``,
        ``baseline_std``.
    is_slow : bool
        Whether to inject artificial latency (skew simulation).
    slow_sleep_s : float
        Duration of the artificial sleep in seconds (only applied when
        *is_slow* is ``True``).

    Returns
    -------
    dict
        ``{zone_id, tick_id, decision, task_latency_s}``
    """
    t0 = time.perf_counter()

    # ── Skew injection ────────────────────────────────────────────────────
    if is_slow:
        logger.debug(
            "Zone %d tick %d: injecting %.2fs skew sleep",
            snapshot_dict["zone_id"],
            snapshot_dict["tick_id"],
            slow_sleep_s,
        )
        time.sleep(slow_sleep_s)

    # ── Z-score computation ───────────────────────────────────────────────
    decision, z = _score_snapshot(snapshot_dict)

    task_latency_s = time.perf_counter() - t0

    logger.debug(
        "Zone %d tick %d: demand=%d, mean=%.2f, std=%.2f, z=%.3f → %s "
        "(latency=%.4fs)",
        snapshot_dict["zone_id"],
        snapshot_dict["tick_id"],
        snapshot_dict["demand"],
        snapshot_dict["baseline_mean"],
        snapshot_dict["baseline_std"],
        z,
        decision,
        task_latency_s,
    )

    return {
        "zone_id": snapshot_dict["zone_id"],
        "tick_id": snapshot_dict["tick_id"],
        "decision": decision,
        "task_latency_s": task_latency_s,
    }


# ── Fire-and-forget scoring + actor reporting (async mode) ────────────────────


@ray.remote
def score_and_report(
    snapshot_dict: dict[str, Any],
    actor_handle,
    is_slow: bool,
    slow_sleep_s: float,
    duplicate_report_probability: float = 0.0,
) -> None:
    """Score a zone and report the result directly to the ZoneActor.

    This function is used by the async driver.  It performs the same
    z-score computation as :func:`score_zone` but instead of returning
    the result to the driver it calls
    ``actor_handle.report_decision.remote()`` directly.  The driver
    treats the returned ``ObjectRef`` as fire-and-forget.

    Parameters
    ----------
    snapshot_dict : dict
        Plain dict with keys: ``zone_id``, ``tick_id``, ``demand``,
        ``hour_of_day``, ``day_of_week``, ``baseline_mean``,
        ``baseline_std``.
    actor_handle : ray.actor.ActorHandle
        Handle to the :class:`ZoneActor` that owns this zone.
    is_slow : bool
        Whether to inject artificial latency (skew simulation).
    slow_sleep_s : float
        Duration of the artificial sleep in seconds (only applied when
        *is_slow* is ``True``).
    duplicate_report_probability : float
        Probability of sending an intentional duplicate report to the
        actor for demonstration purposes.
    """
    t0 = time.perf_counter()

    # ── Skew injection ────────────────────────────────────────────────────
    if is_slow:
        logger.debug(
            "Zone %d tick %d: injecting %.2fs skew sleep",
            snapshot_dict["zone_id"],
            snapshot_dict["tick_id"],
            slow_sleep_s,
        )
        time.sleep(slow_sleep_s)

    # ── Z-score computation ───────────────────────────────────────────────
    decision, z = _score_snapshot(snapshot_dict)

    task_latency_s = time.perf_counter() - t0

    tick_id = snapshot_dict["tick_id"]

    logger.debug(
        "Zone %d tick %d: demand=%d, mean=%.2f, std=%.2f, z=%.3f → %s "
        "(latency=%.4fs)",
        snapshot_dict["zone_id"],
        tick_id,
        snapshot_dict["demand"],
        snapshot_dict["baseline_mean"],
        snapshot_dict["baseline_std"],
        z,
        decision,
        task_latency_s,
    )

    # ── Report directly to actor ──────────────────────────────────────────
    status = ray.get(
        actor_handle.report_decision.remote(tick_id, decision, task_latency_s)
    )

    # ── Optional duplicate report for demonstration ───────────────────────
    if duplicate_report_probability > 0 and random.random() < duplicate_report_probability:
        dup_status = ray.get(
            actor_handle.report_decision.remote(tick_id, decision, task_latency_s)
        )
