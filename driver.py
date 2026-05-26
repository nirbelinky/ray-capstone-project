"""Driver loops — blocking, async, and stress execution modes.

Orchestrates the per-tick lifecycle:
  activate → snapshot → score → report/write → close → metrics

Imports from every runtime module except ``prepare.py``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from statistics import mean as _mean
from typing import Any

import pandas as pd
import ray

from config import RunConfig
from models import TickMetrics, ZoneTickLatency
from scoring import score_zone, score_and_report
from skew import select_slow_zones
from zone_actor import ZoneActor

logger = logging.getLogger(__name__)


# ── Common setup ──────────────────────────────────────────────────────────────


def _init_runtime(
    config: RunConfig,
) -> tuple[dict[int, Any], list[int], set[int], int]:
    """Load prepared assets and spin up one :class:`ZoneActor` per zone.

    Parameters
    ----------
    config : RunConfig
        Must have ``prepared_dir`` pointing at the output of the
        ``prepare`` command.

    Returns
    -------
    tuple
        ``(actors, active_zones, slow_zones, n_ticks)`` where

        * *actors* maps ``zone_id → ActorHandle``
        * *active_zones* is the ordered list of zone IDs
        * *slow_zones* is the set of zone IDs designated as slow
        * *n_ticks* is the total number of replay ticks
    """
    prepared = config.prepared_dir

    # ── Load assets ───────────────────────────────────────────────────────
    with open(os.path.join(prepared, "active_zones.json"), "r") as fh:
        active_zones: list[int] = json.load(fh)

    baseline_df = pd.read_parquet(os.path.join(prepared, "baseline.parquet"))
    replay_df = pd.read_parquet(os.path.join(prepared, "replay_ticks.parquet"))

    with open(os.path.join(prepared, "meta.json"), "r") as fh:
        meta: dict = json.load(fh)

    n_ticks: int = meta["n_ticks"]

    # ── Build per-zone data partitions ────────────────────────────────────
    actors: dict[int, Any] = {}

    for zone_id in active_zones:
        # replay_data: tick_id → {demand, hour_of_day, day_of_week}
        zone_replay = replay_df[replay_df["zone_id"] == zone_id]
        replay_data: dict[int, dict] = {}
        for _, row in zone_replay.iterrows():
            replay_data[int(row["tick_id"])] = {
                "demand": int(row["demand"]),
                "hour_of_day": int(row["hour_of_day"]),
                "day_of_week": int(row["day_of_week"]),
            }

        # baseline_data: (hour_of_day, day_of_week) → {mean_demand, std_demand}
        zone_baseline = baseline_df[baseline_df["zone_id"] == zone_id]
        baseline_data: dict[tuple[int, int], dict] = {}
        for _, row in zone_baseline.iterrows():
            key = (int(row["hour_of_day"]), int(row["day_of_week"]))
            baseline_data[key] = {
                "mean_demand": float(row["mean_demand"]),
                "std_demand": float(row["std_demand"]),
            }

        actors[zone_id] = ZoneActor.remote(zone_id, replay_data, baseline_data)

    # ── Slow-zone selection ───────────────────────────────────────────────
    slow_zones = select_slow_zones(active_zones, config.slow_zone_fraction)

    logger.info(
        "Runtime initialised: %d zones (%d slow), %d ticks",
        len(active_zones),
        len(slow_zones),
        n_ticks,
    )
    return actors, active_zones, slow_zones, n_ticks


# ── Blocking driver ──────────────────────────────────────────────────────────


def run_blocking(
    config: RunConfig,
) -> tuple[list[TickMetrics], list[ZoneTickLatency]]:
    """Blocking driver — waits for **all** scoring tasks every tick.

    Every zone completes before the tick is finalized, so there are
    never any fallbacks, late reports, or duplicates.
    """
    actors, active_zones, slow_zones, n_ticks = _init_runtime(config)
    all_tick_metrics: list[TickMetrics] = []
    all_latencies: list[ZoneTickLatency] = []

    for tick_id in range(n_ticks):
        tick_start = time.time()

        # Step D: Activate tick in all actors
        ray.get([actors[z].activate_tick.remote(tick_id) for z in active_zones])

        # Step D: Collect snapshots from all actors
        snapshots: list[dict] = ray.get(
            [actors[z].get_snapshot.remote(tick_id) for z in active_zones]
        )

        # Step E: Launch scoring tasks for all zones, wait for ALL
        score_refs = [
            score_zone.remote(
                snap, snap["zone_id"] in slow_zones, config.slow_zone_sleep_s
            )
            for snap in snapshots
        ]
        results: list[dict] = ray.get(score_refs)  # BLOCKING: wait for all

        # Step F: Write decisions to actors (driver decides)
        write_refs = []
        for result in results:
            zid = result["zone_id"]
            write_refs.append(
                actors[zid].write_decision.remote(
                    tick_id, result["decision"], False, result["task_latency_s"]
                )
            )
        ray.get(write_refs)  # Explicitly wait for writes to complete

        # Step G: Close ticks
        close_results = ray.get(
            [
                actors[z].close_tick.remote(tick_id, config.fallback_policy)
                for z in active_zones
            ]
        )

        tick_end = time.time()

        # Build metrics
        latencies_this_tick = [r["task_latency_s"] for r in results]
        mean_lat = _mean(latencies_this_tick) if latencies_this_tick else 0.0
        max_lat = max(latencies_this_tick) if latencies_this_tick else 0.0

        tick_metrics = TickMetrics(
            tick_id=tick_id,
            tick_start_ts=tick_start,
            tick_end_ts=tick_end,
            tick_latency_s=tick_end - tick_start,
            zones_total=len(active_zones),
            zones_completed=len(active_zones),  # blocking: all complete
            zones_fallback=0,
            mean_zone_latency_s=mean_lat,
            max_zone_latency_s=max_lat,
            max_mean_ratio=max_lat / max(mean_lat, 1e-9),
            late_reports=0,
            duplicate_reports=0,
        )
        all_tick_metrics.append(tick_metrics)

        for result in results:
            all_latencies.append(
                ZoneTickLatency(
                    zone_id=result["zone_id"],
                    tick_id=tick_id,
                    task_latency_s=result["task_latency_s"],
                    used_fallback=False,
                    decision=result["decision"],
                )
            )

        logger.info(
            "[blocking] tick %d/%d done in %.3fs",
            tick_id,
            n_ticks - 1,
            tick_metrics.tick_latency_s,
        )

    return all_tick_metrics, all_latencies


# ── Async driver ─────────────────────────────────────────────────────────────


def run_async(
    config: RunConfig,
) -> tuple[list[TickMetrics], list[ZoneTickLatency]]:
    """Async driver — actor-reporting / polling design.

    Scoring tasks report directly to :class:`ZoneActor` instances via
    :func:`score_and_report`.  The driver polls actors for readiness
    and closes the tick when enough actors have reported or the
    deadline expires.  Tasks that finish after ``close_tick`` produce
    late reports naturally.
    """
    actors, active_zones, slow_zones, n_ticks = _init_runtime(config)
    all_tick_metrics: list[TickMetrics] = []
    all_latencies: list[ZoneTickLatency] = []

    # Track cumulative late/duplicate counts for per-tick deltas
    prev_total_late = 0
    prev_total_dup = 0

    for tick_id in range(n_ticks):
        tick_start = time.time()

        # Step D: Activate tick
        ray.get([actors[z].activate_tick.remote(tick_id) for z in active_zones])

        # Step D: Collect snapshots
        snapshots: list[dict] = ray.get(
            [actors[z].get_snapshot.remote(tick_id) for z in active_zones]
        )

        # Step E: Launch scoring tasks with bounded concurrency
        # Tasks report directly to actors — driver treats refs as
        # fire-and-forget (does NOT ray.get them or cancel them).
        inflight_refs: list[ray.ObjectRef] = []
        remaining_snaps = list(snapshots)
        all_refs: list[ray.ObjectRef] = []

        while remaining_snaps:
            # Launch tasks up to the inflight limit
            while remaining_snaps and len(inflight_refs) < config.max_inflight_zones:
                snap = remaining_snaps.pop(0)
                zid = snap["zone_id"]
                ref = score_and_report.remote(
                    snap,
                    actors[zid],
                    zid in slow_zones,
                    config.slow_zone_sleep_s,
                    config.duplicate_report_probability,
                )
                inflight_refs.append(ref)
                all_refs.append(ref)

            # Wait for at least one to finish before launching more
            if remaining_snaps and inflight_refs:
                done, inflight_refs = ray.wait(
                    inflight_refs, num_returns=1, timeout=0.1
                )

        # Step F: Poll actors for readiness
        deadline = tick_start + config.tick_timeout_s
        min_complete = max(
            1, math.ceil(len(active_zones) * config.completion_fraction)
        )

        while time.time() < deadline:
            readiness = ray.get(
                [actors[z].is_ready.remote(tick_id) for z in active_zones]
            )
            completed_count = sum(readiness)
            if completed_count >= min_complete:
                break
            time.sleep(0.05)  # small sleep to avoid busy-polling

        # Step G: Close all ticks (actors apply fallback for unreported zones)
        # NOTE: Do NOT cancel all_refs — let them run and produce late reports
        close_results: list[dict] = ray.get(
            [
                actors[z].close_tick.remote(tick_id, config.fallback_policy)
                for z in active_zones
            ]
        )

        tick_end = time.time()

        # Build metrics from close results
        zones_completed = sum(
            1 for cr in close_results if cr.get("status") == "ON_TIME"
        )
        zones_fallback = sum(
            1 for cr in close_results if cr.get("status") == "FALLBACK"
        )
        completed_latencies = [
            cr["task_latency_s"]
            for cr in close_results
            if cr.get("status") == "ON_TIME" and cr.get("task_latency_s", 0.0) > 0
        ]

        # Get actor statuses for late/duplicate counts
        statuses: list[dict] = ray.get(
            [actors[z].get_status.remote() for z in active_zones]
        )
        total_late = sum(s["late_count"] for s in statuses)
        total_dup = sum(s["duplicate_count"] for s in statuses)

        # Per-tick deltas
        tick_late = total_late - prev_total_late
        tick_dup = total_dup - prev_total_dup
        prev_total_late = total_late
        prev_total_dup = total_dup

        mean_lat = _mean(completed_latencies) if completed_latencies else 0.0
        max_lat = max(completed_latencies) if completed_latencies else 0.0

        tick_metrics = TickMetrics(
            tick_id=tick_id,
            tick_start_ts=tick_start,
            tick_end_ts=tick_end,
            tick_latency_s=tick_end - tick_start,
            zones_total=len(active_zones),
            zones_completed=zones_completed,
            zones_fallback=zones_fallback,
            mean_zone_latency_s=mean_lat,
            max_zone_latency_s=max_lat,
            max_mean_ratio=max_lat / max(mean_lat, 1e-9),
            late_reports=tick_late,
            duplicate_reports=tick_dup,
        )
        all_tick_metrics.append(tick_metrics)

        # Build latency records from close results
        for i, z in enumerate(active_zones):
            cr = close_results[i]
            all_latencies.append(
                ZoneTickLatency(
                    zone_id=z,
                    tick_id=tick_id,
                    task_latency_s=cr.get("task_latency_s", 0.0),
                    used_fallback=(cr.get("status") == "FALLBACK"),
                    decision=cr.get("decision", "OK"),
                )
            )

        logger.info(
            "[async] tick %d/%d done in %.3fs "
            "(%d/%d on-time, %d fallback)",
            tick_id,
            n_ticks - 1,
            tick_metrics.tick_latency_s,
            zones_completed,
            len(active_zones),
            zones_fallback,
        )

    return all_tick_metrics, all_latencies


# ── Stress driver ────────────────────────────────────────────────────────────


def run_stress(
    config: RunConfig,
) -> tuple[list[TickMetrics], list[ZoneTickLatency]]:
    """Stress driver — async mode with harsher skew parameters.

    Applies :meth:`RunConfig.stress_overrides` then delegates to
    :func:`run_async`.
    """
    stress_config = RunConfig.stress_overrides(config)
    logger.info(
        "Stress overrides applied: slow_zone_fraction=%.2f, "
        "slow_zone_sleep_s=%.1f, tick_timeout_s=%.1f",
        stress_config.slow_zone_fraction,
        stress_config.slow_zone_sleep_s,
        stress_config.tick_timeout_s,
    )
    return run_async(stress_config)
