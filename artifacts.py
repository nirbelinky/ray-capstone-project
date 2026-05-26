"""Artifact writers — persist run outputs to disk.

All writers are plain functions that accept in-memory data structures
and write them to the specified *output_dir*.  The convenience wrapper
:func:`write_all_artifacts` calls every writer in sequence.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import asdict
from typing import Any

from config import RunConfig
from models import TickMetrics, ZoneTickLatency

logger = logging.getLogger(__name__)


# ── Individual writers ────────────────────────────────────────────────────────


def write_run_config(config: RunConfig, output_dir: str) -> None:
    """Serialize *config* to ``run_config.json`` inside *output_dir*."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "run_config.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(config.to_dict(), fh, indent=2)
    logger.info("Wrote %s", path)


def write_metrics_csv(
    tick_metrics: list[TickMetrics],
    output_dir: str,
) -> None:
    """Write per-tick metrics to ``metrics.csv`` inside *output_dir*.

    Columns correspond to the fields of :class:`~models.TickMetrics`.
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "metrics.csv")

    fieldnames = [
        "tick_id",
        "tick_start_ts",
        "tick_end_ts",
        "tick_latency_s",
        "zones_total",
        "zones_completed",
        "zones_fallback",
        "mean_zone_latency_s",
        "max_zone_latency_s",
        "max_mean_ratio",
        "late_reports",
        "duplicate_reports",
    ]

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for tm in tick_metrics:
            writer.writerow(tm.to_dict())

    logger.info("Wrote %s (%d rows)", path, len(tick_metrics))


def write_latency_log(
    latencies: list[ZoneTickLatency],
    output_dir: str,
) -> None:
    """Write per-zone per-tick latency entries to ``latency_log.json``."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "latency_log.json")

    records: list[dict[str, Any]] = [asdict(entry) for entry in latencies]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)

    logger.info("Wrote %s (%d entries)", path, len(records))


def write_tick_summary(
    tick_metrics: list[TickMetrics],
    output_dir: str,
) -> None:
    """Compute and write an aggregated tick summary to ``tick_summary.json``.

    Summary fields
    --------------
    - ``total_ticks``
    - ``mean_tick_latency_s``
    - ``max_tick_latency_s``
    - ``total_fallbacks``
    - ``total_late_reports``
    - ``total_duplicate_reports``
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "tick_summary.json")

    total_ticks = len(tick_metrics)

    if total_ticks == 0:
        summary: dict[str, Any] = {
            "total_ticks": 0,
            "mean_tick_latency_s": 0.0,
            "max_tick_latency_s": 0.0,
            "total_fallbacks": 0,
            "total_late_reports": 0,
            "total_duplicate_reports": 0,
        }
    else:
        latencies = [tm.tick_latency_s for tm in tick_metrics]
        summary = {
            "total_ticks": total_ticks,
            "mean_tick_latency_s": sum(latencies) / total_ticks,
            "max_tick_latency_s": max(latencies),
            "total_fallbacks": sum(tm.zones_fallback for tm in tick_metrics),
            "total_late_reports": sum(tm.late_reports for tm in tick_metrics),
            "total_duplicate_reports": sum(
                tm.duplicate_reports for tm in tick_metrics
            ),
        }

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    logger.info("Wrote %s", path)


# ── Convenience wrapper ──────────────────────────────────────────────────────


def write_all_artifacts(
    config: RunConfig,
    tick_metrics: list[TickMetrics],
    latencies: list[ZoneTickLatency],
    output_dir: str,
) -> None:
    """Write every output artifact in one call.

    Delegates to :func:`write_run_config`, :func:`write_metrics_csv`,
    :func:`write_latency_log`, and :func:`write_tick_summary`.
    """
    write_run_config(config, output_dir)
    write_metrics_csv(tick_metrics, output_dir)
    write_latency_log(latencies, output_dir)
    write_tick_summary(tick_metrics, output_dir)
    logger.info("All artifacts written to %s", output_dir)
