"""CLI entry point for the TLC Zone Recommendation System.

Subcommands
-----------
``prepare``
    Validate adjacent TLC parquet files, select active zones, build
    reference baselines and replay tick tables, and write prepared
    assets to disk.

``run``
    Initialize Ray, spin up per-zone actors, execute the driver loop
    in the chosen mode (blocking / async / stress), and write output
    artifacts.
"""

from __future__ import annotations

import argparse
import logging
import os

import ray

from config import RunConfig
from prepare import prepare_assets
from driver import run_blocking, run_async, run_stress
from artifacts import write_all_artifacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="TLC Zone Recommendation System"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── prepare subcommand ────────────────────────────────────────────────
    prep = sub.add_parser(
        "prepare",
        help="Validate, aggregate, and write prepared assets from raw TLC parquet files.",
    )
    prep.add_argument("--reference-parquet", required=True)
    prep.add_argument("--replay-parquet", required=True)
    prep.add_argument("--output-dir", required=True)
    prep.add_argument("--top-n", type=int, default=10)
    prep.add_argument("--seed", type=int, default=42)
    prep.add_argument("--tick-minutes", type=int, default=15)

    # ── run subcommand ────────────────────────────────────────────────────
    run = sub.add_parser(
        "run",
        help="Execute the driver loop in blocking, async, or stress mode.",
    )
    run.add_argument("--prepared-dir", required=True)
    run.add_argument("--output-dir", required=True)
    run.add_argument(
        "--mode",
        choices=["blocking", "async", "stress"],
        required=True,
    )
    run.add_argument("--max-inflight-zones", type=int, default=4)
    run.add_argument("--tick-timeout-s", type=float, default=2.0)
    run.add_argument("--completion-fraction", type=float, default=0.75)
    run.add_argument("--slow-zone-fraction", type=float, default=0.25)
    run.add_argument("--slow-zone-sleep-s", type=float, default=1.0)
    run.add_argument("--duplicate-report-probability", type=float, default=0.0)
    run.add_argument("--ray-address", default=None)
    run.add_argument("--max-ticks", type=int, default=None,
                     help="Cap the number of ticks processed (for demo runs).")

    args = parser.parse_args()

    # ── Dispatch ──────────────────────────────────────────────────────────

    if args.command == "prepare":
        prepare_assets(
            ref_path=args.reference_parquet,
            replay_path=args.replay_parquet,
            output_dir=args.output_dir,
            top_n=args.top_n,
            seed=args.seed,
            tick_minutes=args.tick_minutes,
        )
        logger.info("Prepare complete.")
        return 0

    elif args.command == "run":
        for name in [
            "completion_fraction",
            "slow_zone_fraction",
            "duplicate_report_probability",
        ]:
            value = getattr(args, name)
            if not 0.0 <= value <= 1.0:
                parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")

        config = RunConfig(
            mode=args.mode,
            max_inflight_zones=args.max_inflight_zones,
            tick_timeout_s=args.tick_timeout_s,
            completion_fraction=args.completion_fraction,
            slow_zone_fraction=args.slow_zone_fraction,
            slow_zone_sleep_s=args.slow_zone_sleep_s,
            duplicate_report_probability=args.duplicate_report_probability,
            ray_address=args.ray_address,
            prepared_dir=args.prepared_dir,
            output_dir=args.output_dir,
            max_ticks=args.max_ticks,
        )

        os.makedirs(config.output_dir, exist_ok=True)

        # Init Ray
        if not ray.is_initialized():
            if config.ray_address:
                ray.init(address=config.ray_address)
            else:
                ray.init()

        try:
            if config.mode == "blocking":
                tick_metrics, latencies, decisions = run_blocking(config)
            elif config.mode == "async":
                tick_metrics, latencies, decisions = run_async(config)
            elif config.mode == "stress":
                tick_metrics, latencies, decisions = run_stress(config)
            else:
                logger.error("Unknown mode: %s", config.mode)
                return 1

            write_all_artifacts(config, tick_metrics, latencies, config.output_dir, decisions=decisions)
            logger.info("Run complete. Artifacts written to %s", config.output_dir)
        finally:
            ray.shutdown()

        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
