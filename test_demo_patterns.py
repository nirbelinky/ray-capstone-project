"""Comprehensive integration tests for the Ray capstone project.

Uses **real NYC TLC Green Taxi parquet files** downloaded from the TLC
CloudFront CDN (Jan 2024 + Feb 2024).  The download utility
``download_tlc_data.py`` is invoked automatically if the files are missing.

Run with::

    cd Ray/4_ray_capstone_project
    conda run -n 22971-ray python -m pytest test_demo_patterns.py -v --tb=long

"""

from __future__ import annotations

import csv
import json
import math
import os
import shutil
import sys
import time
from pathlib import Path
from statistics import mean as _mean

import pandas as pd
import pytest
import ray

# ── Ensure project root is on sys.path so local imports resolve ──────────────
PROJECT_DIR = Path(__file__).resolve().parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from config import RunConfig
from models import TickMetrics, ZoneTickLatency, ZoneSnapshot, ScoringResult
from prepare import (
    validate_adjacent_months,
    select_active_zones,
    build_baseline,
    build_replay_ticks,
    cross_check,
    prepare_assets,
)
from scoring import score_zone, Z_THRESHOLD
from skew import select_slow_zones
from zone_actor import ZoneActor
from driver import run_blocking, run_async, run_stress
from artifacts import write_all_artifacts

# ── Data paths ───────────────────────────────────────────────────────────────

DATA_DIR = PROJECT_DIR / "data"
REF_PARQUET = DATA_DIR / "green_tripdata_2024-01.parquet"
REPLAY_PARQUET = DATA_DIR / "green_tripdata_2024-02.parquet"

# ── Test-specific constants ──────────────────────────────────────────────────

TOP_N_ZONES = 5          # small for speed
TICK_MINUTES = 60        # 1-hour ticks → fewer ticks for faster tests
MAX_TICKS = 10           # cap replay ticks for integration tests
SEED = 42
SLOW_ZONE_SLEEP_S = 0.5  # enough to trigger timeouts
TICK_TIMEOUT_S = 0.3      # shorter than slow sleep → async will timeout
COMPLETION_FRACTION = 0.6
SLOW_ZONE_FRACTION = 0.4  # 2 out of 5 zones slow


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="session", autouse=True)
def ensure_tlc_data():
    """Download TLC parquet files if they are not already on disk."""
    if REF_PARQUET.exists() and REPLAY_PARQUET.exists():
        return
    from download_tlc_data import download_files
    download_files()
    assert REF_PARQUET.exists(), f"Reference file not found: {REF_PARQUET}"
    assert REPLAY_PARQUET.exists(), f"Replay file not found: {REPLAY_PARQUET}"


@pytest.fixture(scope="session")
def ray_session():
    """Initialize Ray once for the entire test session."""
    if not ray.is_initialized():
        ray.init(num_cpus=4, log_to_driver=False)
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def ref_df():
    """Load the reference-month DataFrame (session-scoped for speed)."""
    return pd.read_parquet(REF_PARQUET)


@pytest.fixture(scope="session")
def replay_df():
    """Load the replay-month DataFrame (session-scoped for speed)."""
    return pd.read_parquet(REPLAY_PARQUET)


@pytest.fixture(scope="session")
def active_zones(ref_df):
    """Select active zones from the reference month."""
    return select_active_zones(ref_df, top_n=TOP_N_ZONES, seed=SEED)


@pytest.fixture(scope="session")
def baseline(ref_df, active_zones):
    """Build the baseline table from the reference month."""
    return build_baseline(ref_df, active_zones, tick_minutes=TICK_MINUTES)


@pytest.fixture(scope="session")
def replay_ticks(replay_df, active_zones):
    """Build the replay ticks table from the replay month."""
    return build_replay_ticks(replay_df, active_zones, tick_minutes=TICK_MINUTES)


@pytest.fixture(scope="session")
def prepared_dir(tmp_path_factory, active_zones, baseline, replay_ticks):
    """Write prepared assets to a temporary directory (session-scoped)."""
    d = tmp_path_factory.mktemp("prepared")

    # Write active_zones.json
    (d / "active_zones.json").write_text(json.dumps(active_zones, indent=2))

    # Write baseline.parquet
    baseline.to_parquet(d / "baseline.parquet", index=False)

    # Write replay_ticks.parquet — cap to MAX_TICKS for speed
    capped = replay_ticks[replay_ticks["tick_id"] < MAX_TICKS].copy()
    capped.to_parquet(d / "replay_ticks.parquet", index=False)

    # Write meta.json
    n_ticks = int(capped["tick_id"].nunique())
    meta = {
        "n_ticks": n_ticks,
        "n_zones": len(active_zones),
        "tick_minutes": TICK_MINUTES,
        "zone_ids": active_zones,
    }
    (d / "meta.json").write_text(json.dumps(meta, indent=2))

    return str(d)


def _make_config(mode: str, prepared_dir: str, output_dir: str, **overrides) -> RunConfig:
    """Build a RunConfig for tests with small/fast parameters.

    Any extra keyword arguments are forwarded to :class:`RunConfig`,
    overriding the defaults defined here.
    """
    defaults = dict(
        mode=mode,
        fallback_policy="always_previous",
        tick_minutes=TICK_MINUTES,
        max_inflight_zones=4,
        tick_timeout_s=TICK_TIMEOUT_S,
        completion_fraction=COMPLETION_FRACTION,
        slow_zone_fraction=SLOW_ZONE_FRACTION,
        slow_zone_sleep_s=SLOW_ZONE_SLEEP_S,
        ray_address=None,
        prepared_dir=prepared_dir,
        output_dir=output_dir,
    )
    defaults.update(overrides)
    return RunConfig(**defaults)


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 1: Preprocessing (prepare.py)
# ═══════════════════════════════════════════════════════════════════════════════


class TestPreprocessing:
    """Tests for data validation, zone selection, baseline, and replay ticks."""

    # 1. validate_adjacent_months — adjacent files pass
    def test_validate_adjacent_months(self):
        """Real Jan-2024 and Feb-2024 files pass adjacency validation."""
        ry, rm, py, pm = validate_adjacent_months(REF_PARQUET, REPLAY_PARQUET)
        assert ry == 2024
        assert rm == 1
        assert py == 2024
        assert pm == 2

    # 2. validate_non_adjacent_months_fails — same file twice → ValueError
    def test_validate_non_adjacent_months_fails(self):
        """Using the same file for both ref and replay raises ValueError."""
        with pytest.raises(ValueError, match="adjacent"):
            validate_adjacent_months(REF_PARQUET, REF_PARQUET)

    # 3. select_active_zones_deterministic — same input + seed → same zones
    def test_select_active_zones_deterministic(self, ref_df):
        """Same input + seed produces identical zone lists."""
        z1 = select_active_zones(ref_df, top_n=TOP_N_ZONES, seed=SEED)
        z2 = select_active_zones(ref_df, top_n=TOP_N_ZONES, seed=SEED)
        assert z1 == z2
        assert len(z1) == TOP_N_ZONES
        assert z1 == sorted(z1), "Zones should be sorted"

    # 4. build_baseline_has_expected_columns
    def test_build_baseline_has_expected_columns(self, baseline):
        """Baseline DataFrame has the required columns."""
        expected = {"zone_id", "hour_of_day", "day_of_week", "mean_demand", "std_demand"}
        assert expected.issubset(set(baseline.columns))
        assert len(baseline) > 0

    # 5. build_replay_ticks_has_expected_columns
    def test_build_replay_ticks_has_expected_columns(self, replay_ticks):
        """Replay ticks DataFrame has the required columns."""
        expected = {"zone_id", "tick_id", "demand", "hour_of_day", "day_of_week"}
        assert expected.issubset(set(replay_ticks.columns))
        assert len(replay_ticks) > 0

    # 6. cross_check_passes
    def test_cross_check_passes(self, replay_ticks, replay_df, active_zones):
        """Cross-check on tick_id=0 does not raise."""
        cross_check(replay_ticks, replay_df, active_zones, sample_tick_id=0)

    # 7. prepare_assets_creates_files
    def test_prepare_assets_creates_files(self, tmp_path):
        """prepare_assets writes all four expected asset files."""
        out = tmp_path / "prep_out"
        prepare_assets(
            ref_path=REF_PARQUET,
            replay_path=REPLAY_PARQUET,
            output_dir=out,
            top_n=TOP_N_ZONES,
            seed=SEED,
            tick_minutes=TICK_MINUTES,
        )
        assert (out / "active_zones.json").exists()
        assert (out / "baseline.parquet").exists()
        assert (out / "replay_ticks.parquet").exists()
        assert (out / "meta.json").exists()

        # Verify meta.json content
        meta = json.loads((out / "meta.json").read_text())
        assert meta["n_zones"] == TOP_N_ZONES
        assert meta["tick_minutes"] == TICK_MINUTES
        assert "n_ticks" in meta


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 2: Core Components (scoring.py, skew.py)
# ═══════════════════════════════════════════════════════════════════════════════


class TestCoreComponents:
    """Tests for scoring logic and skew injection."""

    # 8. score_zone_deterministic
    def test_score_zone_deterministic(self, ray_session):
        """Same snapshot → same decision (deterministic scoring)."""
        snap = {
            "zone_id": 1,
            "tick_id": 0,
            "demand": 10,
            "hour_of_day": 12,
            "day_of_week": 2,
            "baseline_mean": 5.0,
            "baseline_std": 2.0,
        }
        r1 = ray.get(score_zone.remote(snap, False, 0.0))
        r2 = ray.get(score_zone.remote(snap, False, 0.0))
        assert r1["decision"] == r2["decision"]
        assert r1["zone_id"] == r2["zone_id"]

    # 9. score_zone_need_decision — high demand → NEED
    def test_score_zone_need_decision(self, ray_session):
        """Demand far above baseline → NEED decision."""
        snap = {
            "zone_id": 1,
            "tick_id": 0,
            "demand": 100,
            "hour_of_day": 12,
            "day_of_week": 2,
            "baseline_mean": 5.0,
            "baseline_std": 2.0,
        }
        result = ray.get(score_zone.remote(snap, False, 0.0))
        assert result["decision"] == "NEED"

    # score_zone_zero_demand — zero demand → OK
    def test_zero_demand_tick(self, ray_session):
        """A zone with zero demand should produce 'OK' decision."""
        snapshot = {
            "zone_id": 1,
            "tick_id": 0,
            "demand": 0,
            "hour_of_day": 12,
            "day_of_week": 3,
            "baseline_mean": 10.0,
            "baseline_std": 5.0,
        }
        result = ray.get(score_zone.remote(snapshot, False, 0.0))
        assert result["decision"] == "OK", "Zero demand should produce OK"
        # z-score = (0 - 10) / 5 = -2.0, well below threshold of 1.5

    # 10. score_zone_ok_decision — normal demand → OK
    def test_score_zone_ok_decision(self, ray_session):
        """Demand at baseline → OK decision."""
        snap = {
            "zone_id": 1,
            "tick_id": 0,
            "demand": 5,
            "hour_of_day": 12,
            "day_of_week": 2,
            "baseline_mean": 5.0,
            "baseline_std": 2.0,
        }
        result = ray.get(score_zone.remote(snap, False, 0.0))
        assert result["decision"] == "OK"

    # 11. score_zone_with_skew_adds_latency
    def test_score_zone_with_skew_adds_latency(self, ray_session):
        """is_slow=True adds measurable sleep time."""
        snap = {
            "zone_id": 1,
            "tick_id": 0,
            "demand": 5,
            "hour_of_day": 12,
            "day_of_week": 2,
            "baseline_mean": 5.0,
            "baseline_std": 2.0,
        }
        sleep_s = 0.3
        result = ray.get(score_zone.remote(snap, True, sleep_s))
        assert result["task_latency_s"] >= sleep_s * 0.8  # allow small timing jitter

    # 12. select_slow_zones_deterministic
    def test_select_slow_zones_deterministic(self):
        """Same input → same slow zones."""
        zones = [10, 20, 30, 40, 50]
        s1 = select_slow_zones(zones, fraction=0.4, seed=42)
        s2 = select_slow_zones(zones, fraction=0.4, seed=42)
        assert s1 == s2

    # 13. select_slow_zones_fraction
    def test_select_slow_zones_fraction(self):
        """Correct number of slow zones selected."""
        zones = [10, 20, 30, 40, 50]
        slow = select_slow_zones(zones, fraction=0.4, seed=42)
        expected_k = max(1, math.ceil(len(zones) * 0.4))  # 2
        assert len(slow) == expected_k


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 3: ZoneActor Invariants
# ═══════════════════════════════════════════════════════════════════════════════


class TestZoneActorInvariants:
    """Tests for ZoneActor lifecycle, idempotency, and fault tolerance."""

    def _make_actor(self):
        """Create a minimal ZoneActor for testing."""
        replay_data = {
            0: {"demand": 10, "hour_of_day": 12, "day_of_week": 2},
            1: {"demand": 15, "hour_of_day": 13, "day_of_week": 2},
        }
        baseline = {
            (12, 2): {"mean_demand": 8.0, "std_demand": 2.0},
            (13, 2): {"mean_demand": 10.0, "std_demand": 3.0},
        }
        return ZoneActor.remote(zone_id=99, replay_data=replay_data, baseline=baseline)

    # 14. actor_tick_lifecycle — INACTIVE → ACTIVE → CLOSED
    def test_actor_tick_lifecycle(self, ray_session):
        """Actor transitions: INACTIVE → activate → ACTIVE → close → CLOSED."""
        actor = self._make_actor()

        # Initial state
        status = ray.get(actor.get_status.remote())
        assert status["tick_state"] == "INACTIVE"

        # Activate
        ray.get(actor.activate_tick.remote(0))
        status = ray.get(actor.get_status.remote())
        assert status["tick_state"] == "ACTIVE"
        assert status["active_tick_id"] == 0

        # Close
        result = ray.get(actor.close_tick.remote(0, "always_previous"))
        assert result["status"] in ("ON_TIME", "FALLBACK")
        status = ray.get(actor.get_status.remote())
        assert status["tick_state"] == "CLOSED"

    # 15. actor_idempotent_write — writing same tick_id twice doesn't duplicate
    def test_actor_idempotent_write(self, ray_session):
        """Writing the same tick_id twice increments duplicate counter."""
        actor = self._make_actor()
        ray.get(actor.activate_tick.remote(0))

        # First write
        ray.get(actor.write_decision.remote(0, "OK", False, 0.01))
        # Second write (duplicate)
        ray.get(actor.write_decision.remote(0, "OK", False, 0.01))

        status = ray.get(actor.get_status.remote())
        assert status["duplicate_count"] == 1

    # 16. actor_reject_late_report — report after close returns LATE
    def test_actor_reject_late_report(self, ray_session):
        """Report after close_tick returns LATE."""
        actor = self._make_actor()
        ray.get(actor.activate_tick.remote(0))
        ray.get(actor.close_tick.remote(0, "always_previous"))

        # Late report
        result = ray.get(actor.report_decision.remote(0, "NEED", 0.5))
        assert result == "LATE"

        status = ray.get(actor.get_status.remote())
        assert status["late_count"] == 1

    # 17. actor_reject_duplicate_report — second report returns DUPLICATE
    def test_actor_reject_duplicate_report(self, ray_session):
        """Second report for the same active tick returns DUPLICATE."""
        actor = self._make_actor()
        ray.get(actor.activate_tick.remote(0))

        r1 = ray.get(actor.report_decision.remote(0, "OK", 0.1))
        assert r1 == "ACCEPTED"

        r2 = ray.get(actor.report_decision.remote(0, "NEED", 0.2))
        assert r2 == "DUPLICATE"

        status = ray.get(actor.get_status.remote())
        assert status["duplicate_count"] == 1

    # 18. actor_fallback_on_close — close without report applies fallback
    def test_actor_fallback_on_close(self, ray_session):
        """Close without a reported decision applies fallback policy."""
        actor = self._make_actor()
        ray.get(actor.activate_tick.remote(0))

        # Close without reporting — should trigger fallback
        result = ray.get(actor.close_tick.remote(0, "always_previous"))
        assert result["status"] == "FALLBACK"
        assert result["decision"] == "OK"  # default previous is "OK"

        status = ray.get(actor.get_status.remote())
        assert status["fallback_count"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 4: Required Demo Patterns (Integration)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDemoPatterns:
    """Integration tests for blocking, async, and stress execution modes."""

    # 19. test_blocking_baseline
    def test_blocking_baseline(self, ray_session, prepared_dir, tmp_path):
        """Blocking mode: all ticks complete, zero fallbacks, skew visible."""
        output_dir = str(tmp_path / "blocking_out")
        config = _make_config("blocking", prepared_dir, output_dir)

        tick_metrics, latencies = run_blocking(config)

        # All ticks complete (zones_completed == zones_total)
        for tm in tick_metrics:
            assert tm.zones_completed == tm.zones_total, (
                f"tick {tm.tick_id}: {tm.zones_completed} != {tm.zones_total}"
            )

        # Zero fallbacks
        total_fallbacks = sum(tm.zones_fallback for tm in tick_metrics)
        assert total_fallbacks == 0, f"Expected 0 fallbacks, got {total_fallbacks}"

        # Tick latency dominated by slowest zone
        # With slow_zone_sleep_s=0.5, max_zone_latency should be >= 0.4
        for tm in tick_metrics:
            assert tm.max_zone_latency_s >= SLOW_ZONE_SLEEP_S * 0.7, (
                f"tick {tm.tick_id}: max_zone_latency_s={tm.max_zone_latency_s:.3f} "
                f"< {SLOW_ZONE_SLEEP_S * 0.7:.3f}"
            )

        # max_mean_ratio > 1 (skew is visible)
        any_skew_visible = any(tm.max_mean_ratio > 1.0 for tm in tick_metrics)
        assert any_skew_visible, "Expected max_mean_ratio > 1 in at least one tick"

    # 20. test_async_controller
    def test_async_controller(self, ray_session, prepared_dir, tmp_path):
        """Async mode: some fallbacks, tick latency bounded by timeout."""
        output_dir = str(tmp_path / "async_out")
        config = _make_config("async", prepared_dir, output_dir)

        tick_metrics, latencies = run_async(config)

        # Some ticks should have fallbacks (slow zones miss the deadline)
        total_fallbacks = sum(tm.zones_fallback for tm in tick_metrics)
        assert total_fallbacks > 0, (
            "Expected some fallbacks in async mode with skew"
        )

        # Tick latency bounded by tick_timeout_s (with tolerance for Ray overhead:
        # activate_tick, get_snapshot, close_tick RPCs for all actors)
        timeout_bound = TICK_TIMEOUT_S + 1.0  # generous overhead for actor RPCs
        for tm in tick_metrics:
            assert tm.tick_latency_s < timeout_bound, (
                f"tick {tm.tick_id}: latency {tm.tick_latency_s:.3f}s "
                f"> bound {timeout_bound:.3f}s"
            )

    # 21. test_stress_test
    def test_stress_test(self, ray_session, prepared_dir, tmp_path):
        """Stress mode: more aggressive skew, system still progresses."""
        output_dir = str(tmp_path / "stress_out")
        config = _make_config("stress", prepared_dir, output_dir)

        tick_metrics, latencies = run_stress(config)

        # System still progresses — all ticks complete
        assert len(tick_metrics) > 0, "No ticks completed in stress mode"

        # More fallbacks expected under stress
        total_fallbacks = sum(tm.zones_fallback for tm in tick_metrics)
        assert total_fallbacks > 0, "Expected fallbacks under stress"

        # Tick latency bounded by stress tick_timeout_s (1.5s) + overhead.
        # Under stress, slow_zone_sleep_s=3.0 exceeds tick_timeout_s=1.5,
        # so the driver's polling loop expires at ~1.5s but close_tick RPCs
        # and actor status queries add significant overhead on top.
        stress_timeout = 1.5 + 2.0  # stress_overrides sets tick_timeout_s=1.5
        for tm in tick_metrics:
            assert tm.tick_latency_s < stress_timeout, (
                f"tick {tm.tick_id}: latency {tm.tick_latency_s:.3f}s "
                f"> stress bound {stress_timeout:.3f}s"
            )

    # 22. test_blocking_vs_async_comparison
    def test_blocking_vs_async_comparison(self, ray_session, prepared_dir, tmp_path):
        """Blocking mean tick latency > async mean tick latency."""
        # Run blocking
        blocking_out = str(tmp_path / "cmp_blocking")
        blocking_cfg = _make_config("blocking", prepared_dir, blocking_out)
        blocking_metrics, blocking_lat = run_blocking(blocking_cfg)

        # Run async
        async_out = str(tmp_path / "cmp_async")
        async_cfg = _make_config("async", prepared_dir, async_out)
        async_metrics, async_lat = run_async(async_cfg)

        # Compute mean tick latencies
        blocking_mean = _mean([tm.tick_latency_s for tm in blocking_metrics])
        async_mean = _mean([tm.tick_latency_s for tm in async_metrics])

        # Blocking should be slower (dominated by stragglers)
        assert blocking_mean > async_mean, (
            f"Expected blocking ({blocking_mean:.3f}s) > async ({async_mean:.3f}s)"
        )

        # Blocking has 0 fallbacks
        blocking_fallbacks = sum(tm.zones_fallback for tm in blocking_metrics)
        assert blocking_fallbacks == 0

        # Async has > 0 fallbacks
        async_fallbacks = sum(tm.zones_fallback for tm in async_metrics)
        assert async_fallbacks > 0

        # Both produce decisions for all zones × all ticks
        n_ticks = len(blocking_metrics)
        n_zones = blocking_metrics[0].zones_total
        expected_decisions = n_ticks * n_zones
        assert len(blocking_lat) == expected_decisions, (
            f"Blocking: {len(blocking_lat)} != {expected_decisions}"
        )
        assert len(async_lat) == expected_decisions, (
            f"Async: {len(async_lat)} != {expected_decisions}"
        )


    # test_async_late_reports — late reports from slow tasks
    def test_async_late_reports(self, ray_session, prepared_dir, tmp_path):
        """Async mode should produce late reports when slow tasks exceed tick timeout."""
        output_dir = str(tmp_path / "late_out")
        config = _make_config(
            "async",
            prepared_dir,
            output_dir,
            slow_zone_fraction=0.5,      # half the zones are slow
            slow_zone_sleep_s=3.0,       # slow zones sleep 3s
            tick_timeout_s=0.5,          # but timeout is only 0.5s
            completion_fraction=0.5,     # close when 50% ready
        )
        tick_metrics, latencies = run_async(config)

        # Give late tasks time to report to actors
        time.sleep(4.0)

        # Late reports should have occurred since slow tasks (3s) exceed timeout (0.5s).
        # Late reports from tick N that arrive before tick N+1's status query
        # will be counted in tick N+1's metrics.
        total_late = sum(tm.late_reports for tm in tick_metrics)
        assert total_late > 0, "Expected late reports from slow tasks exceeding tick timeout"

    # test_async_duplicate_reports — duplicate reports with probability=1.0
    def test_async_duplicate_reports(self, ray_session, prepared_dir, tmp_path):
        """Async mode with duplicate_report_probability=1.0 should produce duplicate reports."""
        output_dir = str(tmp_path / "dup_out")
        config = _make_config(
            "async",
            prepared_dir,
            output_dir,
            duplicate_report_probability=1.0,
            tick_timeout_s=5.0,          # generous timeout
            completion_fraction=1.0,     # wait for all
        )
        tick_metrics, latencies = run_async(config)

        total_duplicates = sum(tm.duplicate_reports for tm in tick_metrics)
        assert total_duplicates > 0, "Expected duplicate reports with probability=1.0"


# ═══════════════════════════════════════════════════════════════════════════════
# Test Group 5: Artifacts
# ═══════════════════════════════════════════════════════════════════════════════


class TestArtifacts:
    """Tests for artifact writing and content validation."""

    @pytest.fixture()
    def run_artifacts(self, ray_session, prepared_dir, tmp_path):
        """Run blocking mode and write all artifacts."""
        output_dir = str(tmp_path / "artifact_out")
        config = _make_config("blocking", prepared_dir, output_dir)
        tick_metrics, latencies = run_blocking(config)
        write_all_artifacts(config, tick_metrics, latencies, output_dir)
        return output_dir, tick_metrics, latencies

    # 23. test_artifacts_created
    def test_artifacts_created(self, run_artifacts):
        """After a run, all expected artifact files exist."""
        output_dir, _, _ = run_artifacts
        assert os.path.exists(os.path.join(output_dir, "run_config.json"))
        assert os.path.exists(os.path.join(output_dir, "metrics.csv"))
        assert os.path.exists(os.path.join(output_dir, "latency_log.json"))
        assert os.path.exists(os.path.join(output_dir, "tick_summary.json"))

    # 24. test_metrics_csv_columns
    def test_metrics_csv_columns(self, run_artifacts):
        """metrics.csv has all expected columns."""
        output_dir, _, _ = run_artifacts
        csv_path = os.path.join(output_dir, "metrics.csv")
        with open(csv_path, "r") as fh:
            reader = csv.DictReader(fh)
            columns = set(reader.fieldnames or [])

        expected = {
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
        }
        assert expected.issubset(columns), f"Missing columns: {expected - columns}"

    # 25. test_tick_summary_fields
    def test_tick_summary_fields(self, run_artifacts):
        """tick_summary.json has expected aggregated fields."""
        output_dir, _, _ = run_artifacts
        summary_path = os.path.join(output_dir, "tick_summary.json")
        with open(summary_path, "r") as fh:
            summary = json.load(fh)

        expected_keys = {
            "total_ticks",
            "mean_tick_latency_s",
            "max_tick_latency_s",
            "total_fallbacks",
            "total_late_reports",
            "total_duplicate_reports",
        }
        assert expected_keys.issubset(set(summary.keys())), (
            f"Missing keys: {expected_keys - set(summary.keys())}"
        )
        assert summary["total_ticks"] > 0
        assert summary["mean_tick_latency_s"] > 0
        assert summary["total_fallbacks"] == 0  # blocking mode → no fallbacks
