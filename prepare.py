"""Data preprocessing for the TLC-backed per-zone recommendations capstone.

Reads raw TLC Green Taxi parquet files, validates adjacency, selects
active zones, builds reference baselines and replay tick tables, and
writes all prepared assets to disk.

Depends only on :mod:`config` (leaf dependency).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ─── Constants ────────────────────────────────────────────────────────────────

_PICKUP_COL = "lpep_pickup_datetime"
_ZONE_COL = "PULocationID"


# ─── Public API ───────────────────────────────────────────────────────────────


def validate_adjacent_months(ref_path: str | Path, replay_path: str | Path) -> tuple[int, int, int, int]:
    """Validate that *ref_path* and *replay_path* are from the same year
    and adjacent months.

    Both files must be TLC Green Taxi parquet files whose
    ``lpep_pickup_datetime`` column determines the year/month.

    Parameters
    ----------
    ref_path : str | Path
        Path to the reference-month parquet file.
    replay_path : str | Path
        Path to the replay-month parquet file.

    Returns
    -------
    tuple[int, int, int, int]
        ``(ref_year, ref_month, replay_year, replay_month)``.

    Raises
    ------
    ValueError
        If the files are not from the same year (with Dec→Jan wrap
        allowed) or the months are not adjacent.
    """
    ref_df = pd.read_parquet(ref_path, columns=[_PICKUP_COL])
    replay_df = pd.read_parquet(replay_path, columns=[_PICKUP_COL])

    ref_dt = pd.to_datetime(ref_df[_PICKUP_COL])
    replay_dt = pd.to_datetime(replay_df[_PICKUP_COL])

    ref_year = int(ref_dt.dt.year.mode().iloc[0])
    ref_month = int(ref_dt.dt.month.mode().iloc[0])
    replay_year = int(replay_dt.dt.year.mode().iloc[0])
    replay_month = int(replay_dt.dt.month.mode().iloc[0])

    # Check adjacency — allow Dec (12) → Jan (1) year wrap
    if ref_month == 12 and replay_month == 1:
        if replay_year != ref_year + 1:
            raise ValueError(
                f"December→January wrap requires replay_year == ref_year + 1, "
                f"got ref={ref_year}-{ref_month}, replay={replay_year}-{replay_month}"
            )
    else:
        if ref_year != replay_year:
            raise ValueError(
                f"Reference and replay must be from the same year "
                f"(unless Dec→Jan wrap). Got ref={ref_year}, replay={replay_year}"
            )
        if replay_month - ref_month != 1:
            raise ValueError(
                f"Months must be adjacent (replay = ref + 1). "
                f"Got ref_month={ref_month}, replay_month={replay_month}"
            )

    return ref_year, ref_month, replay_year, replay_month


def select_active_zones(
    ref_df: pd.DataFrame,
    top_n: int = 10,
    seed: int = 42,
) -> list[int]:
    """Select the *top_n* busiest pickup zones from the reference month.

    Zones are ranked by total pickup count (rows per ``PULocationID``).
    Ties are broken deterministically by zone ID (ascending) so the
    result is reproducible under a fixed *seed* and fixed input.

    Parameters
    ----------
    ref_df : pd.DataFrame
        Raw reference-month DataFrame with at least a ``PULocationID``
        column.
    top_n : int
        Number of zones to select.
    seed : int
        Random seed — reserved for future stochastic tie-breaking but
        currently unused because ties are broken by zone ID.

    Returns
    -------
    list[int]
        Sorted list of the *top_n* zone IDs.
    """
    counts = (
        ref_df.groupby(_ZONE_COL)
        .size()
        .reset_index(name="pickup_count")
        .sort_values(
            by=["pickup_count", _ZONE_COL],
            ascending=[False, True],
        )
    )
    top_zones = counts.head(top_n)[_ZONE_COL].tolist()
    return sorted(int(z) for z in top_zones)


def build_baseline(
    ref_df: pd.DataFrame,
    active_zones: list[int],
    tick_minutes: int = 15,
) -> pd.DataFrame:
    """Build a reference baseline table from the reference month.

    For each active zone the function:

    1. Filters to rows whose ``PULocationID`` is in *active_zones*.
    2. Floors ``lpep_pickup_datetime`` to *tick_minutes*-wide boundaries.
    3. Counts pickups per ``(zone_id, tick_boundary)``.
    4. Extracts ``hour_of_day`` and ``day_of_week`` from each tick
       boundary.
    5. Groups by ``(zone_id, hour_of_day, day_of_week)`` and computes
       the **mean** and **std** of the per-tick pickup counts.

    Parameters
    ----------
    ref_df : pd.DataFrame
        Raw reference-month DataFrame.
    active_zones : list[int]
        Zone IDs to include.
    tick_minutes : int
        Tick window width in minutes.

    Returns
    -------
    pd.DataFrame
        Columns: ``zone_id, hour_of_day, day_of_week, mean_demand,
        std_demand``.
    """
    df = ref_df[ref_df[_ZONE_COL].isin(active_zones)].copy()
    df[_PICKUP_COL] = pd.to_datetime(df[_PICKUP_COL])
    df["tick_boundary"] = df[_PICKUP_COL].dt.floor(f"{tick_minutes}min")

    # Step 1: count pickups per (zone, tick_boundary)
    tick_counts = (
        df.groupby([_ZONE_COL, "tick_boundary"])
        .size()
        .reset_index(name="pickup_count")
    )

    # Step 2: extract temporal features from the tick boundary
    tick_counts["hour_of_day"] = tick_counts["tick_boundary"].dt.hour
    tick_counts["day_of_week"] = tick_counts["tick_boundary"].dt.dayofweek

    # Step 3: aggregate stats per (zone, hour, dow)
    baseline = (
        tick_counts.groupby([_ZONE_COL, "hour_of_day", "day_of_week"])["pickup_count"]
        .agg(mean_demand="mean", std_demand="std")
        .reset_index()
        .rename(columns={_ZONE_COL: "zone_id"})
    )

    # Fill NaN std (groups with a single observation) with 0.0
    baseline["std_demand"] = baseline["std_demand"].fillna(0.0)

    return baseline


def build_replay_ticks(
    replay_df: pd.DataFrame,
    active_zones: list[int],
    tick_minutes: int = 15,
) -> pd.DataFrame:
    """Aggregate replay-month pickups into fixed tick windows.

    Parameters
    ----------
    replay_df : pd.DataFrame
        Raw replay-month DataFrame.
    active_zones : list[int]
        Zone IDs to include.
    tick_minutes : int
        Tick window width in minutes.

    Returns
    -------
    pd.DataFrame
        Columns: ``zone_id, tick_id, tick_start, demand, hour_of_day,
        day_of_week``.  ``tick_id`` is a sequential integer starting
        from 0, assigned based on sorted unique tick boundaries.
    """
    df = replay_df[replay_df[_ZONE_COL].isin(active_zones)].copy()
    df[_PICKUP_COL] = pd.to_datetime(df[_PICKUP_COL])
    df["tick_start"] = df[_PICKUP_COL].dt.floor(f"{tick_minutes}min")

    # Count pickups per (zone, tick_start)
    tick_counts = (
        df.groupby([_ZONE_COL, "tick_start"])
        .size()
        .reset_index(name="demand")
        .rename(columns={_ZONE_COL: "zone_id"})
    )

    # Build the full grid of (zone × tick) so missing pairs get demand=0
    all_ticks = sorted(tick_counts["tick_start"].unique())
    all_zones = sorted(active_zones)
    full_index = pd.MultiIndex.from_product(
        [all_zones, all_ticks], names=["zone_id", "tick_start"]
    )
    tick_counts = (
        tick_counts.set_index(["zone_id", "tick_start"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )

    # Assign sequential tick_id based on sorted unique tick boundaries
    tick_map = {ts: idx for idx, ts in enumerate(all_ticks)}
    tick_counts["tick_id"] = tick_counts["tick_start"].map(tick_map)

    # Extract temporal features
    tick_counts["hour_of_day"] = tick_counts["tick_start"].dt.hour
    tick_counts["day_of_week"] = tick_counts["tick_start"].dt.dayofweek

    # Reorder columns for clarity
    tick_counts = tick_counts[
        ["zone_id", "tick_id", "tick_start", "demand", "hour_of_day", "day_of_week"]
    ].sort_values(["tick_id", "zone_id"]).reset_index(drop=True)

    return tick_counts


def cross_check(
    replay_ticks_df: pd.DataFrame,
    replay_df: pd.DataFrame,
    active_zones: list[int],
    sample_tick_id: int = 0,
) -> None:
    """Verify that prepared replay counts match a direct pandas groupby.

    For the tick window identified by *sample_tick_id*, this function
    re-derives the pickup counts from the raw *replay_df* and asserts
    they equal the values in *replay_ticks_df*.

    Parameters
    ----------
    replay_ticks_df : pd.DataFrame
        Prepared replay tick table (output of :func:`build_replay_ticks`).
    replay_df : pd.DataFrame
        Raw replay-month DataFrame.
    active_zones : list[int]
        Zone IDs that were selected.
    sample_tick_id : int
        The ``tick_id`` to cross-check.

    Raises
    ------
    AssertionError
        If the prepared counts do not match the direct calculation.
    """
    # Get the tick_start for the sample tick_id
    sample_rows = replay_ticks_df[replay_ticks_df["tick_id"] == sample_tick_id]
    if sample_rows.empty:
        print(f"[cross_check] No rows for tick_id={sample_tick_id}, skipping.")
        return

    tick_start = sample_rows["tick_start"].iloc[0]

    # Determine tick boundaries
    tick_minutes = _infer_tick_minutes(replay_ticks_df)
    tick_end = tick_start + pd.Timedelta(minutes=tick_minutes)

    # Direct calculation from raw data
    raw = replay_df.copy()
    raw[_PICKUP_COL] = pd.to_datetime(raw[_PICKUP_COL])
    raw = raw[
        (raw[_ZONE_COL].isin(active_zones))
        & (raw[_PICKUP_COL] >= tick_start)
        & (raw[_PICKUP_COL] < tick_end)
    ]
    direct_counts = (
        raw.groupby(_ZONE_COL)
        .size()
        .reindex(active_zones, fill_value=0)
        .sort_index()
    )

    # Prepared counts for the same tick
    prepared_counts = (
        sample_rows.set_index("zone_id")["demand"]
        .reindex(active_zones, fill_value=0)
        .sort_index()
    )

    print(f"[cross_check] tick_id={sample_tick_id}  window=[{tick_start}, {tick_end})")
    print(f"  Direct counts:   {direct_counts.to_dict()}")
    print(f"  Prepared counts: {prepared_counts.to_dict()}")

    pd.testing.assert_series_equal(
        direct_counts.rename("demand"),
        prepared_counts.rename("demand"),
        check_names=False,
        check_dtype=False,
    )
    print("  ✓ Cross-check passed.")


def prepare_assets(
    ref_path: str | Path,
    replay_path: str | Path,
    output_dir: str | Path,
    top_n: int = 10,
    seed: int = 42,
    tick_minutes: int = 15,
) -> None:
    """Main entry point — validate, aggregate, and write all prepared assets.

    Orchestrates the full prepare pipeline:

    1. :func:`validate_adjacent_months` — confirm file adjacency.
    2. Load both parquet files.
    3. :func:`select_active_zones` — pick the busiest zones.
    4. :func:`build_baseline` — reference-month statistics.
    5. :func:`build_replay_ticks` — replay-month tick table.
    6. :func:`cross_check` — sanity-check one sample tick.
    7. Write ``active_zones.json``, ``baseline.parquet``,
       ``replay_ticks.parquet``, and ``meta.json`` to *output_dir*.

    Parameters
    ----------
    ref_path : str | Path
        Path to the reference-month parquet file.
    replay_path : str | Path
        Path to the replay-month parquet file.
    output_dir : str | Path
        Directory where prepared assets are written (created if needed).
    top_n : int
        Number of active zones to select.
    seed : int
        Random seed for deterministic zone selection.
    tick_minutes : int
        Tick window width in minutes.
    """
    ref_path = Path(ref_path)
    replay_path = Path(replay_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Validate adjacency ────────────────────────────────────────────
    print(f"[prepare] Validating adjacent months: {ref_path.name} → {replay_path.name}")
    ref_year, ref_month, replay_year, replay_month = validate_adjacent_months(
        ref_path, replay_path
    )
    print(f"  Reference: {ref_year}-{ref_month:02d}  Replay: {replay_year}-{replay_month:02d}")

    # ── 2. Load raw data ─────────────────────────────────────────────────
    print("[prepare] Loading parquet files …")
    ref_df = pd.read_parquet(ref_path)
    replay_df = pd.read_parquet(replay_path)

    # ── 3. Select active zones ───────────────────────────────────────────
    print(f"[prepare] Selecting top-{top_n} active zones (seed={seed}) …")
    active_zones = select_active_zones(ref_df, top_n=top_n, seed=seed)
    print(f"  Active zones: {active_zones}")

    # ── 4. Build baseline ────────────────────────────────────────────────
    print("[prepare] Building reference baseline …")
    baseline = build_baseline(ref_df, active_zones, tick_minutes=tick_minutes)
    print(f"  Baseline rows: {len(baseline)}")

    # ── 5. Build replay ticks ────────────────────────────────────────────
    print("[prepare] Building replay tick table …")
    replay_ticks = build_replay_ticks(replay_df, active_zones, tick_minutes=tick_minutes)
    n_ticks = replay_ticks["tick_id"].nunique()
    print(f"  Replay ticks: {len(replay_ticks)} rows, {n_ticks} unique ticks")

    # ── 6. Cross-check ───────────────────────────────────────────────────
    print("[prepare] Running cross-check on tick_id=0 …")
    cross_check(replay_ticks, replay_df, active_zones, sample_tick_id=0)

    # ── 7. Write assets ──────────────────────────────────────────────────
    zones_path = output_dir / "active_zones.json"
    baseline_path = output_dir / "baseline.parquet"
    replay_path_out = output_dir / "replay_ticks.parquet"
    meta_path = output_dir / "meta.json"

    zones_path.write_text(json.dumps(active_zones, indent=2))
    baseline.to_parquet(baseline_path, index=False)
    replay_ticks.to_parquet(replay_path_out, index=False)

    meta = {
        "ref_path": str(ref_path),
        "replay_path": str(replay_path),
        "top_n": top_n,
        "seed": seed,
        "tick_minutes": tick_minutes,
        "n_ticks": n_ticks,
        "n_zones": len(active_zones),
        "reference_year": ref_year,
        "reference_month": ref_month,
        "replay_year": replay_year,
        "replay_month": replay_month,
        "zone_ids": active_zones,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path.write_text(json.dumps(meta, indent=2))

    print(f"[prepare] Assets written to {output_dir}/")
    print(f"  • {zones_path.name}")
    print(f"  • {baseline_path.name}")
    print(f"  • {replay_path_out.name}")
    print(f"  • {meta_path.name}")


# ─── Private helpers ──────────────────────────────────────────────────────────


def _infer_tick_minutes(replay_ticks_df: pd.DataFrame) -> int:
    """Infer the tick width in minutes from consecutive tick boundaries.

    Falls back to 15 if the table has fewer than two distinct tick
    starts.
    """
    unique_starts = sorted(replay_ticks_df["tick_start"].unique())
    if len(unique_starts) < 2:
        return 15
    delta = pd.Timestamp(unique_starts[1]) - pd.Timestamp(unique_starts[0])
    return int(delta.total_seconds() // 60)
