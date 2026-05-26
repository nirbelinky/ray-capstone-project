# TLC-backed Per-Zone Recommendations Under Skew

A distributed replay-based recommendation system built on **Ray** that watches
NYC TLC Green Taxi pickup demand zone-by-zone and produces per-tick
`NEED` / `OK` recommendations.  The project compares a **blocking baseline**
against an **asynchronous controller** under configurable skew injection to
demonstrate how actor-owned state, bounded concurrency, partial-readiness
policies, and idempotent writes behave in a distributed setting.

---

## Architecture

| Module | Role |
|--------|------|
| [`prepare.py`](prepare.py) | Data preprocessing — validates adjacent months, selects active zones, builds reference baselines and replay tick tables, writes prepared assets to disk |
| [`zone_actor.py`](zone_actor.py) | `ZoneActor` — a `@ray.remote` actor class (one per active zone) that owns mutable zone state, enforces tick lifecycle (`INACTIVE → ACTIVE → CLOSED`), and guarantees idempotent writes keyed by `(zone_id, tick_id)` |
| [`scoring.py`](scoring.py) | `score_zone` — a `@ray.remote` stateless task that receives a `ZoneSnapshot`, computes a z-score decision (`NEED` if z > 1.5, else `OK`), injects skew delay for slow zones, and optionally reports the result to the zone's actor (async mode) |
| [`driver.py`](driver.py) | Driver loops — `run_blocking` (waits for all zones), `run_async` (polls actor readiness with `ray.wait()` and bounded inflight), and `run_stress` (async with harsher skew parameters) |
| [`artifacts.py`](artifacts.py) | Output artifact writers — `run_config.json`, `metrics.csv`, `latency_log.json`, `tick_summary.json` |
| [`models.py`](models.py) | Shared data models — `ZoneSnapshot`, `ScoringResult`, `TickMetrics`, `ZoneTickLatency` |
| [`config.py`](config.py) | `RunConfig` dataclass with defaults, JSON serialization, and `stress_overrides()` factory |
| [`skew.py`](skew.py) | Skew injection — deterministic slow-zone selection and artificial sleep |
| [`main.py`](main.py) | CLI entry point with `prepare` and `run` subcommands |

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Python | 3.12 |
| Conda env | `22971-ray` (see [`environment.yml`](../environment.yml)) |
| Data | Two **adjacent monthly** NYC TLC Green Taxi parquet files (e.g. `2024-01` and `2024-02`) |

Download the parquet files from the
[NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

---

## Quick Start

```bash
# Activate environment
conda activate 22971-ray

# Step 1: Prepare assets
python main.py prepare \
  --reference-parquet path/to/green_tripdata_2024-01.parquet \
  --replay-parquet path/to/green_tripdata_2024-02.parquet \
  --output-dir prepared/

# Step 2: Run blocking baseline
python main.py run \
  --prepared-dir prepared/ \
  --output-dir output/blocking/ \
  --mode blocking

# Step 3: Run async controller
python main.py run \
  --prepared-dir prepared/ \
  --output-dir output/async/ \
  --mode async

# Step 4: Run stress test
python main.py run \
  --prepared-dir prepared/ \
  --output-dir output/stress/ \
  --mode stress
```

---

## Running on Docker Cluster

Use `ray job submit` to run on the multi-container Ray cluster defined in
[`Ray/1_cluster_setup/`](../1_cluster_setup/):

```bash
ray job submit \
  --working-dir . \
  --address http://127.0.0.1:8265 \
  -- python main.py run \
    --prepared-dir /workspace/prepared/ \
    --output-dir /workspace/output/blocking/ \
    --mode blocking \
    --ray-address auto
```

---

## CLI Reference

### `prepare` subcommand

Validates adjacent TLC parquet files, selects active zones, builds reference
baselines and replay tick tables, and writes prepared assets to disk.

| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `--reference-parquet` | Path | Yes | — | Path to the reference-month parquet file |
| `--replay-parquet` | Path | Yes | — | Path to the replay-month parquet file |
| `--output-dir` | Path | Yes | — | Directory for prepared assets |
| `--top-n` | int | No | `10` | Number of busiest pickup zones to select |
| `--seed` | int | No | `42` | Random seed for deterministic zone selection |
| `--tick-minutes` | int | No | `15` | Width of each replay tick window in minutes |

### `run` subcommand

Initializes Ray, creates per-zone actors, executes the driver loop in the
chosen mode, and writes output artifacts.

| Argument | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `--prepared-dir` | Path | Yes | — | Directory containing prepared assets |
| `--output-dir` | Path | Yes | — | Directory for output artifacts |
| `--mode` | str | Yes | — | Execution mode: `blocking`, `async`, or `stress` |
| `--max-inflight-zones` | int | No | `4` | Max concurrent scoring tasks (async/stress) |
| `--tick-timeout-s` | float | No | `2.0` | Wall-clock budget per tick before finalization |
| `--completion-fraction` | float | No | `0.75` | Fraction of zones required before early finalization |
| `--slow-zone-fraction` | float | No | `0.25` | Fraction of zones designated as slow |
| `--slow-zone-sleep-s` | float | No | `1.0` | Artificial sleep injected into slow-zone tasks |
| `--fallback-policy` | str | No | `previous_else_ok` | Policy for late zones: `previous_else_ok` or `always_previous` |
| `--ray-address` | str | No | `None` | Ray cluster address (`None` → local `ray.init()`) |

---

## Output Artifacts

Each run writes four files to the specified `--output-dir`:

| File | Description |
|------|-------------|
| `run_config.json` | Full `RunConfig` snapshot — every parameter used for the run |
| `metrics.csv` | Per-tick metrics: zone counts, latencies, skew ratio, fallback/late/duplicate counts, wall-clock time |
| `latency_log.json` | Per-zone per-tick latency entries including decision, fallback status, and late-arrival flag |
| `tick_summary.json` | Aggregated summary across all ticks: totals, percentiles, `NEED`/`OK` fractions |

---

## Three Required Demo Runs

### 1. Blocking Baseline

Scoring tasks return decision payloads to the controller; the controller waits
for **all** zone results before writing accepted decisions into actors.

**Expected behavior:** tick latency is dominated by the slowest zones — skew
hurts visibly because every tick must wait for every zone.

### 2. Async Controller

Scoring tasks report decisions directly to actors; the driver polls actor
readiness via `ray.wait()` and closes ticks once `completion_fraction` of
zones finish **or** `tick_timeout_s` expires.  Late zones receive a
deterministic fallback (`previous_else_ok` → re-use last accepted decision,
defaulting to `OK` on the first tick).

**Expected behavior:** lower sensitivity to stragglers — tick wall-clock is
bounded by the timeout rather than the slowest zone.

### 3. Stress Test

Runs the async controller with harsher skew parameters
(`slow_zone_fraction=0.5`, `slow_zone_sleep_s=3.0`, `tick_timeout_s=1.5`).

**Expected behavior:** blocking would degrade sharply under these conditions;
the async controller still progresses cleanly with explicit, observable
fallback usage.

---

## Configuration Defaults

| Parameter | Normal (blocking / async) | Stress Override |
|-----------|--------------------------|-----------------|
| `tick_minutes` | `15` | `15` |
| `max_inflight_zones` | `4` | `4` |
| `tick_timeout_s` | `2.0` | `1.5` |
| `completion_fraction` | `0.75` | `0.75` |
| `slow_zone_fraction` | `0.25` | `0.50` |
| `slow_zone_sleep_s` | `1.0` | `3.0` |
| `fallback_policy` | `previous_else_ok` | `previous_else_ok` |

---

## File Structure

```
Ray/4_ray_capstone_project/
├── main.py                  # CLI entry point (prepare / run)
├── config.py                # RunConfig dataclass + defaults
├── models.py                # Shared data models (ZoneSnapshot, ScoringResult, …)
├── prepare.py               # Data preprocessing: validate, aggregate, write assets
├── zone_actor.py            # ZoneActor @ray.remote class (one per zone)
├── scoring.py               # score_zone @ray.remote function + decision logic
├── driver.py                # Driver loops: run_blocking, run_async, run_stress
├── artifacts.py             # Output artifact writers
├── skew.py                  # Skew injection: slow-zone selection and sleep
├── design_doc.md            # Original design specification
├── architecture_plan.md     # Detailed architecture and implementation plan
└── README.md                # This file
```
