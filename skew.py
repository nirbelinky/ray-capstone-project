"""Skew injection — deterministic slow-zone selection.

Provides :func:`select_slow_zones` which picks a reproducible subset of
active zones to receive artificial latency during scoring.
"""

from __future__ import annotations

import math
import random
from typing import Sequence


def select_slow_zones(
    active_zones: Sequence[int],
    fraction: float,
    seed: int = 42,
) -> set[int]:
    """Deterministically select a fraction of *active_zones* as slow.

    Parameters
    ----------
    active_zones : Sequence[int]
        Ordered collection of zone IDs currently participating in the
        simulation.
    fraction : float
        Proportion of zones to mark as slow (clamped so that at least
        one zone is selected when ``fraction > 0``).
    seed : int
        RNG seed for reproducibility.

    Returns
    -------
    set[int]
        Zone IDs designated as slow — returned as a ``set`` for O(1)
        membership checks.
    """
    if not active_zones or fraction <= 0:
        return set()

    k = max(1, math.ceil(len(active_zones) * fraction))
    rng = random.Random(seed)
    selected = rng.sample(list(active_zones), k)
    return set(selected)
