"""Download NYC TLC Green Taxi parquet files for the Ray capstone project.

Downloads two adjacent monthly Green Taxi parquet files from the NYC TLC
CloudFront CDN.  The files are saved to a ``data/`` subdirectory next to
this script.

Usage
-----
    python download_tlc_data.py

The script is idempotent — existing files are skipped.
"""

from __future__ import annotations

import os
import ssl
import sys
import urllib.request

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "green_tripdata_{year_month}.parquet"
)

# Adjacent months for the capstone: reference = Jan 2024, replay = Feb 2024
FILES = [
    {"year_month": "2024-01", "local_name": "green_tripdata_2024-01.parquet"},
    {"year_month": "2024-02", "local_name": "green_tripdata_2024-02.parquet"},
]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _progress_hook(block_num: int, block_size: int, total_size: int) -> None:
    """Print a simple progress bar to stdout."""
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(100, downloaded * 100 // total_size)
        bar = "#" * (pct // 2) + "-" * (50 - pct // 2)
        sys.stdout.write(f"\r  [{bar}] {pct:3d}%")
        sys.stdout.flush()
        if downloaded >= total_size:
            sys.stdout.write("\n")
    else:
        sys.stdout.write(f"\r  {downloaded:,} bytes downloaded")
        sys.stdout.flush()


def download_files(
    files: list[dict] | None = None,
    data_dir: str | None = None,
) -> list[str]:
    """Download TLC parquet files and return their local paths.

    Parameters
    ----------
    files : list[dict] | None
        Override the default file manifest.  Each dict must have
        ``year_month`` and ``local_name`` keys.
    data_dir : str | None
        Override the default data directory.

    Returns
    -------
    list[str]
        Absolute paths to the downloaded files, in manifest order.
    """
    if files is None:
        files = FILES
    if data_dir is None:
        data_dir = DATA_DIR

    os.makedirs(data_dir, exist_ok=True)

    # Try default SSL context first; fall back to unverified if cert
    # verification fails (common behind corporate proxies).
    try:
        ctx = ssl.create_default_context()
        # Quick connectivity test
        urllib.request.urlopen("https://d37ci6vzurychx.cloudfront.net/", context=ctx, timeout=5)
    except (ssl.SSLCertVerificationError, Exception):
        ctx = ssl._create_unverified_context()

    https_handler = urllib.request.HTTPSHandler(context=ctx)
    opener = urllib.request.build_opener(https_handler)
    urllib.request.install_opener(opener)

    paths: list[str] = []
    for entry in files:
        url = BASE_URL.format(year_month=entry["year_month"])
        dest = os.path.join(data_dir, entry["local_name"])
        paths.append(dest)

        if os.path.exists(dest):
            size_mb = os.path.getsize(dest) / (1024 * 1024)
            print(f"[download] SKIP {entry['local_name']} ({size_mb:.1f} MB already on disk)")
            continue

        print(f"[download] GET  {url}")
        urllib.request.urlretrieve(
            url,
            filename=dest,
            reporthook=_progress_hook,
        )
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"[download] Saved {entry['local_name']} ({size_mb:.1f} MB)")

    return paths


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    downloaded = download_files()
    print(f"\nAll files ready in {DATA_DIR}/")
    for p in downloaded:
        print(f"  • {os.path.basename(p)}")
