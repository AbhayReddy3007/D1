#!/usr/bin/env python3
"""
run_all.py — Cloud Run Jobs orchestrator
─────────────────────────────────────────
Two modes, deployed as two Cloud Run Jobs from the same image:

  JOB 1  (--mode patents, 10 parallel tasks):
    Each task discovers all drugs from GCS, takes its shard
    via CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT, processes
    them through the patent pipeline, and uploads to BQ per drug.

  JOB 2  (--mode forecast, 1 task):
    Runs after Job 1 is done. Executes forecast step3→4→5→6,
    then merges loe_table + forecasted_loe → Master_LOE.

  LOCAL  (--mode all):
    Runs everything sequentially for testing.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
BQ_SCRIPT    = BASE_DIR / "2bq.py"
FORECAST_DIR = BASE_DIR / "forecast-main"
MERGE_SCRIPT = BASE_DIR / "merge_to_master_loe.py"

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
PY     = sys.executable


def banner(text):
    print(f"\n{BOLD}{'═' * 64}")
    print(f"  {text}")
    print(f"{'═' * 64}{RESET}\n")


def run_step(label, cmd, cwd=None, dry_run=False):
    print(f"{YELLOW}▶ {label}{RESET}")
    print(f"  cmd: {' '.join(cmd)}")
    if cwd:
        print(f"  cwd: {cwd}")
    if dry_run:
        print(f"  {YELLOW}[DRY RUN] skipped{RESET}\n")
        return
    t0 = time.time()
    result = subprocess.run(cmd, cwd=cwd)
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"\n  {RED}✗ FAILED (exit {result.returncode}) after {elapsed:.1f}s{RESET}")
        print(f"  Pipeline halted at: {label}")
        sys.exit(result.returncode)
    print(f"  {GREEN}✓ Done in {elapsed:.1f}s{RESET}\n")


def discover_drugs():
    """Discover all drug folders from GCS."""
    from dotenv import load_dotenv
    load_dotenv()
    from google.cloud import storage
    from google.oauth2 import service_account

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    prefix      = os.getenv("GCS_PATENTS_PREFIX", "patents").rstrip("/") + "/"

    if not bucket_name:
        print(f"{RED}ERROR: GCS_BUCKET_NAME not set{RESET}")
        sys.exit(1)

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        creds  = service_account.Credentials.from_service_account_file(creds_path)
        client = storage.Client(credentials=creds)
    else:
        client = storage.Client()

    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    prefix_depth = len(prefix.split("/")) - 1
    folders = {}
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) > prefix_depth + 1:
            folder = parts[prefix_depth]
            folders[folder] = True

    drugs = sorted(folders.keys())
    print(f"[DISCOVERY] {len(drugs)} drug(s) in gs://{bucket_name}/{prefix}")
    return drugs


def get_my_shard(drugs):
    """Split drugs by CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT."""
    idx   = int(os.getenv("CLOUD_RUN_TASK_INDEX", "0"))
    count = int(os.getenv("CLOUD_RUN_TASK_COUNT", "1"))
    if count <= 1:
        return drugs
    shard = [d for i, d in enumerate(drugs) if i % count == idx]
    print(f"[SHARD] Task {idx + 1}/{count} → {len(shard)} drug(s): {shard}")
    return shard


def run_patents(dry_run=False):
    banner("PATENT PROCESSING (parallel)")
    drugs = discover_drugs()
    shard = get_my_shard(drugs)
    if not shard:
        print(f"{YELLOW}No drugs in this shard. Done.{RESET}")
        return
    cache_dir = BASE_DIR / "cog" / "results_cache"
    for i, drug in enumerate(shard, 1):
        run_step(
            f"[{i}/{len(shard)}] Patent pipeline: {drug}",
            [PY, "-m", "cog.main", drug],
            cwd=BASE_DIR, dry_run=dry_run,
        )
        run_step(
            f"[{i}/{len(shard)}] BQ upload: {drug}",
            [PY, str(BQ_SCRIPT), "--drug", drug, "--cache-dir", str(cache_dir)],
            dry_run=dry_run,
        )


def run_forecast(dry_run=False):
    banner("FORECASTING PIPELINE")
    for label, cmd in [
        ("Step 3 — IP Landscape + Layering + Filing Analysis",
         [PY, str(FORECAST_DIR / "step3.py"), "--upload"]),
        ("Step 4 — Innovator Filing Patterns",
         [PY, str(FORECAST_DIR / "step4.py")]),
        ("Step 5 — Business Strategy Review",
         [PY, str(FORECAST_DIR / "step5.py")]),
        ("Step 6 — Patent Forecast Generator",
         [PY, str(FORECAST_DIR / "step6.py")]),
    ]:
        run_step(label, cmd, dry_run=dry_run)


def run_merge(dry_run=False):
    banner("MERGE → Master_LOE")
    run_step("merge_to_master_loe.py", [PY, str(MERGE_SCRIPT)], dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="LOE Pipeline orchestrator")
    parser.add_argument(
        "--mode", choices=["patents", "forecast", "all"], default="all",
        help="patents = Job 1 (parallel), forecast = Job 2 (forecast+merge), all = sequential",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    t0 = time.time()
    idx   = os.getenv("CLOUD_RUN_TASK_INDEX", "0")
    count = os.getenv("CLOUD_RUN_TASK_COUNT", "1")
    banner(f"LOE PIPELINE — mode={args.mode} | task {int(idx)+1}/{count}")

    if args.mode in ("patents", "all"):
        run_patents(args.dry_run)
    if args.mode in ("forecast", "all"):
        run_forecast(args.dry_run)
        run_merge(args.dry_run)

    banner(f"DONE — {time.time() - t0:.1f}s ({(time.time() - t0) / 60:.1f} min)")


if __name__ == "__main__":
    main()
