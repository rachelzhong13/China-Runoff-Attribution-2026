from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys

from recompute_model_events import MODELS


SCENARIOS = ("countclim-1901soc", "countclim-histsoc", "obsclim-histsoc")
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "common_reference_events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def complete(model: str, output_dir: Path) -> bool:
    for scenario in SCENARIOS:
        stem = f"{scenario}__{model}"
        csv_path = output_dir / f"{stem}.csv"
        log_path = output_dir / f"{stem}.json"
        if not csv_path.exists() or not log_path.exists():
            return False
        log = json.loads(log_path.read_text(encoding="utf-8"))
        if log.get("processed_grid_count") != 3823:
            return False
        if log.get("calibration_scheme") != "factual_common_monthly":
            return False
        with csv_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            if (
                "Flood_FR_T05_G10" not in header
                or "Flood_FR_T10_G30" not in header
                or sum(1 for _ in reader) != 3823
            ):
                return False
    return True


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for model in sorted(MODELS):
        if complete(model, args.output_dir):
            print(f"skip={model}", flush=True)
            continue
        print(f"start={model}", flush=True)
        subprocess.run([
            sys.executable,
            str(ROOT / "recompute_common_reference_events.py"),
            "--model", model,
            "--raw-root", str(args.raw_root),
            "--china-shapefile", str(args.china_shapefile),
            "--workers", str(args.workers),
            "--output-dir", str(args.output_dir),
        ], check=True)
        print(f"done={model}", flush=True)


if __name__ == "__main__":
    main()
