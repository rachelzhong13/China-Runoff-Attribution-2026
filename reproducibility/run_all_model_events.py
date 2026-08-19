from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys

SCENARIOS = ["countclim-1901soc", "countclim-histsoc", "obsclim-histsoc"]
MODELS = [
    "h08", "hydropy", "jules-w2", "lpjml5-7-10-fire", "miroc-integ-land",
    "watergap2-2e", "web-dhm-sg",
]
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "model_events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def complete(stem: str, output_dir: Path) -> bool:
    csv_path = output_dir / f"{stem}.csv"
    log_path = output_dir / f"{stem}.json"
    if not csv_path.exists() or not log_path.exists():
        return False
    log = json.loads(log_path.read_text(encoding="utf-8"))
    if log.get("processed_grid_count") != 3823:
        return False
    if log.get("invalid_model_grid_rule") != "exclude_if_failed_month_or_nonterminal_missing_sri":
        return False
    if log.get("fit_scale_rule") != "monthly_positive_median":
        return False
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        return (
            "Drought_FR_T05_D10" in header
            and "Flood_FR_T05_G10" in header
            and "Flood_FR_T10_G30" in header
            and "Flood_RR_S1990" in header
            and sum(1 for _ in reader) == 3823
        )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for scenario in SCENARIOS:
        for model in MODELS:
            stem = f"{scenario}__{model}"
            if complete(stem, args.output_dir):
                print(f"skip={stem}", flush=True)
                continue
            print(f"start={stem}", flush=True)
            subprocess.run(
                [
                    sys.executable, str(ROOT / "recompute_model_events.py"),
                    "--scenario", scenario, "--model", model,
                    "--raw-root", str(args.raw_root),
                    "--china-shapefile", str(args.china_shapefile),
                    "--workers", str(args.workers), "--output-dir", str(args.output_dir),
                ], check=True,
            )
            print(f"done={stem}", flush=True)


if __name__ == "__main__":
    main()



