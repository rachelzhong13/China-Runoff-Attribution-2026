from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent
OUTPUTS = ROOT / "outputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    parser.add_argument("--basin-shapefile", type=Path, required=True)
    return parser.parse_args()


def run(script: str, *arguments: str) -> None:
    command = [sys.executable, str(ROOT / script), *arguments]
    print(f"start={' '.join(command[1:])}", flush=True)
    subprocess.run(command, check=True)
    print(f"done={script}", flush=True)


def main() -> None:
    args = parse_args()
    scenario_events = OUTPUTS / "model_events"
    common_events = OUTPUTS / "common_reference_events"
    scenario_aggregated = OUTPUTS / "aggregated"
    common_aggregated = OUTPUTS / "common_reference_aggregated"

    run(
        "run_all_model_events.py",
        "--raw-root", str(args.raw_root),
        "--china-shapefile", str(args.china_shapefile),
        "--workers", str(args.workers),
        "--output-dir", str(scenario_events),
    )
    run(
        "run_all_common_reference.py",
        "--raw-root", str(args.raw_root),
        "--china-shapefile", str(args.china_shapefile),
        "--workers", str(args.workers),
        "--output-dir", str(common_events),
    )
    run(
        "validate_model_outputs.py",
        "--input-dir", str(scenario_events),
        "--output-dir", str(OUTPUTS / "validation" / "scenario_specific"),
        "--expected-calibration-scheme", "scenario_specific_monthly",
    )
    run(
        "validate_model_outputs.py",
        "--input-dir", str(common_events),
        "--output-dir", str(OUTPUTS / "validation" / "common_reference"),
        "--expected-calibration-scheme", "factual_common_monthly",
    )
    run(
        "aggregate_attribution.py",
        "--input-dir", str(scenario_events),
        "--output-dir", str(scenario_aggregated),
        "--basin-shapefile", str(args.basin_shapefile),
    )
    run(
        "aggregate_attribution.py",
        "--input-dir", str(common_events),
        "--output-dir", str(common_aggregated),
        "--basin-shapefile", str(args.basin_shapefile),
    )
    run(
        "compare_calibration_schemes.py",
        "--scenario-dir", str(scenario_aggregated),
        "--common-dir", str(common_aggregated),
        "--output-dir", str(OUTPUTS / "calibration_comparison"),
    )
    figure_dir = OUTPUTS / "figures" / "common_reference"
    run(
        "plot_recomputed_results.py",
        "--data-dir", str(common_aggregated),
        "--output-dir", str(figure_dir),
        "--china-shapefile", str(args.china_shapefile),
    )
    run("plot_common_reference_workflow.py", "--output-dir", str(figure_dir))


if __name__ == "__main__":
    main()
