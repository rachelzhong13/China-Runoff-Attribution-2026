from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--basin-shapefile", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    return parser.parse_args()


def run(script: str, *arguments: str) -> None:
    print(f"start={script}", flush=True)
    subprocess.run([sys.executable, str(ROOT / script), *arguments], check=True)
    print(f"done={script}", flush=True)


def main() -> None:
    args = parse_args()
    run("validate_model_outputs.py")
    run("aggregate_attribution.py", "--basin-shapefile", str(args.basin_shapefile))
    run("plot_recomputed_results.py", "--china-shapefile", str(args.china_shapefile))


if __name__ == "__main__":
    main()
