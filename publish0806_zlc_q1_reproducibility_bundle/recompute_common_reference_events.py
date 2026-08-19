from __future__ import annotations

import argparse
import csv
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import json
from pathlib import Path
import time

import numpy as np

from recompute_model_events import (
    MODELS,
    event_counts,
    find_files,
    china_cells,
    load_runoff_cache,
    read_time_segments,
    sensitivity_fieldnames,
    sensitivity_ratios,
)
from sri_core import (
    backward_running_mean,
    fit_monthly_calibration,
    transform_monthly_sri,
)


SCENARIOS = ("countclim-1901soc", "countclim-histsoc", "obsclim-histsoc")
CALIBRATION_SCENARIO = "obsclim-histsoc"
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "common_reference_events"

_CACHES: dict[str, np.memmap] = {}
_YEARS = None
_MONTHS = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=sorted(MODELS))
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-cells", type=int)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def output_fields() -> list[str]:
    return [
        "Grid_ID", "Flat_Index", "Lon", "Lat",
        "N_Drought_P1", "N_Drought_P2", "N_Flood_P1", "N_Flood_P2",
        "Drought_FR", "Flood_FR",
        "Drought_FR_zero_undefined", "Flood_FR_zero_undefined",
        *sensitivity_fieldnames(),
        "Failed_SRI_Months", "Missing_SRI_Days_1950_2019",
        "SRI_Mean_1950_2019", "SRI_SD_1950_2019", "Selected_Distributions",
    ]


def initialize_worker(
    cache_paths: dict[str, str],
    shape: tuple[int, int],
    years: np.ndarray,
    months: np.ndarray,
) -> None:
    global _CACHES, _YEARS, _MONTHS
    _CACHES = {
        scenario: np.memmap(path, mode="r", dtype="float32", shape=shape)
        for scenario, path in cache_paths.items()
    }
    _YEARS = years
    _MONTHS = months


def process_grid(task):
    column, flat_index, longitude, latitude = task
    running = {
        scenario: backward_running_mean(np.asarray(_CACHES[scenario][column, :]))
        for scenario in SCENARIOS
    }
    calibration = fit_monthly_calibration(
        running[CALIBRATION_SCENARIO], _YEARS, _MONTHS
    )
    study_period = (_YEARS >= 1950) & (_YEARS <= 2019)
    metric_names = [
        "N_Drought_P1", "N_Drought_P2", "N_Flood_P1", "N_Flood_P2",
        "Drought_FR", "Flood_FR", "Drought_FR_zero_undefined",
        "Flood_FR_zero_undefined", *sensitivity_fieldnames(),
    ]
    rows = {}
    negative_counts = {}
    for scenario in SCENARIOS:
        result = transform_monthly_sri(running[scenario], _MONTHS, calibration)
        valid_sri = result.values[study_period]
        finite = np.isfinite(valid_sri)
        missing_days = int(np.count_nonzero(~finite))
        terminal_missing_only = (
            missing_days == 1 and not finite[-1] and np.all(finite[:-1])
        )
        if result.failed_months == 0 and (missing_days == 0 or terminal_missing_only):
            metrics = event_counts(result.values, _YEARS)
            metrics.update(sensitivity_ratios(result.values, _YEARS))
        else:
            metrics = {name: np.nan for name in metric_names}
        rows[scenario] = {
            "Grid_ID": int(flat_index + 1),
            "Flat_Index": int(flat_index),
            "Lon": float(longitude),
            "Lat": float(latitude),
            **metrics,
            "Failed_SRI_Months": result.failed_months,
            "Missing_SRI_Days_1950_2019": missing_days,
            "SRI_Mean_1950_2019": (
                float(np.mean(valid_sri[finite])) if np.any(finite) else np.nan
            ),
            "SRI_SD_1950_2019": (
                float(np.std(valid_sri[finite])) if np.any(finite) else np.nan
            ),
            "Selected_Distributions": "|".join(
                fit.distribution if fit is not None else "failed"
                for fit in calibration
            ),
        }
        negative_counts[scenario] = result.negative_input_count
    selected = tuple(
        fit.distribution if fit is not None else None for fit in calibration
    )
    return rows, selected, sum(fit is None for fit in calibration), negative_counts


def main() -> None:
    args = parse_args()
    if args.workers < 1:
        raise ValueError("workers must be positive")
    started = time.perf_counter()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    scenario_files = {
        scenario: find_files(args.raw_root, scenario, args.model)
        for scenario in SCENARIOS
    }
    flat_indices, longitudes, latitudes = china_cells(
        scenario_files[CALIBRATION_SCENARIO][0], args.china_shapefile
    )
    if args.max_cells is not None:
        flat_indices = flat_indices[:args.max_cells]
        longitudes = longitudes[:args.max_cells]
        latitudes = latitudes[:args.max_cells]

    segment_data = {
        scenario: read_time_segments(files)
        for scenario, files in scenario_files.items()
    }
    calibration_time = segment_data[CALIBRATION_SCENARIO]
    years, months = calibration_time[1], calibration_time[2]
    for scenario, values in segment_data.items():
        if not np.array_equal(values[1], years) or not np.array_equal(values[2], months):
            raise RuntimeError(f"Time axis differs for {scenario} and {CALIBRATION_SCENARIO}")

    cache_paths = {
        scenario: args.output_dir / f"{scenario}__{args.model}.common.runoff.tmp"
        for scenario in SCENARIOS
    }
    cache_shape = None
    try:
        for scenario in SCENARIOS:
            cache = load_runoff_cache(
                segment_data[scenario][0], flat_indices, cache_paths[scenario]
            )
            if cache_shape is None:
                cache_shape = cache.shape
            elif cache.shape != cache_shape:
                raise RuntimeError("Runoff cache shapes differ among scenarios")
            del cache

        tasks = list(zip(
            range(len(flat_indices)), flat_indices, longitudes, latitudes
        ))
        output_paths = {
            scenario: args.output_dir / f"{scenario}__{args.model}.csv"
            for scenario in SCENARIOS
        }
        handles = {
            scenario: path.open("w", newline="", encoding="utf-8")
            for scenario, path in output_paths.items()
        }
        writers = {
            scenario: csv.DictWriter(handle, fieldnames=output_fields())
            for scenario, handle in handles.items()
        }
        for writer in writers.values():
            writer.writeheader()

        distribution_counts = Counter()
        failed_months = 0
        negative_counts = Counter()
        completed = 0
        try:
            with ProcessPoolExecutor(
                max_workers=args.workers,
                initializer=initialize_worker,
                initargs=(
                    {key: str(value) for key, value in cache_paths.items()},
                    cache_shape,
                    years,
                    months,
                ),
            ) as executor:
                for rows, selected, failed, negative in executor.map(
                    process_grid, tasks, chunksize=4
                ):
                    for scenario in SCENARIOS:
                        writers[scenario].writerow(rows[scenario])
                    distribution_counts.update(
                        name for name in selected if name is not None
                    )
                    failed_months += failed
                    negative_counts.update(negative)
                    completed += 1
                    if completed % 250 == 0:
                        print(f"processed={completed}/{len(tasks)}", flush=True)
        finally:
            for handle in handles.values():
                handle.close()

        elapsed = round(time.perf_counter() - started, 3)
        for scenario in SCENARIOS:
            values = segment_data[scenario]
            log = {
                "scenario": scenario,
                "model": args.model,
                "input_file_count": len(scenario_files[scenario]),
                "calendar": values[3],
                "processed_grid_count": len(flat_indices),
                "workers": args.workers,
                "time_start": values[4],
                "time_end": values[5],
                "processed_day_count": len(years),
                "calibration_scheme": "factual_common_monthly",
                "calibration_scenario": CALIBRATION_SCENARIO,
                "distribution_selection_counts": dict(distribution_counts),
                "invalid_model_grid_rule": (
                    "exclude_if_failed_month_or_nonterminal_missing_sri"
                ),
                "fit_scale_rule": "monthly_positive_median",
                "failed_month_fit_count": failed_months,
                "negative_30day_value_count": negative_counts[scenario],
                "elapsed_seconds": elapsed,
                "output": str(output_paths[scenario]),
            }
            log_path = args.output_dir / f"{scenario}__{args.model}.json"
            log_path.write_text(json.dumps(log, indent=2), encoding="utf-8")
        print(json.dumps({
            "model": args.model,
            "processed_grid_count": len(flat_indices),
            "elapsed_seconds": elapsed,
        }, indent=2))
    finally:
        for path in cache_paths.values():
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
