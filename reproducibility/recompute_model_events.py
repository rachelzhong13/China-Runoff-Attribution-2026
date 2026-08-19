from __future__ import annotations

import argparse
import csv
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta
import json
from pathlib import Path
import time

import geopandas as gpd
import numpy as np
from netCDF4 import Dataset, num2date

from sri_core import (
    backward_running_mean,
    corrected_frequency_ratio,
    corrected_rate_ratio,
    count_drought_events,
    count_flood_events,
    fit_monthly_sri,
    undefined_zero_frequency_ratio,
)

SCENARIOS = {"countclim-1901soc", "countclim-histsoc", "obsclim-histsoc"}
MODELS = {
    "h08", "hydropy", "jules-w2", "lpjml5-7-10-fire", "miroc-integ-land",
    "watergap2-2e", "web-dhm-sg",
}

_CACHE = None
_YEARS = None
_MONTHS = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    parser.add_argument("--model", required=True, choices=sorted(MODELS))
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    parser.add_argument(
        "--output-dir", type=Path,
        default=Path(__file__).resolve().parent / "outputs" / "model_events",
    )
    parser.add_argument("--max-cells", type=int)
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def find_files(raw_root: Path, scenario: str, model: str) -> list[Path]:
    files = sorted((raw_root / scenario / model).glob("*.nc"))
    if len(files) != 12:
        raise RuntimeError(f"Expected 12 NetCDF files, found {len(files)}")
    return files


def china_cells(
    reference_file: Path, china_shapefile: Path
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    with Dataset(reference_file) as dataset:
        lon = np.asarray(dataset.variables["lon"][:])
        lat = np.asarray(dataset.variables["lat"][:])
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    points = gpd.GeoSeries(
        gpd.points_from_xy(lon_grid.ravel(), lat_grid.ravel()), crs="EPSG:4326"
    )
    mainland = gpd.read_file(china_shapefile).to_crs("EPSG:4326").geometry.union_all()
    flat_indices = np.flatnonzero(points.within(mainland).to_numpy())
    return flat_indices, lon_grid.ravel()[flat_indices], lat_grid.ravel()[flat_indices]


def read_time_segments(files: list[Path]):
    segments = []
    all_dates = []
    calendar_name = ""
    previous_date = None
    for path in files:
        with Dataset(path) as dataset:
            time_var = dataset.variables["time"]
            calendar = getattr(time_var, "calendar", "standard")
            if not calendar_name:
                calendar_name = calendar
            elif calendar != calendar_name:
                raise RuntimeError("Calendar changes within one model-scenario series")
            dates = np.asarray(
                num2date(
                    time_var[:], units=time_var.units, calendar=calendar,
                    only_use_cftime_datetimes=True,
                ), dtype=object,
            )
            if previous_date is not None and dates[0] - previous_date != timedelta(days=1):
                raise RuntimeError(f"Non-daily boundary before {path.name}")
            if any(
                dates[i + 1] - dates[i] != timedelta(days=1)
                for i in range(len(dates) - 1)
            ):
                raise RuntimeError(f"Non-daily time axis in {path.name}")
            previous_date = dates[-1]
            keep = np.fromiter((date.year >= 1949 for date in dates), dtype=bool)
            if np.any(keep):
                indices = np.flatnonzero(keep)
                segments.append({
                    "path": path, "start": int(indices[0]), "stop": int(indices[-1] + 1)
                })
                all_dates.extend(dates[keep])
    years = np.fromiter((date.year for date in all_dates), dtype=np.int16)
    months = np.fromiter((date.month for date in all_dates), dtype=np.int8)
    return segments, years, months, calendar_name, str(all_dates[0]), str(all_dates[-1])


def load_runoff_cache(segments, flat_indices: np.ndarray, cache_path: Path):
    total_days = sum(item["stop"] - item["start"] for item in segments)
    cache = np.memmap(
        cache_path, mode="w+", dtype="float32", shape=(len(flat_indices), total_days)
    )
    position = 0
    for item in segments:
        with Dataset(item["path"]) as dataset:
            variable = dataset.variables["qtot"]
            if variable.dimensions != ("time", "lat", "lon"):
                raise RuntimeError(f"Unexpected qtot dimensions in {item['path']}")
            block = variable[item["start"]:item["stop"], :, :]
            block = np.ma.filled(block, np.nan).astype(np.float32, copy=False)
            block = block.reshape(block.shape[0], -1)[:, flat_indices]
            cache[:, position:position + len(block)] = block.T
            position += len(block)
    cache.flush()
    return cache


def event_counts(sri: np.ndarray, years: np.ndarray) -> dict[str, int | float]:
    period1 = (years >= 1950) & (years <= 1984)
    period2 = (years >= 1985) & (years <= 2019)
    drought1 = count_drought_events(sri[period1])
    drought2 = count_drought_events(sri[period2])
    flood1 = count_flood_events(sri[period1])
    flood2 = count_flood_events(sri[period2])
    return {
        "N_Drought_P1": drought1, "N_Drought_P2": drought2,
        "N_Flood_P1": flood1, "N_Flood_P2": flood2,
        "Drought_FR": corrected_frequency_ratio(drought2, drought1),
        "Flood_FR": corrected_frequency_ratio(flood2, flood1),
        "Drought_FR_zero_undefined": undefined_zero_frequency_ratio(drought2, drought1),
        "Flood_FR_zero_undefined": undefined_zero_frequency_ratio(flood2, flood1),
    }


def sensitivity_fieldnames() -> list[str]:
    names = [
        f"Drought_FR_T{threshold}_D{duration}"
        for threshold in ("05", "08", "10")
        for duration in (10, 20, 30)
    ]
    names.extend(
        f"Flood_FR_T{threshold}_G{gap}"
        for threshold in ("05", "08", "10")
        for gap in (10, 20, 30)
    )
    for split in (1980, 1985, 1990):
        names.extend((f"Drought_RR_S{split}", f"Flood_RR_S{split}"))
    return names


def sensitivity_ratios(sri: np.ndarray, years: np.ndarray) -> dict[str, float]:
    early = (years >= 1950) & (years <= 1984)
    recent = (years >= 1985) & (years <= 2019)
    values = {}
    for threshold, tag in ((-0.5, "05"), (-0.8, "08"), (-1.0, "10")):
        for duration in (10, 20, 30):
            n1 = count_drought_events(sri[early], threshold, duration)
            n2 = count_drought_events(sri[recent], threshold, duration)
            values[f"Drought_FR_T{tag}_D{duration}"] = corrected_frequency_ratio(n2, n1)
    for threshold, tag in ((0.5, "05"), (0.8, "08"), (1.0, "10")):
        for gap in (10, 20, 30):
            n1 = count_flood_events(sri[early], threshold, gap)
            n2 = count_flood_events(sri[recent], threshold, gap)
            values[f"Flood_FR_T{tag}_G{gap}"] = corrected_frequency_ratio(n2, n1)
    for split in (1980, 1985, 1990):
        first = (years >= 1950) & (years < split)
        second = (years >= split) & (years <= 2019)
        early_years = split - 1950
        recent_years = 2020 - split
        d1 = count_drought_events(sri[first])
        d2 = count_drought_events(sri[second])
        f1 = count_flood_events(sri[first])
        f2 = count_flood_events(sri[second])
        values[f"Drought_RR_S{split}"] = corrected_rate_ratio(
            d2, d1, recent_years, early_years
        )
        values[f"Flood_RR_S{split}"] = corrected_rate_ratio(
            f2, f1, recent_years, early_years
        )
    return values


def initialize_worker(cache_path: str, shape: tuple[int, int], years, months) -> None:
    global _CACHE, _YEARS, _MONTHS
    _CACHE = np.memmap(cache_path, mode="r", dtype="float32", shape=shape)
    _YEARS = years
    _MONTHS = months


def process_grid(task):
    column, flat_index, longitude, latitude = task
    running = backward_running_mean(np.asarray(_CACHE[column, :]))
    result = fit_monthly_sri(running, _YEARS, _MONTHS)
    study_period = (_YEARS >= 1950) & (_YEARS <= 2019)
    valid_sri = result.values[study_period]
    finite = np.isfinite(valid_sri)
    missing_days = int(np.count_nonzero(~finite))
    metric_names = [
        "N_Drought_P1", "N_Drought_P2", "N_Flood_P1", "N_Flood_P2",
        "Drought_FR", "Flood_FR", "Drought_FR_zero_undefined",
        "Flood_FR_zero_undefined", *sensitivity_fieldnames(),
    ]
    terminal_missing_only = (
        missing_days == 1
        and not finite[-1]
        and np.all(finite[:-1])
    )
    if result.failed_months == 0 and (missing_days == 0 or terminal_missing_only):
        metrics = event_counts(result.values, _YEARS)
        metrics.update(sensitivity_ratios(result.values, _YEARS))
    else:
        metrics = {name: np.nan for name in metric_names}
    row = {
        "Grid_ID": int(flat_index + 1), "Flat_Index": int(flat_index),
        "Lon": float(longitude), "Lat": float(latitude),
        **metrics,
        "Failed_SRI_Months": result.failed_months,
        "Missing_SRI_Days_1950_2019": missing_days,
        "SRI_Mean_1950_2019": float(np.mean(valid_sri[finite])) if np.any(finite) else np.nan,
        "SRI_SD_1950_2019": float(np.std(valid_sri[finite])) if np.any(finite) else np.nan,
        "Selected_Distributions": "|".join(
            name if name is not None else "failed" for name in result.selected_distributions
        ),
    }
    return row, result.selected_distributions, result.failed_months, result.negative_input_count

def main() -> None:
    args = parse_args()
    if args.workers < 1:
        raise ValueError("workers must be positive")
    started = time.perf_counter()
    files = find_files(args.raw_root, args.scenario, args.model)
    flat_indices, longitudes, latitudes = china_cells(files[0], args.china_shapefile)
    if args.max_cells is not None:
        flat_indices = flat_indices[:args.max_cells]
        longitudes = longitudes[:args.max_cells]
        latitudes = latitudes[:args.max_cells]

    segments, years, months, calendar, first_date, last_date = read_time_segments(files)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.scenario}__{args.model}"
    cache_path = args.output_dir / f"{stem}.runoff.tmp"
    output_path = args.output_dir / f"{stem}.csv"
    log_path = args.output_dir / f"{stem}.json"
    cache = load_runoff_cache(segments, flat_indices, cache_path)
    cache_shape = cache.shape
    del cache

    tasks = list(zip(range(len(flat_indices)), flat_indices, longitudes, latitudes))
    fields = [
        "Grid_ID", "Flat_Index", "Lon", "Lat",
        "N_Drought_P1", "N_Drought_P2", "N_Flood_P1", "N_Flood_P2",
        "Drought_FR", "Flood_FR",
        "Drought_FR_zero_undefined", "Flood_FR_zero_undefined",
        *sensitivity_fieldnames(),
        "Failed_SRI_Months", "Missing_SRI_Days_1950_2019",
        "SRI_Mean_1950_2019", "SRI_SD_1950_2019", "Selected_Distributions",
    ]
    distribution_counts = Counter()
    total_failed_months = 0
    total_negative_values = 0
    completed = 0

    try:
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            with ProcessPoolExecutor(
                max_workers=args.workers,
                initializer=initialize_worker,
                initargs=(str(cache_path), cache_shape, years, months),
            ) as executor:
                for row, selected, failed, negative in executor.map(
                    process_grid, tasks, chunksize=4
                ):
                    writer.writerow(row)
                    distribution_counts.update(name for name in selected if name is not None)
                    total_failed_months += failed
                    total_negative_values += negative
                    completed += 1
                    if completed % 250 == 0:
                        print(f"processed={completed}/{len(tasks)}", flush=True)
    finally:
        cache_path.unlink(missing_ok=True)

    log = {
        "scenario": args.scenario, "model": args.model,
        "input_file_count": len(files), "calendar": calendar,
        "processed_grid_count": len(flat_indices), "workers": args.workers,
        "time_start": first_date, "time_end": last_date,
        "processed_day_count": len(years),
        "distribution_selection_counts": dict(distribution_counts),
        "calibration_scheme": "scenario_specific_monthly",
        "invalid_model_grid_rule": "exclude_if_failed_month_or_nonterminal_missing_sri",
        "fit_scale_rule": "monthly_positive_median",
        "failed_month_fit_count": total_failed_months,
        "negative_30day_value_count": total_negative_values,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "output": str(output_path),
    }
    log_path.write_text(json.dumps(log, indent=2), encoding="utf-8")
    print(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()





