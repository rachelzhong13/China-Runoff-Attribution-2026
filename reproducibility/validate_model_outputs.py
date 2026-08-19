from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = ROOT / "outputs" / "model_events"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "validation"
SCENARIOS = ["countclim-1901soc", "countclim-histsoc", "obsclim-histsoc"]
MODELS = [
    "h08", "hydropy", "jules-w2", "lpjml5-7-10-fire", "miroc-integ-land",
    "watergap2-2e", "web-dhm-sg",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--expected-calibration-scheme")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    reference = None
    for scenario in SCENARIOS:
        for model in MODELS:
            stem = f"{scenario}__{model}"
            csv_path = args.input_dir / f"{stem}.csv"
            log_path = args.input_dir / f"{stem}.json"
            if not csv_path.exists() or not log_path.exists():
                raise FileNotFoundError(stem)
            frame = pd.read_csv(csv_path)
            log = json.loads(log_path.read_text(encoding="utf-8"))
            coordinates = frame[["Grid_ID", "Lon", "Lat"]].sort_values("Grid_ID").reset_index(drop=True)
            if reference is None:
                reference = coordinates
                aligned = True
            else:
                aligned = coordinates.equals(reference)
            required = {"Flood_FR_T05_G10", "Flood_FR_T05_G20", "Flood_FR_T10_G30"}
            missing = required - set(frame.columns)
            if missing:
                raise RuntimeError(f"{csv_path.name} lacks {sorted(missing)}")
            drought_baseline_error = (frame["Drought_FR"] - frame["Drought_FR_T05_D20"]).abs().max()
            flood_baseline_error = (frame["Flood_FR"] - frame["Flood_FR_T05_G20"]).abs().max()
            drought_split_error = (frame["Drought_FR"] - frame["Drought_RR_S1985"]).abs().max()
            flood_split_error = (frame["Flood_FR"] - frame["Flood_RR_S1985"]).abs().max()
            invalid_expected = (frame["Failed_SRI_Months"] > 0) | (frame["Missing_SRI_Days_1950_2019"] > 1)
            actual_missing = frame[["Drought_FR", "Flood_FR"]].isna().any(axis=1)
            rows.append({
                "Scenario": scenario,
                "Model": model,
                "Calendar": log["calendar"],
                "Invalid_Grid_Rule": log.get("invalid_model_grid_rule", ""),
                "Fit_Scale_Rule": log.get("fit_scale_rule", ""),
                "Calibration_Scheme": log.get("calibration_scheme", "scenario_specific_monthly"),
                "Time_Start": log["time_start"],
                "Time_End": log["time_end"],
                "Grid_Count": len(frame),
                "Unique_Grid_Count": frame["Grid_ID"].nunique(),
                "Coordinates_Aligned": aligned,
                "Failed_Month_Fit_Count": int(frame["Failed_SRI_Months"].sum()),
                "Grids_With_Failed_Months": int((frame["Failed_SRI_Months"] > 0).sum()),
                "Missing_SRI_Days": int(frame["Missing_SRI_Days_1950_2019"].sum()),
                "Grids_With_Missing_SRI": int((frame["Missing_SRI_Days_1950_2019"] > 0).sum()),
                "Negative_30Day_Value_Count": int(log["negative_30day_value_count"]),
                "Valid_Grid_Count": int((~actual_missing).sum()),
                "FR_Missing_Rule_Mismatch_Count": int((invalid_expected != actual_missing).sum()),
                "Max_Drought_Baseline_Error": float(drought_baseline_error),
                "Max_Flood_Baseline_Error": float(flood_baseline_error),
                "Max_Drought_1985_Split_Error": float(drought_split_error),
                "Max_Flood_1985_Split_Error": float(flood_split_error),
                "SRI_Mean_Min": float(frame["SRI_Mean_1950_2019"].min()),
                "SRI_Mean_Max": float(frame["SRI_Mean_1950_2019"].max()),
                "SRI_SD_Min": float(frame["SRI_SD_1950_2019"].min()),
                "SRI_SD_Max": float(frame["SRI_SD_1950_2019"].max()),
                "Elapsed_Seconds": float(log["elapsed_seconds"]),
            })
    validation = pd.DataFrame(rows)
    validation.to_csv(args.output_dir / "model_output_validation.csv", index=False)
    calibration_scheme_valid = (
        True if args.expected_calibration_scheme is None
        else validation["Calibration_Scheme"].eq(args.expected_calibration_scheme).all()
    )
    passed = (
        (validation["Grid_Count"] == 3823).all()
        and (validation["Unique_Grid_Count"] == 3823).all()
        and validation["Coordinates_Aligned"].all()
        and (validation["FR_Missing_Rule_Mismatch_Count"] == 0).all()
        and validation["Invalid_Grid_Rule"].isin({
            "exclude_if_failed_month_or_missing_sri",
            "exclude_if_failed_month_or_nonterminal_missing_sri",
        }).all()
        and validation["Fit_Scale_Rule"].eq("monthly_positive_median").all()
        and calibration_scheme_valid
        and (validation["Time_End"].str.startswith("2019-12-31")).all()
        and (validation[[
            "Max_Drought_Baseline_Error", "Max_Flood_Baseline_Error",
            "Max_Drought_1985_Split_Error", "Max_Flood_1985_Split_Error",
        ]].max().max() < 1e-12)
    )
    report = [
        "# Recomputed model-output validation",
        "",
        f"Overall structural validation: {'PASS' if passed else 'FAIL'}",
        "",
        f"Model-scenario outputs checked: {len(validation)}",
        f"Mainland grid cells per output: {validation['Grid_Count'].min()}-{validation['Grid_Count'].max()}",
        f"Total failed month fits: {validation['Failed_Month_Fit_Count'].sum()}",
        f"Total missing SRI days in 1950-2019 outputs: {validation['Missing_SRI_Days'].sum()}",
        f"Total negative 30-day values: {validation['Negative_30Day_Value_Count'].sum()}",
        f"Maximum baseline/sensitivity identity error: {validation.filter(like='Error').max().max():.3e}",
        "",
        "See `model_output_validation.csv` for model-scenario details.",
    ]
    (args.output_dir / "model_output_validation_report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report))
    if not passed:
        raise RuntimeError("Structural validation failed")


if __name__ == "__main__":
    main()



