from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_SCENARIO_DIR = ROOT / "outputs" / "aggregated"
DEFAULT_COMMON_DIR = ROOT / "outputs" / "common_reference_aggregated"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "calibration_comparison"
DEFAULT_SCENARIO_EVENTS = ROOT / "outputs" / "model_events"
DEFAULT_COMMON_EVENTS = ROOT / "outputs" / "common_reference_events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario-dir", type=Path, default=DEFAULT_SCENARIO_DIR)
    parser.add_argument("--common-dir", type=Path, default=DEFAULT_COMMON_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--scenario-events-dir", type=Path, default=DEFAULT_SCENARIO_EVENTS)
    parser.add_argument("--common-events-dir", type=Path, default=DEFAULT_COMMON_EVENTS)
    return parser.parse_args()


def sign_concordance(left: pd.Series, right: pd.Series) -> float:
    valid = left.notna() & right.notna() & left.ne(0.0) & right.ne(0.0)
    if not valid.any():
        return np.nan
    return float(np.sign(left[valid]).eq(np.sign(right[valid])).mean())


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    scenario_models = pd.read_csv(args.scenario_dir / "per_model_attribution.csv")
    common_models = pd.read_csv(args.common_dir / "per_model_attribution.csv")
    keys = ["Grid_ID", "Model", "Event_Type"]
    metrics = ["Delta_Total", "Delta_HA", "Delta_CC"]
    paired = scenario_models[keys + metrics].merge(
        common_models[keys + metrics],
        on=keys,
        suffixes=("_ScenarioSpecific", "_CommonReference"),
        validate="one_to_one",
    )

    rows = []
    for event, group in paired.groupby("Event_Type"):
        for metric in metrics:
            left = group[f"{metric}_ScenarioSpecific"]
            right = group[f"{metric}_CommonReference"]
            valid = left.notna() & right.notna()
            rows.append({
                "Event_Type": event,
                "Metric": metric,
                "Paired_Count": int(valid.sum()),
                "Pearson_R": float(left[valid].corr(right[valid])) if valid.sum() > 1 else np.nan,
                "Median_Absolute_Difference": float((left[valid] - right[valid]).abs().median()),
                "Sign_Concordance": sign_concordance(left, right),
            })
    metric_comparison = pd.DataFrame(rows)
    metric_comparison.to_csv(
        args.output_dir / "calibration_grid_model_metrics.csv", index=False
    )

    scenario_grid = pd.read_csv(args.scenario_dir / "grid_ensemble_attribution.csv")
    common_grid = pd.read_csv(args.common_dir / "grid_ensemble_attribution.csv")
    grid_keys = ["Grid_ID", "Lon", "Lat", "Event_Type"]
    ensemble_metrics = [f"{metric}_mean" for metric in metrics]
    grid_pair = scenario_grid[grid_keys + ensemble_metrics].merge(
        common_grid[grid_keys + ensemble_metrics],
        on=grid_keys,
        suffixes=("_ScenarioSpecific", "_CommonReference"),
        validate="one_to_one",
    )
    grid_rows = []
    for event, group in grid_pair.groupby("Event_Type"):
        for metric in ensemble_metrics:
            left = group[f"{metric}_ScenarioSpecific"]
            right = group[f"{metric}_CommonReference"]
            valid = left.notna() & right.notna()
            grid_rows.append({
                "Event_Type": event,
                "Metric": metric,
                "Paired_Count": int(valid.sum()),
                "Pearson_R": float(left[valid].corr(right[valid])) if valid.sum() > 1 else np.nan,
                "Median_Absolute_Difference": float((left[valid] - right[valid]).abs().median()),
                "Sign_Concordance": sign_concordance(left, right),
            })
    pd.DataFrame(grid_rows).to_csv(
        args.output_dir / "calibration_grid_ensemble_metrics.csv", index=False
    )
    grid_pair.to_csv(
        args.output_dir / "calibration_grid_ensemble_comparison.csv", index=False
    )

    scenario_basin = pd.read_csv(args.scenario_dir / "table1_basin_summary.csv")
    common_basin = pd.read_csv(args.common_dir / "table1_basin_summary.csv")
    basin_keys = ["Basin_Name", "Event_Type"]
    basin = scenario_basin[basin_keys + ["Pooled_DIndex"]].merge(
        common_basin[basin_keys + ["Pooled_DIndex"]],
        on=basin_keys,
        suffixes=("_ScenarioSpecific", "_CommonReference"),
        validate="one_to_one",
    )
    basin["DIndex_Difference"] = (
        basin["Pooled_DIndex_CommonReference"]
        - basin["Pooled_DIndex_ScenarioSpecific"]
    )
    basin["Dominance_ScenarioSpecific"] = np.where(
        basin["Pooled_DIndex_ScenarioSpecific"] > 0.5, "Climate", "Human"
    )
    basin["Dominance_CommonReference"] = np.where(
        basin["Pooled_DIndex_CommonReference"] > 0.5, "Climate", "Human"
    )
    basin["Dominance_Stable"] = (
        basin["Dominance_ScenarioSpecific"] == basin["Dominance_CommonReference"]
    )
    basin.to_csv(args.output_dir / "calibration_basin_comparison.csv", index=False)

    scenario_lomo = pd.read_csv(args.scenario_dir / "lomo_basin_dindex.csv")
    common_lomo = pd.read_csv(args.common_dir / "lomo_basin_dindex.csv")
    lomo_keys = ["Basin_Name", "Event_Type", "Omitted_Model"]
    lomo = scenario_lomo[lomo_keys + ["Pooled_DIndex"]].merge(
        common_lomo[lomo_keys + ["Pooled_DIndex"]],
        on=lomo_keys,
        suffixes=("_ScenarioSpecific", "_CommonReference"),
        validate="one_to_one",
    )
    lomo["DIndex_Difference"] = (
        lomo["Pooled_DIndex_CommonReference"]
        - lomo["Pooled_DIndex_ScenarioSpecific"]
    )
    lomo.to_csv(args.output_dir / "calibration_lomo_comparison.csv", index=False)

    factual_max_abs_difference = 0.0
    factual_nan_mismatch_count = 0
    factual_text_mismatch_count = 0
    factual_files_checked = 0
    for scenario_path in sorted(args.scenario_events_dir.glob("obsclim-histsoc__*.csv")):
        common_path = args.common_events_dir / scenario_path.name
        if not common_path.exists():
            raise FileNotFoundError(common_path)
        left = pd.read_csv(scenario_path).sort_values("Grid_ID").reset_index(drop=True)
        right = pd.read_csv(common_path).sort_values("Grid_ID").reset_index(drop=True)
        if not left["Grid_ID"].equals(right["Grid_ID"]):
            raise RuntimeError(f"Factual Grid_ID mismatch in {scenario_path.name}")
        for column in left.columns.intersection(right.columns):
            if pd.api.types.is_numeric_dtype(left[column]):
                factual_nan_mismatch_count += int(
                    (left[column].isna() != right[column].isna()).sum()
                )
                valid = left[column].notna() & right[column].notna()
                if valid.any():
                    difference = float((left.loc[valid, column] - right.loc[valid, column]).abs().max())
                    factual_max_abs_difference = max(factual_max_abs_difference, difference)
            else:
                factual_text_mismatch_count += int(
                    left[column].fillna("<missing>").ne(
                        right[column].fillna("<missing>")
                    ).sum()
                )
        factual_files_checked += 1

    summary = {
        "paired_grid_model_rows": int(len(paired)),
        "basin_event_rows": int(len(basin)),
        "basin_dominance_stable_count": int(basin["Dominance_Stable"].sum()),
        "basin_dominance_total_count": int(len(basin)),
        "max_abs_basin_dindex_difference": float(basin["DIndex_Difference"].abs().max()),
        "max_abs_lomo_dindex_difference": float(lomo["DIndex_Difference"].abs().max()),
        "factual_files_checked": factual_files_checked,
        "max_abs_factual_identity_difference": factual_max_abs_difference,
        "factual_nan_mismatch_count": factual_nan_mismatch_count,
        "factual_text_mismatch_count": factual_text_mismatch_count,
    }
    (args.output_dir / "calibration_comparison_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
