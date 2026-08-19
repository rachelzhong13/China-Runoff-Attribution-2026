from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = ROOT / "outputs" / "model_events"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "aggregated"
SCENARIOS = ["countclim-1901soc", "countclim-histsoc", "obsclim-histsoc"]
MODELS = [
    "h08", "hydropy", "jules-w2", "lpjml5-7-10-fire", "miroc-integ-land",
    "watergap2-2e", "web-dhm-sg",
]
SCENARIO_LABEL = {
    "countclim-1901soc": "Natural",
    "countclim-histsoc": "Historical",
    "obsclim-histsoc": "Factual",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--basin-shapefile", type=Path, required=True)
    return parser.parse_args()


def read_output(input_dir: Path, scenario: str, model: str) -> pd.DataFrame:
    path = input_dir / f"{scenario}__{model}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_csv(path)
    required = {
        "Grid_ID", "Lon", "Lat", "Drought_FR", "Flood_FR",
        "Drought_FR_T05_D10", "Flood_FR_T05_G10", "Flood_FR_T05_G20",
        "Flood_FR_T10_G30", "Flood_RR_S1990",
    }
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"{path.name} lacks {sorted(missing)}")
    if len(frame) != 3823 or frame["Grid_ID"].nunique() != 3823:
        raise RuntimeError(f"{path.name} does not contain 3823 unique mainland cells")
    return frame


def paired_model_results(frames: dict[tuple[str, str], pd.DataFrame]) -> pd.DataFrame:
    outputs = []
    for model in MODELS:
        natural = frames[("countclim-1901soc", model)]
        historical = frames[("countclim-histsoc", model)]
        factual = frames[("obsclim-histsoc", model)]
        for event in ("Drought", "Flood"):
            primary = f"{event}_FR"
            alternate = f"{event}_FR_zero_undefined"
            result = natural[["Grid_ID", "Lon", "Lat", primary, alternate]].rename(
                columns={primary: "FR_Natural", alternate: "FR_Natural_ZeroUndefined"}
            )
            result = result.merge(
                historical[["Grid_ID", primary, alternate]].rename(
                    columns={primary: "FR_Historical", alternate: "FR_Historical_ZeroUndefined"}
                ), on="Grid_ID", validate="one_to_one",
            )
            result = result.merge(
                factual[["Grid_ID", primary, alternate]].rename(
                    columns={primary: "FR_Factual", alternate: "FR_Factual_ZeroUndefined"}
                ), on="Grid_ID", validate="one_to_one",
            )
            result["Model"] = model
            result["Event_Type"] = event
            result["Delta_Total"] = result["FR_Factual"] - result["FR_Natural"]
            result["Delta_HA"] = result["FR_Historical"] - result["FR_Natural"]
            result["Delta_CC"] = result["FR_Factual"] - result["FR_Historical"]
            primary_valid = result[["FR_Natural", "FR_Historical", "FR_Factual"]].notna().all(axis=1)
            result.loc[~primary_valid, ["Delta_Total", "Delta_HA", "Delta_CC"]] = np.nan
            result["Delta_Total_ZeroUndefined"] = result["FR_Factual_ZeroUndefined"] - result["FR_Natural_ZeroUndefined"]
            result["Delta_HA_ZeroUndefined"] = result["FR_Historical_ZeroUndefined"] - result["FR_Natural_ZeroUndefined"]
            result["Delta_CC_ZeroUndefined"] = result["FR_Factual_ZeroUndefined"] - result["FR_Historical_ZeroUndefined"]
            alternate_valid = result[[
                "FR_Natural_ZeroUndefined", "FR_Historical_ZeroUndefined", "FR_Factual_ZeroUndefined"
            ]].notna().all(axis=1)
            result.loc[~alternate_valid, [
                "Delta_Total_ZeroUndefined", "Delta_HA_ZeroUndefined", "Delta_CC_ZeroUndefined"
            ]] = np.nan
            outputs.append(result)
    return pd.concat(outputs, ignore_index=True)


def sign_agreement(values: pd.Series) -> float:
    values = values.dropna().to_numpy()
    if len(values) == 0:
        return np.nan
    centre_sign = np.sign(values.mean())
    if centre_sign == 0:
        return np.nan
    return float(np.mean(np.sign(values) == centre_sign))


def grid_summary(per_model: pd.DataFrame) -> pd.DataFrame:
    keys = ["Grid_ID", "Lon", "Lat", "Event_Type"]
    metrics = ["FR_Natural", "FR_Historical", "FR_Factual", "Delta_Total", "Delta_HA", "Delta_CC"]
    grouped = per_model.groupby(keys, sort=False)
    summary = grouped[metrics].agg(
        ["mean", "std", lambda x: x.quantile(0.25), lambda x: x.quantile(0.75), "count"]
    )
    summary.columns = [f"{metric}_{stat}" for metric, stat in summary.columns]
    summary = summary.rename(
        columns=lambda name: name.replace("_<lambda_0>", "_q25").replace("_<lambda_1>", "_q75")
    ).reset_index()
    for metric in ("Delta_Total", "Delta_HA", "Delta_CC"):
        agreement = grouped[metric].apply(sign_agreement).rename(f"{metric}_Sign_Agreement")
        summary = summary.merge(agreement.reset_index(), on=keys, validate="one_to_one")
    return summary


def basin_mapping(per_model: pd.DataFrame, basin_shapefile: Path) -> pd.DataFrame:
    coordinates = per_model[["Grid_ID", "Lon", "Lat"]].drop_duplicates()
    points = gpd.GeoDataFrame(
        coordinates,
        geometry=gpd.points_from_xy(coordinates["Lon"], coordinates["Lat"]),
        crs="EPSG:4326",
    )
    basins = gpd.read_file(basin_shapefile).to_crs("EPSG:4326")
    basins["geometry"] = basins.geometry.make_valid()
    basins = basins[["Basin_Name", "geometry"]].dissolve(by="Basin_Name").reset_index()
    mapped = gpd.sjoin(points, basins, how="inner", predicate="within")
    if mapped["Grid_ID"].duplicated().any():
        raise RuntimeError("At least one grid centre maps to multiple basin names")
    return mapped[["Grid_ID", "Basin_Name"]]


def weighted_mean(frame: pd.DataFrame, column: str) -> float:
    valid = frame[column].notna()
    if not valid.any():
        return np.nan
    return float(np.average(frame.loc[valid, column], weights=frame.loc[valid, "Weight"]))


def basin_model_stats(per_model: pd.DataFrame, mapping: pd.DataFrame, suffix: str = ""):
    data = per_model.merge(mapping, on="Grid_ID", validate="many_to_one")
    data["Weight"] = np.cos(np.radians(data["Lat"]))
    rows = []
    for (model, event, basin), group in data.groupby(["Model", "Event_Type", "Basin_Name"]):
        total = f"Delta_Total{suffix}"
        cc = f"Delta_CC{suffix}"
        ha = f"Delta_HA{suffix}"
        valid = group[[total, cc, ha]].notna().all(axis=1)
        analysis = group.loc[valid].copy()
        mean_total = weighted_mean(analysis, total)
        mean_cc = weighted_mean(analysis, cc)
        mean_ha = weighted_mean(analysis, ha)
        analysis["Abs_CC"] = analysis[cc].abs()
        analysis["Abs_HA"] = analysis[ha].abs()
        abs_cc = weighted_mean(analysis, "Abs_CC")
        abs_ha = weighted_mean(analysis, "Abs_HA")
        rows.append({
            "Model": model, "Event_Type": event, "Basin_Name": basin,
            "Mean_Delta_Total": mean_total, "Mean_Delta_CC": mean_cc,
            "Mean_Delta_HA": mean_ha, "Mean_Abs_Delta_CC": abs_cc,
            "Mean_Abs_Delta_HA": abs_ha,
            "DIndex": abs_cc / (abs_cc + abs_ha) if abs_cc + abs_ha > 0 else np.nan,
            "Residual": mean_total - mean_cc - mean_ha,
            "Valid_Grid_Count": int(valid.sum()),
        })
    return pd.DataFrame(rows)


def basin_summary(model_stats: pd.DataFrame) -> pd.DataFrame:
    keys = ["Basin_Name", "Event_Type"]
    metrics = [
        "Mean_Delta_Total", "Mean_Delta_CC", "Mean_Delta_HA",
        "Mean_Abs_Delta_CC", "Mean_Abs_Delta_HA", "DIndex",
    ]
    grouped = model_stats.groupby(keys, sort=True)
    summary = grouped[metrics].agg(
        ["mean", "std", lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]
    )
    summary.columns = [f"{metric}_{stat}" for metric, stat in summary.columns]
    summary = summary.rename(
        columns=lambda name: name.replace("_<lambda_0>", "_q25").replace("_<lambda_1>", "_q75")
    ).reset_index()
    summary["Model_Count"] = grouped["Model"].nunique().to_numpy()
    summary["Pooled_DIndex"] = summary["Mean_Abs_Delta_CC_mean"] / (
        summary["Mean_Abs_Delta_CC_mean"] + summary["Mean_Abs_Delta_HA_mean"]
    )
    dominance = grouped["DIndex"].agg(
        Climate_Dominant_Model_Count=lambda x: int(x.gt(0.5).sum()),
        Human_Dominant_Model_Count=lambda x: int(x.lt(0.5).sum()),
        Equal_Model_Count=lambda x: int(x.eq(0.5).sum()),
    ).reset_index()
    summary = summary.merge(dominance, on=keys, validate="one_to_one")
    return summary


def leave_one_model_out(model_stats: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (basin, event), group in model_stats.groupby(["Basin_Name", "Event_Type"]):
        for omitted in sorted(group["Model"].unique()):
            kept = group[group["Model"] != omitted]
            mean_cc = float(kept["Mean_Abs_Delta_CC"].mean())
            mean_ha = float(kept["Mean_Abs_Delta_HA"].mean())
            denominator = mean_cc + mean_ha
            rows.append({
                "Basin_Name": basin,
                "Event_Type": event,
                "Omitted_Model": omitted,
                "Retained_Model_Count": int(kept["Model"].nunique()),
                "Mean_Abs_Delta_CC": mean_cc,
                "Mean_Abs_Delta_HA": mean_ha,
                "Pooled_DIndex": mean_cc / denominator if denominator > 0 else np.nan,
            })
    return pd.DataFrame(rows)


def definition_specs() -> list[tuple[str, str, str, str]]:
    specs = []
    for threshold in ("05", "08", "10"):
        for duration in (10, 20, 30):
            definition = f"T{threshold}_D{duration}"
            specs.append((
                "Event_Definition", "Drought", definition,
                f"Drought_FR_{definition}",
            ))
        for gap in (10, 20, 30):
            definition = f"T{threshold}_G{gap}"
            specs.append((
                "Event_Definition", "Flood", definition,
                f"Flood_FR_{definition}",
            ))
    for event in ("Drought", "Flood"):
        for split in (1980, 1985, 1990):
            definition = f"S{split}"
            specs.append((
                "Period_Split", event, definition, f"{event}_RR_{definition}"
            ))
    return specs


def definition_basin_stats(
    frames: dict[tuple[str, str], pd.DataFrame], mapping: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = []
    for family, event, definition, column in definition_specs():
        for model in MODELS:
            natural = frames[("countclim-1901soc", model)]
            historical = frames[("countclim-histsoc", model)]
            factual = frames[("obsclim-histsoc", model)]
            data = natural[["Grid_ID", "Lon", "Lat", column]].rename(
                columns={column: "Natural"}
            )
            data = data.merge(
                historical[["Grid_ID", column]].rename(columns={column: "Historical"}),
                on="Grid_ID", validate="one_to_one",
            )
            data = data.merge(
                factual[["Grid_ID", column]].rename(columns={column: "Factual"}),
                on="Grid_ID", validate="one_to_one",
            )
            data = data.merge(mapping, on="Grid_ID", validate="many_to_one")
            valid = data[["Natural", "Historical", "Factual"]].notna().all(axis=1)
            data = data.loc[valid].copy()
            data["Weight"] = np.cos(np.radians(data["Lat"]))
            data["Delta_CC"] = data["Factual"] - data["Historical"]
            data["Delta_HA"] = data["Historical"] - data["Natural"]
            data["Abs_CC"] = data["Delta_CC"].abs()
            data["Abs_HA"] = data["Delta_HA"].abs()
            for basin, group in data.groupby("Basin_Name"):
                mean_cc = weighted_mean(group, "Abs_CC")
                mean_ha = weighted_mean(group, "Abs_HA")
                denominator = mean_cc + mean_ha
                rows.append({
                    "Family": family,
                    "Event_Type": event,
                    "Definition": definition,
                    "Model": model,
                    "Basin_Name": basin,
                    "Mean_Abs_Delta_CC": mean_cc,
                    "Mean_Abs_Delta_HA": mean_ha,
                    "DIndex": mean_cc / denominator if denominator > 0 else np.nan,
                    "Valid_Grid_Count": int(len(group)),
                })
    by_model = pd.DataFrame(rows)
    keys = ["Family", "Event_Type", "Definition", "Basin_Name"]
    grouped = by_model.groupby(keys, sort=True)
    summary = grouped[["Mean_Abs_Delta_CC", "Mean_Abs_Delta_HA", "DIndex"]].agg(
        ["mean", "std", "min", "max"]
    )
    summary.columns = [f"{metric}_{stat}" for metric, stat in summary.columns]
    summary = summary.reset_index()
    summary["Model_Count"] = grouped["Model"].nunique().to_numpy()
    summary["Pooled_DIndex"] = summary["Mean_Abs_Delta_CC_mean"] / (
        summary["Mean_Abs_Delta_CC_mean"] + summary["Mean_Abs_Delta_HA_mean"]
    )
    dominance = grouped["DIndex"].agg(
        Climate_Dominant_Model_Count=lambda x: int(x.gt(0.5).sum()),
        Human_Dominant_Model_Count=lambda x: int(x.lt(0.5).sum()),
    ).reset_index()
    summary = summary.merge(dominance, on=keys, validate="one_to_one")

    range_keys = ["Family", "Event_Type", "Basin_Name"]
    robustness = summary.groupby(range_keys)["Pooled_DIndex"].agg(
        Definition_Count="count", DIndex_Min="min", DIndex_Max="max"
    ).reset_index()
    robustness["Crosses_0_5"] = (
        robustness["DIndex_Min"].lt(0.5) & robustness["DIndex_Max"].gt(0.5)
    )
    return by_model, summary, robustness


def sensitivity_outputs(frames: dict[tuple[str, str], pd.DataFrame]):
    drought_rows = []
    flood_rows = []
    split_rows = []
    drought_columns = [
        f"Drought_FR_T{threshold}_D{duration}"
        for threshold in ("05", "08", "10") for duration in (10, 20, 30)
    ]
    flood_columns = [
        f"Flood_FR_T{threshold}_G{gap}"
        for threshold in ("05", "08", "10") for gap in (10, 20, 30)
    ]
    for scenario in SCENARIOS:
        for model in MODELS:
            frame = frames[(scenario, model)]
            for column in drought_columns:
                drought_rows.append({
                    "Scenario": scenario, "Scenario_Label": SCENARIO_LABEL[scenario],
                    "Model": model, "Definition": column.removeprefix("Drought_FR_"),
                    "Percent_FR_GT_1": float(frame[column].dropna().gt(1.0).mean() * 100.0),
                })
            for column in flood_columns:
                flood_rows.append({
                    "Scenario": scenario, "Scenario_Label": SCENARIO_LABEL[scenario],
                    "Model": model, "Definition": column.removeprefix("Flood_FR_"),
                    "Percent_FR_GT_1": float(frame[column].dropna().gt(1.0).mean() * 100.0),
                })
            for event in ("Drought", "Flood"):
                for split in (1980, 1985, 1990):
                    column = f"{event}_RR_S{split}"
                    split_rows.append({
                        "Scenario": scenario, "Scenario_Label": SCENARIO_LABEL[scenario],
                        "Model": model, "Event_Type": event, "Split_Year": split,
                        "Percent_RR_GT_1": float(frame[column].dropna().gt(1.0).mean() * 100.0),
                    })
    drought = pd.DataFrame(drought_rows)
    flood = pd.DataFrame(flood_rows)
    splits = pd.DataFrame(split_rows)
    stats = ["mean", "std", "min", "max", lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]
    drought_summary = drought.groupby(["Scenario", "Scenario_Label", "Definition"])["Percent_FR_GT_1"].agg(stats).reset_index()
    flood_summary = flood.groupby(["Scenario", "Scenario_Label", "Definition"])["Percent_FR_GT_1"].agg(stats).reset_index()
    split_summary = splits.groupby(["Scenario", "Scenario_Label", "Event_Type", "Split_Year"])["Percent_RR_GT_1"].agg(stats).reset_index()
    for frame in (drought_summary, flood_summary, split_summary):
        frame.rename(columns={"<lambda_0>": "q25", "<lambda_1>": "q75"}, inplace=True)
    return drought, drought_summary, flood, flood_summary, splits, split_summary


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    frames = {
        (scenario, model): read_output(args.input_dir, scenario, model)
        for scenario in SCENARIOS for model in MODELS
    }
    per_model = paired_model_results(frames)
    per_model.to_csv(args.output_dir / "per_model_attribution.csv", index=False)
    grid_summary(per_model).to_csv(args.output_dir / "grid_ensemble_attribution.csv", index=False)

    mapping = basin_mapping(per_model, args.basin_shapefile)
    mapping.to_csv(args.output_dir / "grid_to_basin.csv", index=False)
    basin_models = basin_model_stats(per_model, mapping)
    basin_models.to_csv(args.output_dir / "table1_basin_by_model.csv", index=False)
    basin_summary(basin_models).to_csv(args.output_dir / "table1_basin_summary.csv", index=False)
    leave_one_model_out(basin_models).to_csv(
        args.output_dir / "lomo_basin_dindex.csv", index=False
    )
    definition_models, definition_summary, definition_robustness = (
        definition_basin_stats(frames, mapping)
    )
    definition_models.to_csv(
        args.output_dir / "definition_basin_by_model.csv", index=False
    )
    definition_summary.to_csv(
        args.output_dir / "definition_basin_summary.csv", index=False
    )
    definition_robustness.to_csv(
        args.output_dir / "definition_robustness_range.csv", index=False
    )
    basin_zero = basin_model_stats(per_model, mapping, "_ZeroUndefined")
    basin_zero.to_csv(args.output_dir / "zero_rule_basin_by_model.csv", index=False)
    basin_summary(basin_zero).to_csv(args.output_dir / "zero_rule_basin_summary.csv", index=False)

    names = [
        "drought_sensitivity_by_model.csv", "drought_sensitivity_summary.csv",
        "flood_sensitivity_by_model.csv", "flood_sensitivity_summary.csv",
        "period_sensitivity_by_model.csv", "period_sensitivity_summary.csv",
    ]
    for name, frame in zip(names, sensitivity_outputs(frames)):
        frame.to_csv(args.output_dir / name, index=False)

    validation = {
        "mainland_grid_count": int(per_model["Grid_ID"].nunique()),
        "model_count": int(per_model["Model"].nunique()),
        "basin_names": sorted(mapping["Basin_Name"].unique().tolist()),
        "mapped_basin_grid_count": int(mapping["Grid_ID"].nunique()),
        "max_abs_per_model_additivity_residual": float(
            (per_model["Delta_Total"] - per_model["Delta_CC"] - per_model["Delta_HA"]).abs().max()
        ),
        "max_abs_basin_additivity_residual": float(basin_models["Residual"].abs().max()),
    }
    (args.output_dir / "aggregation_validation.json").write_text(
        json.dumps(validation, indent=2), encoding="utf-8"
    )
    print(json.dumps(validation, indent=2))


if __name__ == "__main__":
    main()


