from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = ROOT / "outputs" / "common_reference_aggregated"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "figures" / "common_reference"

plt.rcParams.update({
    "font.family": "Arial",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans", "sans-serif"],
    "font.size": 8,
    "axes.linewidth": 0.7,
    "xtick.major.width": 0.7,
    "ytick.major.width": 0.7,
    "savefig.dpi": 400,
    "svg.fonttype": "none",
    "pdf.fonttype": 42,
})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--china-shapefile", type=Path, required=True)
    return parser.parse_args()


def save_publication_figure(fig, output_dir: Path, filename: str) -> None:
    fig.savefig(output_dir / f"{filename}.png", dpi=600)
    fig.savefig(output_dir / f"{filename}.tiff", dpi=600)
    fig.savefig(output_dir / f"{filename}.pdf")
    fig.savefig(output_dir / f"{filename}.svg")


def plot_component_maps(
    grid: pd.DataFrame, output_dir: Path, china_shapefile: Path
) -> None:
    china = gpd.read_file(china_shapefile).to_crs("EPSG:4326")
    components = [
        ("Delta_Total", "Total scenario contrast", "Figure5_DeltaTotal"),
        ("Delta_CC", "Climate scenario contrast", "Figure6_DeltaCC"),
        ("Delta_HA", "Aggregate human-activity scenario contrast", "Figure7_DeltaHA"),
    ]
    for component, label, filename in components:
        values = grid[f"{component}_mean"].to_numpy()
        finite = np.abs(values[np.isfinite(values)])
        limit = float(np.quantile(finite, 0.98)) if len(finite) else 1.0
        limit = max(limit, 0.05)
        norm = TwoSlopeNorm(vmin=-limit, vcenter=0.0, vmax=limit)
        fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.35), sharex=True, sharey=True)
        image = None
        for panel, (axis, event) in enumerate(zip(axes, ("Flood", "Drought"))):
            data = grid[grid["Event_Type"] == event]
            image = axis.scatter(
                data["Lon"], data["Lat"], c=data[f"{component}_mean"],
                cmap="RdBu_r", norm=norm, marker="s", s=7.5,
                linewidths=0, rasterized=True,
            )
            agreeing_models = data[f"{component}_Sign_Agreement"] * data[f"{component}_count"]
            robust = agreeing_models >= 5.0
            axis.scatter(
                data.loc[robust, "Lon"], data.loc[robust, "Lat"],
                s=0.55, c="#202020", linewidths=0, alpha=0.65, rasterized=True,
            )
            china.boundary.plot(ax=axis, color="#303030", linewidth=0.55)
            axis.set_xlim(72.5, 135.5)
            axis.set_ylim(17.5, 54.0)
            axis.set_aspect("equal")
            axis.set_title(event, fontsize=9, pad=4)
            axis.text(0.015, 0.985, f"({chr(97 + panel)})", transform=axis.transAxes,
                      ha="left", va="top", fontsize=9, fontweight="bold")
        axes[0].set_ylabel("Latitude (degrees N)")
        fig.suptitle(f"{label} (common factual SRI reference)", fontsize=10, y=0.985)
        fig.text(0.5, 0.205, "Longitude (degrees E)", ha="center", va="center")
        colorbar_axis = fig.add_axes([0.34, 0.105, 0.32, 0.035])
        colorbar = fig.colorbar(image, cax=colorbar_axis, orientation="horizontal")
        colorbar.set_label("Change in frequency ratio")
        fig.subplots_adjust(left=0.07, right=0.99, top=0.87, bottom=0.27, wspace=0.08)
        save_publication_figure(fig, output_dir, filename)
        plt.close(fig)


def plot_drought_sensitivity(data_dir: Path, output_dir: Path) -> None:
    summary = pd.read_csv(data_dir / "drought_sensitivity_summary.csv")
    models = pd.read_csv(data_dir / "drought_sensitivity_by_model.csv")
    order = ["obsclim-histsoc", "countclim-histsoc", "countclim-1901soc"]
    labels = [
        "Factual\n(obsclim-histsoc)",
        "Counterfactual Climate\n(counterclim-histsoc)",
        "Natural Baseline\n(counterclim-1901soc)",
    ]
    colors = ["#3D6D9A", "#C65D4B", "#5C8A5B"]
    baseline = []
    lower = []
    upper = []
    points = []
    for scenario in order:
        subset = summary[summary["Scenario"] == scenario]
        centre = float(subset.loc[subset["Definition"] == "T05_D20", "mean"].iloc[0])
        baseline.append(centre)
        lower.append(centre - float(subset["mean"].min()))
        upper.append(float(subset["mean"].max()) - centre)
        points.append(
            models[(models["Scenario"] == scenario) & (models["Definition"] == "T05_D20")]["Percent_FR_GT_1"].to_numpy()
        )

    x = np.arange(3)
    fig, axis = plt.subplots(figsize=(7.0, 4.1))
    axis.bar(x, baseline, color=colors, width=0.58, edgecolor="#202020", linewidth=0.7, zorder=2)
    axis.errorbar(x, baseline, yerr=np.array([lower, upper]), fmt="none", ecolor="#202020",
                  elinewidth=1.2, capsize=5, capthick=1.2, zorder=4)
    offsets = np.linspace(-0.16, 0.16, 7)
    for index, values in enumerate(points):
        axis.scatter(np.full(len(values), x[index]) + offsets[:len(values)], values,
                     s=17, facecolor="white", edgecolor="#202020", linewidth=0.65, zorder=5)
    for index, value in enumerate(baseline):
        label_height = max(value + upper[index], float(np.max(points[index]))) + 0.8
        axis.text(index, label_height, f"{value:.1f}%", ha="center", va="bottom", fontsize=9)
    axis.set_xticks(x, labels)
    axis.set_ylabel("Grid Cells with Increased Drought Frequency (%)")
    axis.set_title("Sensitivity of Drought Intensification Extent to Alternative Event Definitions", pad=12)
    axis.grid(axis="y", color="#D9D9D9", linewidth=0.6, zorder=0)
    axis.spines[["top", "right"]].set_visible(False)
    axis.set_ylim(0, max(max(map(np.max, points)), max(np.array(baseline) + np.array(upper))) + 4)
    fig.subplots_adjust(left=0.11, right=0.99, top=0.88, bottom=0.23)
    save_publication_figure(fig, output_dir, "FigureS_DroughtSensitivity")
    plt.close(fig)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    grid = pd.read_csv(args.data_dir / "grid_ensemble_attribution.csv")
    plot_component_maps(grid, args.output_dir, args.china_shapefile)
    plot_drought_sensitivity(args.data_dir, args.output_dir)


if __name__ == "__main__":
    main()

