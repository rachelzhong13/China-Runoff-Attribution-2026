from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "figures" / "common_reference"

mpl.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans", "sans-serif"],
    "font.size": 7,
    "svg.fonttype": "none",
    "pdf.fonttype": 42,
})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def add_box(axis, x, y, width, height, title, lines, color):
    box = FancyBboxPatch(
        (x, y), width, height,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.1, edgecolor=color, facecolor="#F8FAFC",
    )
    axis.add_patch(box)
    axis.text(
        x + 0.02 * width, y + height - 0.10 * height, title,
        color=color, fontsize=6.6, fontweight="bold", va="top",
    )
    axis.plot(
        [x + 0.04 * width, x + 0.96 * width],
        [y + height - 0.24 * height] * 2,
        color="#D6DEE6", linewidth=0.7,
    )
    axis.text(
        x + width / 2, y + 0.39 * height, "\n".join(lines),
        color="#26323C", fontsize=5.35, ha="center", va="center",
        linespacing=1.28,
    )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    fig, axis = plt.subplots(figsize=(7.2, 4.15))
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")

    blue = "#2F6F9F"
    teal = "#3D7F73"
    green = "#557F55"
    ochre = "#A66A1F"
    magenta = "#9B4669"
    colors = [blue, teal, green, ochre, magenta]
    titles = [
        "1  Inputs", "2  Common SRI reference", "3  Events within models",
        "4  Frequency change", "5  Paired attribution",
    ]
    contents = [
        ["Daily qtot", "7 GHMs × 3 scenarios", "1950–2019; native calendars"],
        ["30-day backward mean", "Fit 12 monthly distributions", "to factual obsclim-histsoc", "per model and grid cell"],
        ["Use the same 12 fits", "for all three scenarios", "Detect events separately in", "each model–scenario", "No model-mean SRI"],
        ["Drought: SRI ≤ −0.5; ≥20 d", "Flood: SRI ≥ 0.5; reset 20 d", "P1: 1950–1984; P2: 1985–2019", r"FR = (N$_2$ + 0.5)/(N$_1$ + 0.5)"],
        ["ΔHA = FRhist − FR1901", "ΔCC = FRobs − FRhist", "ΔTotal = ΔCC + ΔHA", "Basin DIndex from |Δ|"],
    ]
    x_positions = [0.02, 0.218, 0.416, 0.614, 0.812]
    width = 0.168
    y = 0.48
    height = 0.45
    for index, (x, title, lines, color) in enumerate(
        zip(x_positions, titles, contents, colors)
    ):
        add_box(axis, x, y, width, height, title, lines, color)
        if index < 4:
            axis.add_patch(FancyArrowPatch(
                (x + width + 0.006, y + height / 2),
                (x_positions[index + 1] - 0.006, y + height / 2),
                arrowstyle="-|>", mutation_scale=10, linewidth=1.0,
                color="#56636F",
            ))

    lower = FancyBboxPatch(
        (0.02, 0.08), 0.96, 0.28,
        boxstyle="round,pad=0.012,rounding_size=0.018",
        linewidth=1.0, edgecolor="#6B7884", facecolor="#F4F6F8",
    )
    axis.add_patch(lower)
    axis.text(
        0.04, 0.32, "QUALITY CONTROL AND ROBUSTNESS",
        fontsize=7.2, fontweight="bold", color="#56636F", va="top",
    )
    columns = [
        (0.13, "QC", ["Exclude failed monthly fits", "or internal SRI gaps", "retain every grid-level QC row"]),
        (0.37, "Calibration", ["Primary: factual common reference", "Sensitivity: scenario-specific fits", "compare grid and basin results"]),
        (0.62, "Definitions", ["Thresholds: 0.5, 0.8, 1.0", "duration/reset: 10, 20, 30 d", "split years: 1980, 1985, 1990"]),
        (0.86, "Aggregation", ["paired contrasts within model", "cos(latitude) basin weighting", "equal model weights and LOMO"]),
    ]
    for x, heading, lines in columns:
        axis.text(x, 0.245, heading, ha="center", va="top", fontsize=6.7,
                  fontweight="bold", color=blue)
        axis.text(x, 0.205, "\n".join(lines), ha="center", va="top",
                  fontsize=5.8, color="#26323C", linespacing=1.25)
    for x in (0.25, 0.50, 0.75):
        axis.plot([x, x], [0.105, 0.29], color="#D6DEE6", linewidth=0.7)

    axis.text(
        0.5, 0.965,
        "Common-reference workflow for runoff-extreme frequency attribution",
        ha="center", va="top", fontsize=9, fontweight="bold", color="#26323C",
    )
    fig.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
    stem = args.output_dir / "Figure4_CommonReferenceWorkflow"
    fig.savefig(stem.with_suffix(".svg"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), dpi=600, bbox_inches="tight")
    fig.savefig(stem.with_suffix(".tiff"), dpi=600, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
