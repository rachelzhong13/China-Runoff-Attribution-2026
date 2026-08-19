from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np
from scipy import stats


CANDIDATES = {
    "lognormal": stats.lognorm,
    "gamma": stats.gamma,
    "gumbel": stats.gumbel_r,
    "weibull": stats.weibull_min,
}


@dataclass(frozen=True)
class SriResult:
    values: np.ndarray
    selected_distributions: tuple[str | None, ...]
    failed_months: int
    negative_input_count: int


@dataclass(frozen=True)
class MonthlySriFit:
    distribution: str
    parameters: tuple[float, ...]
    zero_probability: float
    fit_scale: float
    sample_size: int


def backward_running_mean(values: np.ndarray, window: int = 30) -> np.ndarray:
    """Return a full-window backward mean; missing values invalidate a window."""
    x = np.asarray(values, dtype=np.float64)
    if x.ndim != 1:
        raise ValueError("values must be one-dimensional")
    if window < 1:
        raise ValueError("window must be positive")

    valid = np.isfinite(x)
    sums = np.cumsum(np.where(valid, x, 0.0), dtype=np.float64)
    counts = np.cumsum(valid, dtype=np.int64)
    sums = np.concatenate(([0.0], sums))
    counts = np.concatenate(([0], counts))

    out = np.full(x.shape, np.nan, dtype=np.float64)
    if len(x) < window:
        return out
    window_sums = sums[window:] - sums[:-window]
    window_counts = counts[window:] - counts[:-window]
    positions = np.arange(window - 1, len(x))
    complete = window_counts == window
    out[positions[complete]] = window_sums[complete] / window
    return out


def _fit_candidate(name: str, positive: np.ndarray) -> tuple[tuple[float, ...], float]:
    distribution = CANDIDATES[name]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if name in {"lognormal", "gamma", "weibull"}:
            params = distribution.fit(positive, floc=0.0)
        else:
            params = distribution.fit(positive)
        log_pdf = distribution.logpdf(positive, *params)

    if not np.all(np.isfinite(params)) or not np.all(np.isfinite(log_pdf)):
        raise ValueError("non-finite fit")
    # All candidates have two free continuous-distribution parameters.
    aic = 4.0 - 2.0 * float(log_pdf.sum())
    return tuple(float(value) for value in params), aic


def fit_monthly_calibration(
    running_mean: np.ndarray,
    years: np.ndarray,
    months: np.ndarray,
    calibration_start: int = 1950,
    calibration_end: int = 2019,
    minimum_positive: int = 30,
) -> tuple[MonthlySriFit | None, ...]:
    """Fit one mixed runoff distribution for each calendar month."""
    x = np.asarray(running_mean, dtype=np.float64)
    years = np.asarray(years)
    months = np.asarray(months)
    if x.shape != years.shape or x.shape != months.shape:
        raise ValueError("running_mean, years, and months must have equal shapes")

    fitted: list[MonthlySriFit | None] = []
    calibration_year = (years >= calibration_start) & (years <= calibration_end)

    for month in range(1, 13):
        target = (months == month) & np.isfinite(x) & (x >= 0.0)
        calibration = target & calibration_year
        sample = x[calibration]
        positive = sample[sample > 0.0]
        if len(positive) < minimum_positive:
            fitted.append(None)
            continue

        zero_probability = float(np.count_nonzero(sample == 0.0) / len(sample))
        fit_scale = float(np.median(positive))
        if not np.isfinite(fit_scale) or fit_scale <= 0.0:
            fitted.append(None)
            continue
        scaled_positive = positive / fit_scale
        best_name = None
        best_params = None
        best_aic = np.inf
        for name in CANDIDATES:
            try:
                params, continuous_aic = _fit_candidate(name, scaled_positive)
                if zero_probability > 0.0:
                    log_mass = (
                        np.count_nonzero(sample == 0.0) * np.log(zero_probability)
                        + len(positive) * np.log1p(-zero_probability)
                    )
                else:
                    log_mass = 0.0
                aic = continuous_aic - 2.0 * float(log_mass)
                if aic < best_aic:
                    best_name = name
                    best_params = params
                    best_aic = aic
            except (ValueError, RuntimeError, FloatingPointError, OverflowError):
                continue

        if best_name is None or best_params is None:
            fitted.append(None)
            continue

        fitted.append(MonthlySriFit(
            distribution=best_name,
            parameters=best_params,
            zero_probability=zero_probability,
            fit_scale=fit_scale,
            sample_size=len(sample),
        ))

    return tuple(fitted)


def transform_monthly_sri(
    running_mean: np.ndarray,
    months: np.ndarray,
    calibration: tuple[MonthlySriFit | None, ...],
) -> SriResult:
    """Transform runoff using a supplied twelve-month calibration."""
    x = np.asarray(running_mean, dtype=np.float64)
    months = np.asarray(months)
    if x.shape != months.shape:
        raise ValueError("running_mean and months must have equal shapes")
    if len(calibration) != 12:
        raise ValueError("calibration must contain twelve monthly entries")

    sri = np.full(x.shape, np.nan, dtype=np.float64)
    negative_count = int(np.count_nonzero(np.isfinite(x) & (x < 0.0)))
    selected: list[str | None] = []

    for month, fit in enumerate(calibration, start=1):
        selected.append(fit.distribution if fit is not None else None)
        if fit is None:
            continue

        target = (months == month) & np.isfinite(x) & (x >= 0.0)
        values = x[target]
        if len(values) == 0:
            continue
        probability = np.empty(values.shape, dtype=np.float64)
        zeros = values == 0.0
        if np.any(zeros):
            probability[zeros] = fit.zero_probability / 2.0
        if np.any(~zeros):
            continuous_cdf = CANDIDATES[fit.distribution].cdf(
                values[~zeros] / fit.fit_scale, *fit.parameters
            )
            probability[~zeros] = fit.zero_probability + (
                1.0 - fit.zero_probability
            ) * continuous_cdf

        tail_limit = 0.5 / (fit.sample_size + 1.0)
        probability = np.clip(probability, tail_limit, 1.0 - tail_limit)
        sri[target] = stats.norm.ppf(probability)

    return SriResult(
        values=sri,
        selected_distributions=tuple(selected),
        failed_months=sum(name is None for name in selected),
        negative_input_count=negative_count,
    )


def fit_monthly_sri(
    running_mean: np.ndarray,
    years: np.ndarray,
    months: np.ndarray,
    calibration_start: int = 1950,
    calibration_end: int = 2019,
    minimum_positive: int = 30,
) -> SriResult:
    """Fit and apply model-scenario-specific monthly SRI distributions."""
    calibration = fit_monthly_calibration(
        running_mean,
        years,
        months,
        calibration_start=calibration_start,
        calibration_end=calibration_end,
        minimum_positive=minimum_positive,
    )
    return transform_monthly_sri(running_mean, months, calibration)


def count_drought_events(
    values: np.ndarray, threshold: float = -0.5, minimum_duration: int = 20
) -> int:
    drought = np.isfinite(values) & (np.asarray(values) <= threshold)
    padded = np.concatenate(([False], drought, [False]))
    transitions = np.diff(padded.astype(np.int8))
    starts = np.flatnonzero(transitions == 1)
    ends = np.flatnonzero(transitions == -1)
    return int(np.count_nonzero((ends - starts) >= minimum_duration))


def count_flood_events(
    values: np.ndarray, threshold: float = 0.5, reset_gap: int = 20
) -> int:
    flood = np.isfinite(values) & (np.asarray(values) >= threshold)
    in_event = False
    gap = 0
    count = 0
    for is_flood in flood:
        if is_flood:
            if not in_event:
                in_event = True
                count += 1
            gap = 0
        elif in_event:
            gap += 1
            if gap >= reset_gap:
                in_event = False
                gap = 0
    return count


def corrected_frequency_ratio(recent: int, early: int) -> float:
    return (recent + 0.5) / (early + 0.5)


def corrected_rate_ratio(
    recent: int, early: int, recent_years: int, early_years: int
) -> float:
    return ((recent + 0.5) / recent_years) / ((early + 0.5) / early_years)


def undefined_zero_frequency_ratio(recent: int, early: int) -> float:
    if early == 0:
        return np.nan
    return recent / early


