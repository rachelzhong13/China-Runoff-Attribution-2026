# Recomputed attribution method specification

Status: frozen for the final Q1 analysis. Existing archived outputs are not overwritten.

## Spatial and temporal domain

- Use grid-cell centres within `China_boundary_dis_mainland.shp`.
- Preserve each model's native NetCDF calendar.
- Use 1950-1984 as Period 1 and 1985-2019 as Period 2.
- Identify events before model aggregation, so daily values are not aligned across models.

## Daily SRI

For each model and grid cell, using the factual `obsclim-histsoc` series as the primary calibration reference:

1. Calculate a backward-looking 30-day arithmetic mean of daily `qtot`.
2. Use factual `obsclim-histsoc` runoff from 1950-2019 as the calibration period, then apply the fitted transformation unchanged to all three scenarios for that model-grid.
3. Group the 30-day series by calendar month. This replaces the invalid repeating 12-observation grouping produced by applying SCI 1.0-3 directly to daily data.
4. Divide positive calibration values by their monthly median for numerical fitting, then fit lognormal, gamma, Gumbel, and Weibull distributions separately for each month. Apply each fitted CDF on the same scaled axis and select the finite fit with the lowest AIC. This positive scale change does not alter the candidate ranking.
5. Estimate the zero-runoff mass from the calibration sample. Transform zero values at the centre of that mass and positive values with the mixed CDF.
6. Transform mixed-CDF probabilities to standard-normal scores. Bound only exact numerical tail probabilities using the monthly calibration sample size.
7. Record failed monthly fits and missing SRI values. If any calendar-month fit fails or the 1950-2019 SRI series has an internal missing value, retain the model-grid QC row but set its event counts, FR, and sensitivity metrics to missing. JULES-W2 has a source-data missing value at the terminal date, 2019-12-31, across the mainland domain. This terminal day remains missing and is not imputed, but it does not invalidate an otherwise complete model-grid series. Do not substitute random values or silently choose parameters from a different distribution.

Primary fits are model-, grid-, and month-specific and are estimated only from the factual scenario. The same fitted family, parameters, zero-flow probability, and scale are applied to `obsclim-histsoc`, `counterclim-histsoc`, and `counterclim-1901soc`. The analysis does not use the archived Shapiro-Wilk screening because that screen was applied to randomly filled values and was not a valid distribution comparison.

## Events and frequency ratios

- Drought: SRI <= -0.5 for at least 20 consecutive native-calendar days.
- Flood: SRI >= 0.5 starts an event. Pulses separated by fewer than 20 non-flood days remain one event.
- Primary FR: `(N_P2 + 0.5) / (N_P1 + 0.5)`.
- Zero-rule sensitivity: ordinary `N_P2 / N_P1` only when `N_P1 > 0`; all zero-denominator cases are undefined.
- There is no near-zero threshold because event counts are integers.

Sensitivity analyses vary the drought threshold (-0.5, -0.8, -1.0), minimum
duration (10, 20, 30 days), flood threshold (0.5, 0.8, 1.0), flood reset gap
(10, 20, 30 days), and period split (1980, 1985, 1990). The primary common
factual reference preserves between-scenario distribution shifts. A secondary
calibration sensitivity instead fits each model-scenario-grid series separately
to quantify how scenario-specific standardisation changes the results.

## Model aggregation

- Identify events and calculate FR separately for every model.
- Form DeltaHA, DeltaCC, and DeltaTotal as paired within-model scenario contrasts.
- Aggregate paired model results with equal model weight.
- Retain model spread and sign agreement with the ensemble-mean contrast.
- Recalculate pooled basin DIndex after omitting each model in turn.
- DeltaHA remains an aggregate human-activity scenario contrast.


