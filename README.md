# China Runoff Attribution 2026

Reproducibility materials for:

**Climate and Aggregate Human-Activity Scenario Contrasts in Flood and Drought Event Frequencies across China (1950–2019): An ISIMIP3a Multi-Model Analysis**

## Scope

The study evaluates flood- and drought-event frequency changes over mainland China using seven ISIMIP3a global hydrological models and three model scenarios. Events are detected separately within each model-scenario series before paired scenario contrasts and equal-weight model aggregation.

The primary workflow uses a common factual monthly calibration for the 30-day Standardized Runoff Index. It retains model spread, valid-model counts, sign agreement, event-definition sensitivity, alternative period splits, a zero-event sensitivity rule, leave-one-model-out basin summaries, and a scenario-specific calibration sensitivity analysis.

`Delta_HA` is an aggregate human-activity scenario contrast. It does not isolate reservoirs, irrigation, land use, abstraction, or groundwater. Reservoir intensity is used only as basin-scale diagnostic evidence.

## Reproducibility package

The validated workflow is in [`reproducibility/`](reproducibility/):

- [`reproducibility/README.md`](reproducibility/README.md): input contract and execution commands.
- [`reproducibility/METHOD_SPEC.md`](reproducibility/METHOD_SPEC.md): frozen computational method.
- [`reproducibility/requirements.txt`](reproducibility/requirements.txt): direct Python dependencies.
- [`reproducibility/outputs/common_reference_aggregated/`](reproducibility/outputs/common_reference_aggregated/): primary aggregate results.
- [`reproducibility/outputs/calibration_comparison/`](reproducibility/outputs/calibration_comparison/): calibration-scheme comparison.
- [`reproducibility/outputs/validation/`](reproducibility/outputs/validation/): automated validation reports.
- [`reproducibility/outputs/figures/common_reference/`](reproducibility/outputs/figures/common_reference/): publication figures in PNG, PDF, and SVG formats.

Raw ISIMIP NetCDF files and boundary shapefiles are not redistributed. Their locations are supplied at run time through explicit command-line arguments.

## Verification status

- Eight core unit tests passed.
- Limited H08 scenario-specific and common-reference smoke calculations passed.
- Smoke outputs matched the previously verified results.
- The deliverable validator reports `PASS`.
- No full 21-combination NetCDF rerun was performed during packaging because numerical logic was unchanged.

## Legacy material

[`legacy/`](legacy/) contains files uploaded before the validated Q1 workflow was assembled. They are retained only for provenance and are not used to reproduce the reported results. Their methods, paths, periods, and terminology may differ from the final workflow.
