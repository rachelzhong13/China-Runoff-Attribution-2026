# ISIMIP3a flood and drought frequency scenario-contrast workflow

This bundle contains the analysis and document-build workflow used for the Q1 revision. It does not overwrite raw NetCDF inputs or archived SRI/event files.

## Inputs

The commands require three paths supplied at run time:

- `<RAW_ROOT>`: directory containing `<scenario>/<model>/*.nc`; each of the 21 model-scenario directories contains 12 regional daily `qtot` NetCDF files.
- `<CHINA_SHP>`: mainland-China polygon shapefile used to select grid-cell centres.
- `<BASIN_SHP>`: basin polygon shapefile containing the `Basin_Name` field.

The raw simulations and shapefiles are not redistributed in this bundle.

## Environment

Python 3.12 was used. Install the direct dependencies in `requirements.txt`:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Frozen method

See `METHOD_SPEC.md`. The primary workflow uses native NetCDF calendars, a backward 30-day runoff mean, 1950-2019 factual monthly calibration, explicit zero mass, event detection within each model, paired within-model scenario contrasts, equal model weights, and a mainland-China mask. Scenario-specific calibration is retained as a sensitivity analysis.

`Delta_HA` is an aggregate human-activity scenario contrast. It does not isolate reservoirs, irrigation, land use, abstraction, or groundwater. Reservoir intensity is diagnostic evidence only.

## Verification

Run the eight core unit tests:

```powershell
python -m unittest -v test_sri_core.py
```

Run a limited single-scenario smoke calculation:

```powershell
python recompute_model_events.py --scenario obsclim-histsoc --model h08 --max-cells 2 --workers 1 --raw-root <RAW_ROOT> --china-shapefile <CHINA_SHP> --output-dir outputs\smoke_scenario
```

Run a limited common-reference smoke calculation for all three scenarios:

```powershell
python recompute_common_reference_events.py --model h08 --max-cells 2 --workers 1 --raw-root <RAW_ROOT> --china-shapefile <CHINA_SHP> --output-dir outputs\smoke_common
```

## Full analysis

The following command runs both calibration workflows, validates model outputs, aggregates scenario contrasts, compares calibration schemes, and exports figures:

```powershell
python run_q1_revision_analysis.py --workers 4 --raw-root <RAW_ROOT> --china-shapefile <CHINA_SHP> --basin-shapefile <BASIN_SHP>
```

The existing model-event outputs can be aggregated and plotted without rerunning NetCDF calculations:

```powershell
python run_postprocessing.py --china-shapefile <CHINA_SHP> --basin-shapefile <BASIN_SHP>
```

## Document build

`<SOURCE_DOCX>` is the pre-Q1 recomputed manuscript template containing the paragraph anchors expected by the builder. It is not the original August manuscript and is not redistributed in this analysis bundle. The document builder is not required to reproduce the numerical outputs.

Build the manuscript from that template, final figures, and basin summary:

```powershell
python build_q1_manuscript.py <SOURCE_DOCX> <OUTPUT_DOCX> --figure-dir outputs\figures\common_reference --basin-csv outputs\common_reference_aggregated\table1_basin_summary.csv --report outputs\validation\manuscript_build.json
```

Build the supplementary information:

```powershell
python build_supplementary.py <OUTPUT_SUPPLEMENT_DOCX> --analysis-dir outputs
```

After exporting both DOCX files to PDF with Microsoft Word, validate all deliverables:

```powershell
python validate_q1_deliverables.py --source <SOURCE_DOCX> --main <OUTPUT_DOCX> --supplement <OUTPUT_SUPPLEMENT_DOCX> --main-pdf <OUTPUT_PDF> --supplement-pdf <OUTPUT_SUPPLEMENT_PDF> --figure-dir outputs\figures\common_reference --basin-csv outputs\common_reference_aggregated\table1_basin_summary.csv
```

## Main outputs

- `outputs/common_reference_events`: primary common-reference model-scenario event counts, FR, QC, and sensitivity metrics.
- `outputs/common_reference_aggregated`: model, grid, basin, zero-rule, event-definition, period, and leave-one-model-out summaries.
- `outputs/model_events` and `outputs/aggregated`: scenario-specific calibration sensitivity outputs.
- `outputs/calibration_comparison`: direct calibration-scheme comparisons.
- `outputs/validation`: structural and numerical validation reports.
- `outputs/figures/common_reference`: publication figures in PNG, PDF, SVG, and TIFF formats.
