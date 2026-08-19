# First-batch reproducibility report

## Scope

This batch changed only reproducibility interfaces, document builders, validation, and release packaging. It did not change SRI fitting, event detection, frequency-ratio rules, scenario contrasts, basin aggregation, model weighting, numerical results, figures, equations, citations, or Table 1 values. The complete 21 model-scenario NetCDF workflow was not rerun because no numerical logic changed.

## Changes

1. Removed machine-specific absolute data paths from analysis code.
2. Added explicit `--raw-root`, `--china-shapefile`, and `--basin-shapefile` arguments and propagated them through the top-level runners.
3. Synchronized the manuscript and supplementary-information builders with the canonical v2 title, scenario-contrast terminology, duplicate-paragraph removal, and final availability language.
4. Added a minimal direct-dependency specification in `requirements.txt`.
5. Updated `README.md` with portable commands and explicit input contracts.
6. Updated deliverable validation for the canonical v2 title, stale/forbidden phrases, EndNote fields, page counts, Table 1, Table S5, figures, and equations.

## Verification

- Core unit tests: 8 passed, 0 failed.
- Scenario-specific smoke test: H08 `obsclim-histsoc`, 2 mainland grid cells, 12 NetCDF files, passed.
- Common-reference smoke test: H08, all three scenarios, 2 mainland grid cells, passed.
- Smoke CSV regression: all four generated CSV files were byte-for-byte identical to the previously verified smoke outputs.
- Temporary manuscript build: 29 PDF pages; paragraph and Table 1 text matched the canonical v2 manuscript.
- Temporary supplementary build: 4 PDF pages; paragraph and table text matched the canonical v2 supplementary information.
- EndNote preservation: 32 citation fields and 1 bibliography field.
- Table 1: five basin rows complete on PDF page 21 and values matched the basin summary CSV.
- Table S5: drought and flood rows complete on supplementary PDF page 4.
- Figures 1-7 and equations (1)-(8): detected in the rendered PDF.
- Deliverable validator: PASS.

## Remaining external item

The public repository name and persistent identifier are not assigned. Repository publication was outside this batch.
