# Attribution Analysis of Runoff Extremes in China (1980–2014)

## Project Overview
This repository contains the Python-based analytical pipeline for a research project investigating the impacts of **Climate Change (CC)** and **Human Activities (HA)** (specifically land-use change and reservoir dynamics) on runoff variability and hydrological extremes in China.

By utilizing multi-model ensemble data from the **ISIMIP3b** (Inter-Sectoral Impact Model Intercomparison Project phase 3b), this project quantifies the contribution of different drivers to the frequency changes of droughts and floods across 7,971 grid points in China.

## Core Workflow & Scripts

The pipeline is organized into four logical stages. Each script is designed to handle large-scale geospatial data efficiently.

### 1. Data Pre-processing
* `calculate_means.py`: Performs Multi-Model Ensemble (MME) averaging across 7 hydrological models (h08, hydropy, jules-w2, lpjml, miroc-integ, watergap2, web-dhm-sg).
* `grind.py`: Handles batch processing and spatial slicing of NetCDF/CSV datasets.

### 2. Frequency Analysis
* `calculate_frequency.py`: Identifies extreme events based on the threshold-exceedance method.
    * **Flood Threshold**: $Q_{95}$ or $Q_{1.0}$ (Standard Deviation).
    * **Drought Threshold**: $Q_{10}$ or $-1.0$ (Standard Deviation).

### 3. Attribution Logic
* `run_final_attribution.py`: The core analytical engine that isolates drivers using the Delta method:
    * **Climate Change Impact ($\Delta CC$):** $S_{obs} - S_{count\_hist}$
    * **Human Activity Impact ($\Delta HA$):** $S_{count\_hist} - S_{1901soc}$

### 4. Visualization
* `plot_FINAL_attribution_maps.py`: Generates high-quality, interactive spatial maps of attribution results using Plotly and GeoPandas.

## Methodology Summary

The study utilizes three primary simulation scenarios to decouple impacts:
1. **Factual ($S_{obs}$):** Observed climate + Historical socio-economics.
2. **Counterfactual Climate ($S_{count\_hist}$):** Detrended climate + Historical socio-economics.
3. **Natural Baseline ($S_{1901soc}$):** Detrended climate + 1901 socio-economic conditions (pre-industrial/pre-dam).

The **Risk Ratio (RR)** is defined as:
$$RR = \frac{Frequency_{factual}}{Frequency_{counterfactual}}$$

## Installation & Requirements

To run these scripts, you need a Python 3.9+ environment with the following dependencies:
* `xarray` & `netCDF4`: For multidimensional climate data.
* `pandas` & `numpy`: For statistical processing.
* `geopandas` & `shapely`: For geographic masking.
* `plotly` & `matplotlib`: For visualization.

```bash
pip install xarray pandas geopandas plotly matplotlib
