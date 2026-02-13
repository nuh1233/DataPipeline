# ETL Pipeline

General-purpose ETL (Extract, Transform, Load) pipeline for tabular data. It extracts datasets from
common formats, applies configurable transformations, and loads cleaned outputs to CSV or Parquet.
The current example configuration uses NYC buildings data, but the pipeline is reusable for any
tabular dataset.

## Setup

```bat
cd big-data\data_pipeline
python -m venv ..\venv
..\venv\Scripts\python -m pip install -r ..\requirements.txt
```

## Run

```bat
cd big-data\data_pipeline
..\venv\Scripts\python main.py list
..\venv\Scripts\python main.py nyc_buildings
```

## What It Does (ETL)

- Extract: Auto-detects input format (CSV, Parquet, JSON, Excel)
- Transform: Filters or keeps rows by column values
- Transform: Sorts by a custom category order
- Transform: Creates clusters/sub-clusters for basic grouping statistics
- Load: Writes outputs in CSV or Parquet with optional compression

## Notes

- The pipeline expects `Nyc_data.csv` to be present in this folder.
- Output files are written under `output/`.
