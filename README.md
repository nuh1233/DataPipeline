# DataPipeline

A flexible, configuration-driven ETL (Extract, Transform, Load) pipeline for processing tabular data. Built with Python, this pipeline supports multiple file formats and provides powerful data transformation capabilities with minimal code changes.

## ✨ Features

- **Multi-format Support**: Auto-detects and processes CSV, Parquet, JSON, and Excel files
- **Configuration-Driven**: Define multiple pipeline configurations via JSON without modifying code
- **Flexible Transformations**:
  - Filter or keep rows based on column values
  - Custom sorting with user-defined category orders
  - Clustering and grouping with statistical summaries
- **Multiple Output Formats**: Export to CSV or Parquet with optional compression (snappy, gzip)
- **Statistics Generation**: Optional per-cluster and sub-cluster statistics

## 📋 Prerequisites

- Python 3.7+
- pip (Python package installer)

## 🚀 Quick Start

### Installation

```bat
cd big-data\data_pipeline
python -m venv ..\venv
..\venv\Scripts\python -m pip install -r ..\requirements.txt
```

### Usage

List all available pipeline configurations:
```bat
cd big-data\data_pipeline
..\venv\Scripts\python main.py list
```

Run a specific pipeline configuration:
```bat
..\venv\Scripts\python main.py nyc_buildings
```

## 📁 Project Structure

```
DataPipeline/
├── main.py                  # Entry point and CLI
├── data_processing.py       # Core ETL logic
├── datasets_config.json     # Pipeline configurations
├── Nyc_data.csv            # Sample input data
├── output/                 # Generated output files
└── README.md
```

## ⚙️ Configuration

Pipeline configurations are defined in `datasets_config.json`. Each configuration supports:

| Parameter | Description | Required |
|-----------|-------------|----------|
| `input_file` | Source data file path | ✓ |
| `output_file` | Output file name (CSV or Parquet) | ✓ |
| `output_dir` | Output directory path | ✓ |
| `filter_column` | Column to filter rows from | |
| `filter_values` | Values to exclude | |
| `keep_column` | Column to keep specific rows | |
| `keep_values` | Values to retain | |
| `primary_column` | Main grouping column | |
| `sub_columns` | Sub-grouping columns | |
| `sort_order` | Custom category order | |
| `compression` | Compression type (snappy, gzip) | |
| `show_stats` | Generate statistics (true/false) | |

### Example Configuration

```json
{
  "manhattan_only": {
    "input_file": "Nyc_data.csv",
    "output_file": "Manhattan_buildings.parquet",
    "output_dir": "output/nyc_green_data",
    "compression": "gzip",
    "keep_column": "City",
    "keep_values": ["Manhattan"],
    "primary_column": "Largest Property Use Type",
    "show_stats": true
  }
}
```

## 📊 Example Pipelines

The repository includes several pre-configured pipelines for NYC buildings data:

- `nyc_buildings`: Filter parking properties with custom city sorting
- `nyc_no_parking_storage`: Exclude parking and storage properties
- `nyc_multifamily_only`: Keep only multifamily housing and offices
- `property_types_parquet`: Export specific property types to compressed Parquet
- `manhattan_only`: Extract Manhattan buildings with statistics

## 🔧 ETL Process Flow

```
Extract → Transform → Load
   ↓          ↓          ↓
Auto-detect → Filter   → CSV/Parquet
  format   → Sort      → with optional
           → Cluster   → compression
           → Stats
```

## 📝 Notes

- Place your input data file (`Nyc_data.csv` or custom files) in the project root
- Output files are automatically created in the specified `output_dir`
- The pipeline creates directories if they don't exist

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the issues page.

## 👤 Author

**nuh1233**

- GitHub: [@nuh1233](https://github.com/nuh1233)
