# Snowmeta Pipeline Framework

A metadata-driven ingestion framework for Snowflake that leverages native capabilities including Snowpipe, Dynamic Tables, Tasks, and Stored Procedures to automate data ingestion and transformation across Bronze, Silver, and Gold layers.

## 🎯 Overview

Snowmeta Pipeline is a comprehensive framework designed to be **flexible**, **scalable**, and **observable**, providing:

- ✅ Metadata-driven configuration
- ✅ Bronze layer ingestion with automatic schema inference
- ✅ Silver layer transformations (SCD Type 1 & Type 2)
- ✅ Data quality checks and CDC handling
- ✅ Environment promotion capabilities
- ✅ Minimal operational overhead

## 🚀 Features

### Bronze Layer Ingestion
- **Automatic Schema Inference**: Uses Snowflake's `INFER_SCHEMA` function
- **Multiple File Formats**: Support for CSV, JSON, Parquet, and more
- **Unified Stored Procedures**: Single procedure for all tables
- **Task Automation**: Automated execution with Snowflake Tasks

### Silver Layer Transformations

#### SCD Type 1 (Dynamic Tables)
- Latest record only (overwrites)
- Automatic deduplication
- Low maintenance with Dynamic Tables

#### SCD Type 2 (Stored Procedures + Tasks) 🆕
- **Full Historical Tracking**: `VALID_FROM`, `VALID_TO`, `IS_CURRENT` columns
- **Dynamic Column Detection**: Automatically adapts to schema changes
- **Change Detection**: Tracks all attribute changes
- **Task Chaining**: Sequential execution with dependencies
- **Flexible Configuration**: Metadata-driven with column exclusions

### Data Quality & CDC
- Built-in data quality expectations
- CDC (Change Data Capture) support
- Configurable sequence columns for ordering

## 📦 Installation

### From Source



### Requirements

```
snowflake-snowpark-python>=1.0.0
```

## 🏃 Quick Start

### 2. Bronze Layer Ingestion

```python
from snowmeta.snowmeta_pipeline import SnowmetaPipeline

pipeline = SnowmetaPipeline(session)

pipeline_bronze_data = [
    {
        "source_table": "Banks_2022_2023_raw",
        "source_path_dev": "@RAW.ETBANKSFINANCIAL.LANDING/",
        "reader_format": "CSV",
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "FINANCIAL_BRONZE",
        "bronze_table": "Banks_2022_2023"
    }
]

pipeline.invoke_bronze_pipeline(
    pipeline_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

### 3. Silver Layer - SCD Type 2

```python
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "FINANCIAL_BRONZE",
        "bronze_table": "Banks_2022_2023",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "FINANCIAL_SILVER",
        "silver_table": "Banks_2022_2023",
        "silver_cdc_apply_changes": {
            "keys": ["customer_id"],
            "sequence_by": "dmsTimestamp",
            "scd_type": "2",
            "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
        }
    }
]

pipeline.invoke_silver_scd2_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    warehouse_name="COMPUTE_WH",
    bronze_task_name="ANALYTICS.FINANCIAL_BRONZE.INGEST_ALL_BRONZE",
    execute_tasks=True
)
```

## 📚 Documentation

- **[SCD Type 2 Pipeline Guide](docs/SCD2_PIPELINE_GUIDE.md)** - Comprehensive guide for SCD Type 2 implementation
- **[Examples README](examples/README.md)** - Example scripts and usage patterns
- **[Quick Start Script](examples/quickstart_financial_scd2.py)** - Ready-to-use financial data pipeline

## 🔧 Architecture

### Data Flow

```
Raw Stage Data → Bronze Layer → Silver Layer → Gold Layer
            (Ingestion)   (SCD 1/2)      (Analytics)
```

### Bronze Layer
- **Input**: Raw files from cloud storage (S3, Azure Blob, GCS)
- **Processing**: Schema inference, COPY INTO
- **Output**: Raw tables with minimal transformations

### Silver Layer
- **Input**: Bronze tables
- **Processing**: 
  - SCD Type 1: Dynamic Tables with deduplication
  - SCD Type 2: Stored Procedures with historical tracking
- **Output**: Clean, historized business entities

### Gold Layer
- **Input**: Silver tables
- **Processing**: Aggregations, joins, business logic
- **Output**: Analytics-ready datasets


## 🗂️ Project Structure

```
snow-meta/
├── snowmeta/                    # Main package
│   ├── __init__.py
│   ├── snowmeta_pipeline.py    # Core pipeline logic
│   ├── controltable_reader.py  # Metadata reader
│   ├── config.py               # Configuration
│   └── ...
├── examples/                    # Example scripts
│   ├── onboarding.ipynb
│   └── pipeline.ipynb
├── tests/                       # Test suite
├── ui/                          # UI components
├── setup.py                     # Package setup
└── README.md
```

## 🔌 API Reference

### SnowmetaPipeline Class

#### Bronze Layer Methods

- `invoke_bronze_pipeline(pipeline_data, warehouse_name, use_stored_procedures)`
  - Execute bronze ingestion pipeline
  
- `create_unified_bronze_stored_procedure(pipeline_data)`
  - Generate stored procedure SQL for bronze ingestion
  
- `generate_bronze_sql_scripts(pipeline_data, warehouse_name)`
  - Generate standalone SQL scripts

#### Silver Layer Methods - SCD Type 1

- `invoke_silver_pipeline(pipeline_silver_data, warehouse_name)`
  - Execute SCD Type 1 pipeline using Dynamic Tables

#### Silver Layer Methods - SCD Type 2 🆕

- `invoke_silver_scd2_pipeline(pipeline_silver_data, warehouse_name, bronze_task_name, execute_tasks)`
  - Execute SCD Type 2 pipeline with stored procedures and tasks
  
- `create_scd2_stored_procedure(silver_config)`
  - Generate stored procedure SQL for SCD Type 2
  
- `create_scd2_task(silver_config, warehouse_name, after_task)`
  - Generate task SQL for SCD Type 2
  
- `generate_scd2_sql_scripts(pipeline_silver_data, warehouse_name, bronze_task_name)`
  - Generate standalone SQL scripts for SCD Type 2

Built on top of Snowflake's powerful features:
- Snowpark Python
- Dynamic Tables
- Stored Procedures
- Tasks
- COPY INTO with schema inference



## 🗺️ Roadmap

- [ ] Data quality framework expansion
- [ ] Enhanced UI dashboard
- [ ] Automated testing framework
