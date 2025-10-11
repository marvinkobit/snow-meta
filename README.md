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

## 🏗️ Implementation Approach: Snowpark DataFrame API vs Snowpark SQL

Snowmeta Pipeline uses a **hybrid approach** that leverages the strengths of both Snowpark DataFrame API and Snowpark SQL to deliver optimal performance, maintainability, and feature coverage.

### Design Philosophy

Our framework strategically chooses between DataFrame API and SQL based on the specific use case:

#### Snowpark SQL (String-Based) ✅
**Best for:**
- **Bronze Layer Ingestion** - Full access to `COPY INTO`, `INFER_SCHEMA`, and file format options
- **SCD Type 2 Logic** - Complex `MERGE` statements and window functions with `QUALIFY`
- **Stored Procedures & Tasks** - Native Snowflake orchestration and scheduling
- **Bulk Operations** - Maximum performance without Python overhead

**Advantages:**
- ✅ Complete feature access to all Snowflake capabilities
- ✅ Native SQL execution with optimal performance
- ✅ Easy to audit, review, and debug
- ✅ Better integration with Snowflake's task orchestration
- ✅ Familiar to most data engineers

**Trade-offs:**
- ⚠️ String concatenation can be error-prone
- ⚠️ No compile-time type safety
- ⚠️ Harder to unit test without database connection

#### Snowpark DataFrame API ✅
**Best for:**
- **Metadata Operations** - Reading control tables and configuration
- **Data Transformations** - Dynamic column mapping and type conversions
- **Validation & Monitoring** - Row counts, profiling, quality checks
- **Schema Operations** - Inspecting and manipulating table schemas

**Advantages:**
- ✅ Type safety with compile-time error checking
- ✅ IDE support (auto-complete, IntelliSense)
- ✅ Programmatic transformation building
- ✅ Easier unit testing with mock DataFrames
- ✅ Better for complex conditional logic

**Trade-offs:**
- ⚠️ Python overhead adds latency
- ⚠️ Not all Snowflake features available
- ⚠️ Generated SQL may be suboptimal for complex queries

### Decision Matrix

| Aspect | DataFrame API | SQL | **Current Choice** |
|--------|---------------|-----|-------------------|
| **Bronze Ingestion** | ❌ Limited (`COPY INTO` options) | ✅ Full feature set | ✅ **SQL** |
| **Silver SCD Type 1** | ✅ Good (simple dedup) | ✅ Good (Dynamic Tables) | ✅ **SQL** (Dynamic Tables) |
| **Silver SCD Type 2** | ❌ Complex MERGE logic | ✅ Native window functions | ✅ **SQL** |
| **Metadata Reading** | ✅ Type-safe objects | ❌ Verbose string parsing | ✅ **DataFrame API** |
| **Data Transformations** | ✅ Programmatic building | ❌ String concatenation | 🔄 **Hybrid** (could expand) |
| **Testing** | ✅ Mock-friendly | ❌ Requires DB connection | ✅ **DataFrame API** |
| **Performance** | ⚠️ Python overhead | ✅ Native execution | ✅ **SQL** |
| **Maintainability** | ✅ Type-safe refactoring | ⚠️ Runtime errors only | 🔄 **Balanced** |

### Implementation Examples

#### ✅ Using SQL for Bronze (Current Approach)
```python
# Leverage full COPY INTO capabilities
sql_procedure = f"""
    CREATE TABLE IF NOT EXISTS {bronze_table}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(INFER_SCHEMA(LOCATION => '{source_path}', ...))
    );
    
    COPY INTO {bronze_table} FROM '{source_path}'
    FILE_FORMAT = (FORMAT_NAME = '{format}')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"""
```

#### ✅ Using DataFrame API for Metadata (Current Approach)
```python
# Type-safe metadata operations
df = self.session.table(self.bronze_control_table)
rows = df.collect()
bronze_specs = [BronzeControlTableSpec(**row.asDict()) for row in rows]
```

#### 🔄 Hybrid for Transformations (Enhancement Opportunity)
```python
# DataFrame API for validation
df = self.session.table(bronze_table)
row_count = df.count()
self.logger.info(f"Loaded {row_count} rows")

# SQL for bulk transformations
self.session.sql(copy_sql).collect()
```

### Why This Matters

The hybrid approach ensures:
1. **Performance** - SQL for data-intensive operations
2. **Maintainability** - DataFrame API for configuration and metadata
3. **Feature Coverage** - No limitations from API gaps
4. **Production Readiness** - Native Snowflake orchestration (Tasks, Procedures)

This design makes Snowmeta Pipeline both **developer-friendly** and **production-grade**.

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
