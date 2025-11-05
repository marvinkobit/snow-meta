# Snowmeta Ingestion Framework

A metadata-driven ingestion framework for Snowflake that leverages native capabilities including Snowpipe, Tasks, and Stored Procedures to automate data ingestion and transformation across Bronze and Silver layers.

## 🎯 Overview

Snowmeta Pipeline is a comprehensive framework designed to be **flexible**, **scalable**, and **observable**, providing:

- ✅ Metadata-driven configuration
- ✅ Bronze layer ingestion with automatic schema inference
- ✅ Variant loading for nested JSON data in bronze layer
- ✅ Silver layer transformations (SCD Type 1 & Type 2)
- ✅ JSON flattening for nested VARIANT/JSON columns
- ✅ Custom select expressions for column transformations
- ✅ Data quality expectations with drop and quarantine support
- ✅ Data quality checks and CDC handling
- ✅ Environment promotion capabilities
- ✅ Minimal operational overhead

### Built using Snowflake-native features:

- **Snowpark Python** - Python API for Snowflake data processing
- **Stored Procedures & Tasks** - Native orchestration and reusable SQL logic
- **COPY INTO & INFER_SCHEMA** - High-performance data loading with automatic schema inference
- **Streams for CDC** - Change Data Capture using Snowflake Streams
- **INFORMATION_SCHEMA** - Metadata introspection for dynamic column detection

**Integration**: Snowpark Python + SQL-based ingestion and transformation using Snowflake's native capabilities.

## 🚀 Features

### Bronze Layer Ingestion

- **Automatic Schema Inference**: Uses Snowflake's `INFER_SCHEMA` function
- **Multiple File Formats**: Support for CSV, JSON, Parquet, and more
- **Variant Loading**: Option to load nested JSON data as VARIANT type for semi-structured data
- **Unified Stored Procedures**: Single procedure for all tables
- **Task Automation**: Automated execution with Snowflake Tasks

### Silver Layer Transformations

#### SCD Type 1 (Stored Procedures + Tasks)
- Latest record only (overwrites)
- Automatic deduplication using MERGE statements
- Soft delete support for removed records
- Stored procedures with task orchestration

#### SCD Type 2 (Stored Procedures + Tasks) 🆕
- **Full Historical Tracking**: `VALID_FROM`, `VALID_TO`, `IS_CURRENT` columns
- **Dynamic Column Detection**: Automatically adapts to schema changes
- **Change Detection**: Tracks all attribute changes
- **Task Chaining**: Sequential execution with dependencies
- **Flexible Configuration**: Metadata-driven with column exclusions

#### Transformation Features

**JSON Flattening**:
- Automatically flattens nested JSON/VARIANT columns into relational structure
- Supports multiple columns simultaneously
- Uses dynamic key extraction from actual data
- Creates flattened views with prefixed column names

**Select Expression**:
- Custom column selection and transformation
- Supports complex expressions and aliases
- Dynamic view creation based on configuration
- Useful for column renaming, type casting, and calculations

**Data Quality Expectations**:
- **Drop Filtering**: Rows failing critical expectations are excluded from processing
- **Quarantine Support**: Rows passing drop but failing quarantine expectations are isolated
- **Flexible Predicates**: SQL-based expectation expressions
- **Automatic View Creation**: Filtered views created dynamically

### Data Quality & CDC
- Built-in data quality expectations with drop and quarantine support
- CDC (Change Data Capture) support via Snowflake Streams
- Configurable sequence columns for ordering
- Transformation pipelines (JSON flattening, column selection, filtering)

## 📦 Installation

### Requirements

- Python >= 3.9
- Snowflake account with Snowpark enabled
- Snowflake-snowpark-python >= 1.0.0

### Install from Source

```bash
# Clone the repository
git clone https://github.com/marvinkobit/snow-meta.git
cd snow-meta

# Install the package
pip install -e .
```

### Install from PyPI

```bash
pip install snowmeta
```

## 🏃 Quick Start

### 1. Create a Snowflake Session

```python
from snowflake.snowpark import Session

connection_parameters = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "warehouse": "COMPUTE_WH",
    "database": "ANALYTICS",
    "schema": "PUBLIC",
    "role": "ACCOUNTADMIN"
}

session = Session.builder.configs(connection_parameters).create()
```

### 2. Bronze Layer Ingestion

#### Standard Ingestion (CSV/Parquet)

```python
from snowmeta.snowmeta_pipeline import SnowmetaPipeline

# Initialize pipeline
pipeline = SnowmetaPipeline(session)

# Define bronze pipeline configuration
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

# Execute bronze pipeline
pipeline.invoke_bronze_pipeline(
    pipeline_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

#### Variant Loading for Nested JSON

```python
# Bronze pipeline with variant loading for nested JSON
pipeline_bronze_data = [
    {
        "source_table": "products_json",
        "source_path_dev": "@RAW.PRODUCTS.LANDING/",
        "reader_format": "JSON",
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "PRODUCTS_BRONZE",
        "bronze_table": "products_raw",
        "variant_load": True,  # Load as VARIANT type
        "variant_column_name": "SRC"  # Optional: specify column name (default: "SRC")
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
# Define silver pipeline configuration
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

# Execute SCD Type 2 pipeline
pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    bronze_task_name="ANALYTICS.FINANCIAL_BRONZE.INGEST_ALL_BRONZE",
    execute_tasks=True,
    use_stored_procedures=True
)
```

### 4. Silver Layer - SCD Type 1

```python
# Define silver pipeline configuration for SCD Type 1
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "FINANCIAL_BRONZE",
        "bronze_table": "Banks_2022_2023",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "FINANCIAL_SILVER",
        "silver_table": "Banks_2022_2023_latest",
        "silver_cdc_apply_changes": {
            "keys": ["customer_id"],
            "sequence_by": "dmsTimestamp",
            "scd_type": "1",
            "columns_to_track": ["customer_name", "email", "status"]
        }
    }
]

# Execute SCD Type 1 pipeline
pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    execute_tasks=True,
    use_stored_procedures=True
)
```

### 5. Silver Layer with Transformations

#### JSON Flattening

```python
# Silver pipeline with JSON flattening
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "PRODUCTS_BRONZE",
        "bronze_table": "products_raw",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "PRODUCTS_SILVER",
        "silver_table": "products",
        "silver_transformation_json": {
            "columns_to_flatten": ["product_details", "shipping_info"]  # Flatten these VARIANT columns
        },
        "silver_cdc_apply_changes": {
            "keys": ["product_id"],
            "sequence_by": "updated_at",
            "scd_type": "2"
        }
    }
]

pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

#### Select Expression

```python
# Silver pipeline with custom select expressions
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "FINANCIAL_BRONZE",
        "bronze_table": "transactions",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "FINANCIAL_SILVER",
        "silver_table": "transactions_clean",
        "silver_transformation_json": {
            "select_exp": [
                "transaction_id",
                "customer_id",
                "amount::DECIMAL(10,2) AS amount",
                "transaction_date::DATE AS transaction_date",
                "UPPER(status) AS status",
                "CASE WHEN amount > 1000 THEN 'HIGH' ELSE 'LOW' END AS amount_category"
            ]
        },
        "silver_cdc_apply_changes": {
            "keys": ["transaction_id"],
            "sequence_by": "updated_at",
            "scd_type": "1"
        }
    }
]

pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

#### Data Quality Expectations

```python
# Silver pipeline with data quality expectations
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "FINANCIAL_BRONZE",
        "bronze_table": "customers",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "FINANCIAL_SILVER",
        "silver_table": "customers",
        "silver_transformation_json": {
            "data_quality_expectations": {
                "expect_or_drop": [
                    "customer_id IS NOT NULL",
                    "email IS NOT NULL",
                    "LENGTH(email) > 0"
                ],
                "expect_or_quarantine": [
                    "email LIKE '%@%.%'",  # Valid email format
                    "age BETWEEN 18 AND 120"
                ]
            }
        },
        "silver_cdc_apply_changes": {
            "keys": ["customer_id"],
            "sequence_by": "updated_at",
            "scd_type": "2"
        }
    }
]

pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

#### Combined Transformations

```python
# Silver pipeline with all transformations (flattening, select, DQ)
pipeline_silver_data = [
    {
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "PRODUCTS_BRONZE",
        "bronze_table": "products_raw",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "PRODUCTS_SILVER",
        "silver_table": "products",
        "silver_transformation_json": {
            # Step 1: Flatten JSON columns
            "columns_to_flatten": ["product_details", "shipping_info"],
            # Step 2: Select and transform columns
            "select_exp": [
                "product_id",
                "PRODUCT_DETAILS_NAME AS product_name",
                "PRODUCT_DETAILS_BRAND AS brand",
                "PRODUCT_DETAILS_PRICE::DECIMAL(10,2) AS price",
                "SHIPPING_INFO_METHOD AS shipping_method"
            ],
            # Step 3: Apply data quality filters
            "data_quality_expectations": {
                "expect_or_drop": [
                    "product_id IS NOT NULL",
                    "product_name IS NOT NULL"
                ],
                "expect_or_quarantine": [
                    "price > 0",
                    "LENGTH(product_name) > 0"
                ]
            }
        },
        "silver_cdc_apply_changes": {
            "keys": ["product_id"],
            "sequence_by": "updated_at",
            "scd_type": "2"
        }
    }
]

pipeline.invoke_silver_scd_pipeline(
    pipeline_silver_data=pipeline_silver_data,
    pipeline_bronze_data=pipeline_bronze_data,
    warehouse_name="COMPUTE_WH",
    use_stored_procedures=True
)
```

## 🏗️ Architecture

### Data Flow

```
Raw Stage Data → Bronze Layer → Silver Layer → Gold Layer
            (Ingestion)   (SCD 1/2)      (Analytics)
```

### Bronze Layer

**Purpose**: Raw data ingestion with minimal transformations

- **Input**: Raw files from cloud storage (S3, Azure Blob, GCS)
- **Processing**: 
  - Schema inference using `INFER_SCHEMA`
  - Data loading via `COPY INTO`
  - Format validation
- **Output**: Raw tables with inferred schemas

### Silver Layer

**Purpose**: Clean, historized business entities

- **Input**: Bronze tables
- **Processing**: 
  - **SCD Type 1**: Stored procedures with MERGE statements (latest record only, overwrites)
  - **SCD Type 2**: Stored procedures with historical tracking (full history with VALID_FROM/VALID_TO)
- **Output**: Clean, validated, historized business entities

### Gold Layer

**Purpose**: Analytics-ready datasets

- **Input**: Silver tables
- **Processing**: Aggregations, joins, business logic
- **Output**: Analytics-ready datasets for reporting and BI

## 🔧 Stored Procedures & Tasks Architecture

Snowmeta Pipeline uses a **dynamic code generation** approach to create stored procedures and tasks based on metadata configuration. This architecture provides flexibility, maintainability, and native Snowflake orchestration.

### Dynamic Generation Strategy

The framework generates SQL code at runtime based on:

1. **Metadata Configuration**: Table names, schemas, keys, transformations
2. **Schema Introspection**: Column lists retrieved from `INFORMATION_SCHEMA`
3. **Transformation Logic**: JSON flattening, column selection, data quality filters
4. **SCD Type**: Different MERGE logic for Type 1 (overwrite) vs Type 2 (historical)

### Bronze Layer Architecture

**Unified Stored Procedure Pattern**:

```
┌─────────────────────────────────────┐
│  SP_INGEST_ALL_BRONZE()             │
│  ───────────────────────────         │
│  • Table 1: CREATE + COPY INTO      │
│  • Table 2: CREATE + COPY INTO      │
│  • Table N: CREATE + COPY INTO      │
└─────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────┐
│  TASK: INGEST_ALL_BRONZE             │
│  ───────────────────────────         │
│  CALL SP_INGEST_ALL_BRONZE();        │
└─────────────────────────────────────┘
```

**Key Features**:
- Single stored procedure handles all bronze tables in a schema
- Procedure body dynamically built from pipeline configuration
- One task executes the entire bronze ingestion pipeline
- Supports multiple file formats (CSV, JSON, Parquet, Variant)

### Silver Layer Architecture

**Master Procedure Pattern**:

```
┌─────────────────────────────────────────────────────┐
│  SP_SNOWMETA_SILVER_MASTER_{SCHEMA}()               │
│  ─────────────────────────────────────              │
│  • CALL SP_FLATTEN_JSON_{TABLE}();  (if needed)     │
│  • CALL SP_SELECT_EXPRESSION_{TABLE}(); (if needed) │
│  • CALL SP_DQ_EXPECTATIONS_{TABLE}(); (if needed)   │
│  • CALL SP_UPSERT_SCD{1|2}_{TABLE}();               │
│  • CALL SP_UPSERT_SCD{1|2}_{TABLE_N}();             │
└─────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────┐
│  TASK: TASK_SILVER_SCD_{SCHEMA}                     │
│  ─────────────────────────────────────              │
│  AFTER {bronze_task_name}                           │
│  CALL SP_SNOWMETA_SILVER_MASTER_{SCHEMA}();         │
└─────────────────────────────────────────────────────┘
```

**Individual SCD Procedures**:

For each silver table, the framework generates:

1. **Transformation Procedures** (optional):
   - `SP_FLATTEN_JSON_{TABLE}()` - Flattens nested JSON columns
   - `SP_SELECT_EXPRESSION_{TABLE}()` - Applies column transformations
   - `SP_DQ_EXPECTATIONS_{TABLE}()` - Applies data quality filters

2. **SCD Procedure**:
   - `SP_UPSERT_SCD1_{TABLE}()` - MERGE with overwrite logic
   - `SP_UPSERT_SCD2_{TABLE}()` - MERGE with historical tracking

**Key Features**:
- Master procedure orchestrates all transformations for a schema
- Individual procedures are modular and reusable
- Task chaining via `AFTER` clause ensures proper execution order
- Column lists dynamically retrieved from `INFORMATION_SCHEMA`
- Change detection predicates generated based on tracked columns

### Dynamic SQL Generation

The framework generates SQL using these techniques:

1. **Column List Generation**:
   ```sql
   SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ')
   FROM INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = '{schema}' 
     AND TABLE_NAME = '{table}'
     AND COLUMN_NAME NOT IN ({excluded_columns});
   ```

2. **Change Detection Logic**:
   - Type 1: Updates only tracked columns when values differ
   - Type 2: Detects changes using `IS DISTINCT FROM` for all non-excluded columns

3. **MERGE Statement Construction**:
   - Dynamic INSERT column lists
   - Dynamic UPDATE SET clauses
   - Conditional logic based on SCD type

### Task Orchestration

Tasks are automatically created with:

- **Warehouse Assignment**: Configurable per pipeline
- **Dependency Chaining**: `AFTER` clause links tasks
- **Execution Control**: Can execute immediately or schedule later
- **Error Handling**: Task state can be monitored via Snowflake UI

### Benefits of This Architecture

1. **Metadata-Driven**: Changes to configuration automatically update generated code
2. **Schema-Agnostic**: Works with any table structure without code changes
3. **Type-Safe**: Column references use quoted identifiers for case-sensitivity
4. **Maintainable**: All logic in stored procedures, easy to audit and debug
5. **Scalable**: Master procedures handle multiple tables efficiently
6. **Native Integration**: Uses Snowflake's task orchestration for scheduling

## 🔧 Implementation Approach

Snowmeta Pipeline uses a **hybrid approach** that leverages the strengths of both Snowpark DataFrame API and Snowpark SQL to deliver optimal performance, maintainability, and feature coverage.

### Design Philosophy

The framework strategically chooses between DataFrame API and SQL based on the specific use case:

#### Snowpark SQL (String-Based) ✅

**Best for:**
- Bronze Layer Ingestion - Full access to `COPY INTO`, `INFER_SCHEMA`, and file format options
- SCD Type 1 & 2 Logic - Complex `MERGE` statements and window functions with `QUALIFY`
- Stored Procedures & Tasks - Native Snowflake orchestration and scheduling
- Bulk Operations - Maximum performance without Python overhead

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
- Metadata Operations - Reading control tables and configuration
- Data Transformations - Dynamic column mapping and type conversions
- Validation & Monitoring - Row counts, profiling, quality checks
- Schema Operations - Inspecting and manipulating table schemas

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
| **Silver SCD Type 1** | ❌ Complex MERGE logic | ✅ Native MERGE statements | ✅ **SQL** |
| **Silver SCD Type 2** | ❌ Complex MERGE logic | ✅ Native window functions | ✅ **SQL** |
| **Metadata Reading** | ✅ Type-safe objects | ❌ Verbose string parsing | ✅ **DataFrame API** |
| **Data Transformations** | ✅ Programmatic building | ❌ String concatenation | 🔄 **Hybrid** (could expand) |
| **Testing** | ✅ Mock-friendly | ❌ Requires DB connection | ✅ **DataFrame API** |
| **Performance** | ⚠️ Python overhead | ✅ Native execution | ✅ **SQL** |
| **Maintainability** | ✅ Type-safe refactoring | ⚠️ Runtime errors only | 🔄 **Balanced** |

### Why This Matters

The hybrid approach ensures:
1. **Performance** - SQL for data-intensive operations
2. **Maintainability** - DataFrame API for configuration and metadata
3. **Feature Coverage** - No limitations from API gaps
4. **Production Readiness** - Native Snowflake orchestration (Tasks, Procedures)

This design makes Snowmeta Pipeline both **developer-friendly** and **production-grade**.

## 🔌 API Reference

### SnowmetaPipeline Class

The main class for executing pipeline operations.

#### Initialization

```python
pipeline = SnowmetaPipeline(session)
```

#### Bronze Layer Methods

##### `invoke_bronze_pipeline(pipeline_data, warehouse_name, use_stored_procedures)`

Execute bronze ingestion pipeline.

**Parameters:**
- `pipeline_data` (list): List of bronze pipeline configurations
- `warehouse_name` (str): Snowflake warehouse name
- `use_stored_procedures` (bool): Whether to use stored procedures

**Returns:** None

##### `create_unified_bronze_stored_procedure(pipeline_data)`

Generate stored procedure SQL for bronze ingestion. Creates a unified procedure that handles all tables in the pipeline configuration.

**Parameters:**
- `pipeline_data` (list): List of bronze pipeline configurations

**Returns:** str - SQL script for stored procedure

##### `generate_bronze_sql_scripts(pipeline_data, warehouse_name)`

Generate standalone SQL scripts for bronze ingestion.

**Parameters:**
- `pipeline_data` (list): List of bronze pipeline configurations
- `warehouse_name` (str): Snowflake warehouse name

**Returns:** dict - Dictionary with 'procedure', 'task', 'procedure_name', and 'task_name' keys

#### Silver Layer Methods - SCD Type 1 & 2

##### `invoke_silver_scd_pipeline(pipeline_silver_data, pipeline_bronze_data, warehouse_name, bronze_task_name, execute_tasks, use_stored_procedures)`

Execute silver layer pipeline that creates stored procedures and tasks for SCD Type 1 or Type 2 logic.

**Parameters:**
- `pipeline_silver_data` (list): List of silver pipeline configurations
- `pipeline_bronze_data` (list): List of bronze pipeline configurations
- `warehouse_name` (str): Snowflake warehouse name
- `bronze_task_name` (str, optional): Name of the bronze task to chain after
- `execute_tasks` (bool): Whether to execute tasks immediately (default: True)
- `use_stored_procedures` (bool): Whether to use stored procedures and tasks (default: False)

**Returns:** Execution result if tasks are executed, None otherwise

##### `create_scd1_stored_procedure(silver_config, flattened_view_name, stream_name)`

Generate stored procedure SQL for SCD Type 1.

**Parameters:**
- `silver_config` (dict): Silver pipeline configuration
- `flattened_view_name` (str, optional): Name of flattened view if transformations applied
- `stream_name` (str, optional): Name of stream if using incremental load

**Returns:** str - SQL script for stored procedure

##### `create_scd2_stored_procedure(silver_config, flattened_view_name, stream_name)`

Generate stored procedure SQL for SCD Type 2.

**Parameters:**
- `silver_config` (dict): Silver pipeline configuration
- `flattened_view_name` (str, optional): Name of flattened view if transformations applied
- `stream_name` (str, optional): Name of stream if using incremental load

**Returns:** str - SQL script for stored procedure

##### `create_master_silver_task(pipeline_silver_data, warehouse_name, after_task)`

Generate task SQL for master silver procedure.

**Parameters:**
- `pipeline_silver_data` (list): List of silver pipeline configurations
- `warehouse_name` (str): Snowflake warehouse name
- `after_task` (str, optional): Task name to chain after

**Returns:** str - SQL script for task

## 🗂️ Project Structure

```
snow-meta/
├── snowmeta/                    # Main package
│   ├── __init__.py
│   ├── snowmeta_pipeline.py    # Core pipeline logic
│   ├── snowmeta_sql.py         # SQL generation utilities
│   ├── controltable_reader.py # Metadata reader
│   ├── controltable_spec.py   # Control table specifications
│   ├── onboard_controltable.py # Control table onboarding
│   └── config.py              # Configuration management
├── examples/                    # Example scripts
│   ├── onboarding.ipynb
│   ├── pipeline.ipynb
│   ├── sample_onboarding_metadata.json
│   └── sample_silver_transformations.json
├── tests/                       # Test suite
├── ui/                          # UI components
│   ├── app.py
│   └── README.md
├── docs/                        # Additional documentation
├── setup.py                     # Package setup
└── README.md                    # This file
```

## 📚 Documentation

- **[SCD Type 2 Pipeline Guide](docs/SCD2_PIPELINE_GUIDE.md)** - Comprehensive guide for SCD Type 2 implementation
- **[Examples README](examples/README.md)** - Example scripts and usage patterns
- **[UI Documentation](ui/README.md)** - UI component documentation

## 🛠️ Built With

- **Snowpark Python** - Snowflake's native Python library
- **Stored Procedures** - Reusable SQL logic with dynamic generation
- **Tasks** - Automated orchestration and scheduling
- **Snowpipe COPY INTO** - High-performance data loading with schema inference
- **MERGE Statements** - Efficient upsert operations for SCD logic

## 🗺️ Roadmap

- [ ] Data quality framework expansion
- [ ] Enhanced UI dashboard
- [ ] Automated testing framework
- [ ] Gold layer transformation templates
- [ ] Enhanced monitoring and observability
- [ ] Multi-environment deployment automation

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Maeruf**
- Email: maeruf@nexsis.ca
- GitHub: [@marvinkobit](https://github.com/marvinkobit)

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/marvinkobit/snow-meta/issues).
