# Snowflake-META Streamlit UI

A comprehensive Streamlit application for managing metadata-driven data pipelines in Snowflake.

## Features

- **Step 1: Onboarding** - Configure data ingestion pipeline metadata
- **Step 2: Deployment** - Deploy pipelines with Snowflake Tasks and Streams
- **Demo Tab** - View sample configurations and examples
- **CLI Tab** - Command-line reference and SQL commands

## Snowflake Terminology Mapping

This application uses Snowflake-native concepts instead of Databricks terminology:

| Databricks Concept | Snowflake Equivalent |
|-------------------|---------------------|
| Unity Catalog | Database (with optional catalog) |
| DBFS | Snowflake Stages/Volumes |
| DLT (Delta Live Tables) | Streams + Tasks + Stored Procedures |
| Delta Lake | Snowflake Native Tables |
| Serverless DLT | Serverless Tasks |
| Workspace | Account/Database |
| Metastore | Information Schema / Account Usage |

## Running Locally

### Prerequisites

- Python 3.8 or higher
- Snowflake account with appropriate permissions
- Access to Snowflake Stages for configuration files

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the Streamlit app:
```bash
streamlit run app.py
```

The application will open in your default web browser at `http://localhost:8501`.

## Deploying to Snowflake

Snowflake supports running Streamlit apps natively using Streamlit in Snowflake.

### Prerequisites

- Snowflake account with Streamlit in Snowflake enabled
- Appropriate privileges to create Streamlit apps
- Files uploaded to Snowflake Stage

### Deployment Steps

#### Option 1: Using Snowsight UI

1. Log into Snowsight (Snowflake web UI)
2. Navigate to **Data** > **Streamlit**
3. Click **+ Streamlit App**
4. Name your app: `SNOWMETA_UI`
5. Select warehouse: `COMPUTE_WH`
6. Select database and schema for the app
7. Copy the contents of `app.py` into the editor
8. Click **Run** to deploy

#### Option 2: Using SQL Commands

```sql
-- Create Streamlit app
CREATE STREAMLIT SNOWMETA_UI
  ROOT_LOCATION = '@MY_STAGE/streamlit_app'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = COMPUTE_WH;

-- Grant access to roles
GRANT USAGE ON STREAMLIT SNOWMETA_UI TO ROLE DATA_ENGINEER;
```

#### Option 3: Using SnowCLI

```bash
# Install SnowCLI
pip install snowflake-cli-labs

# Configure connection
snow connection add --name my_connection

# Deploy Streamlit app
snow streamlit deploy \
  --name SNOWMETA_UI \
  --file app.py \
  --warehouse COMPUTE_WH \
  --database MY_DB \
  --schema MY_SCHEMA
```

## Configuration

The application collects the following configuration parameters:

### Onboarding Configuration

- **Snowflake Horizon Enabled**: Enable governance features
- **Database Name**: Target Snowflake database
- **Serverless Enabled**: Use serverless compute
- **Onboarding File Path**: Stage path to configuration file
- **Schema Names**: Bronze, Silver, and metadata schema names
- **Dataflow Spec Tables**: Control table names
- **Environment**: Target environment (dev/staging/prod)
- **Version**: Configuration version identifier

### Deployment Configuration

- **DataFlow Schema Name**: Schema containing specifications
- **Target Schema Name**: Output schema for pipeline data
- **Bronze/Silver Group Names**: Pipeline group identifiers
- **Pipeline Name**: Unique pipeline identifier
- **Layer Selection**: Bronze, Silver, or Gold

## Usage Examples

### Example 1: Basic Onboarding

1. Navigate to **UI** > **Step 1: Onboarding**
2. Enable Snowflake Horizon
3. Enter database name: `ANALYTICS`
4. Set onboarding file path: `@MY_STAGE/configs/onboarding.json`
5. Configure schema names for bronze/silver layers
6. Click **Run Onboarding**

### Example 2: Pipeline Deployment

1. Complete onboarding first
2. Navigate to **UI** > **Step 2: Deployment**
3. Review auto-populated fields from onboarding
4. Enter target schema name: `PRODUCTION`
5. Set pipeline name: `USER_DATA_PIPELINE`
6. Click **Deploy**

### Example 3: Viewing Examples

1. Navigate to **Demo** tab
2. Review sample JSON configurations
3. Copy and modify for your use case

### Example 4: CLI Commands

1. Navigate to **CLI** tab
2. Review Python and SQL examples
3. Copy commands for terminal execution

## Architecture

The Snowflake-META pipeline consists of three layers:

```
┌─────────────────────────────────────────────────────┐
│                  Source Systems                      │
│  (S3, Azure Blob, GCS, Snowflake Tables, APIs)      │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│              Bronze Layer (Raw Data)                 │
│  • Data ingestion from stages                        │
│  • Minimal transformation                            │
│  • Full historical data                              │
│  • Implemented via: COPY INTO, Snowpipe              │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│           Silver Layer (Cleansed Data)               │
│  • Data quality checks                               │
│  • Type casting and validation                       │
│  • CDC processing via Streams                        │
│  • Implemented via: Streams + Tasks                  │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│          Gold Layer (Business Metrics)               │
│  • Aggregations and joins                            │
│  • Business logic                                    │
│  • Ready for consumption                             │
│  • Implemented via: Dynamic Tables, Tasks            │
└─────────────────────────────────────────────────────┘
```

## Key Snowflake Features Used

### Stages and Volumes
- **Internal Stages**: Store configuration and data files within Snowflake
- **External Stages**: Connect to S3, Azure, or GCS
- **Volumes**: Snowflake-managed storage for unstructured data

### Streams
- Track changes (CDC) in tables
- Support for insert, update, delete operations
- Enable incremental processing

### Tasks
- Schedule SQL execution
- Chain tasks with dependencies
- Serverless or warehouse-based execution

### Snowflake Horizon
- Unified governance
- Data quality rules
- Column-level security
- Tag-based policies

## Troubleshooting

### Issue: "Database not found"
**Solution**: Ensure the database exists and you have appropriate privileges.

```sql
-- Check available databases
SHOW DATABASES;

-- Grant necessary privileges
GRANT USAGE ON DATABASE ANALYTICS TO ROLE DATA_ENGINEER;
```

### Issue: "Stage path not accessible"
**Solution**: Verify stage exists and contains the files.

```sql
-- List files in stage
LIST @MY_STAGE/configs/;

-- Grant read access
GRANT READ ON STAGE MY_STAGE TO ROLE DATA_ENGINEER;
```

### Issue: "Streamlit app won't start"
**Solution**: Check warehouse is running and you have compute privileges.

```sql
-- Check warehouse status
SHOW WAREHOUSES;

-- Resume warehouse if suspended
ALTER WAREHOUSE COMPUTE_WH RESUME;
```

## Contributing

Contributions are welcome! Please ensure:
- Code follows PEP 8 style guidelines
- All Databricks terminology is replaced with Snowflake equivalents
- Documentation is updated for new features

## License

See the main project LICENSE file for details.

## Support

For issues or questions:
- Check the **Demo** tab for examples
- Review the **CLI** tab for command references
- Consult [Snowflake Documentation](https://docs.snowflake.com/)

## Version History

- **v1.0.0** (2025-10-03): Initial release with onboarding and deployment UI
  - Full Snowflake terminology
  - Streamlit in Snowflake support
  - Three-tab interface (UI, Demo, CLI)

