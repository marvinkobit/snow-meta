"""
Snowflake-META UI Application
A Streamlit application for onboarding and deploying data pipelines in Snowflake
"""

import streamlit as st
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Snowflake-META",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'onboarding_complete' not in st.session_state:
    st.session_state.onboarding_complete = False
if 'onboarding_data' not in st.session_state:
    st.session_state.onboarding_data = {}


def render_header():
    """Render application header"""
    st.title("❄️ Snowflake-META")
    st.markdown("### Metadata-driven Data Pipeline Management for Snowflake")
    st.divider()


def render_onboarding_tab():
    """Render the onboarding configuration tab"""
    st.header("Step 1: Onboarding")
    st.markdown("Configure your data ingestion pipeline metadata")
    
    # Create three columns for the first row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        horizon_enabled = st.radio(
            "Run onboarding with Snowflake Horizon enabled?",
            options=[True, False],
            index=0,
            help="Enable Snowflake Horizon for governance and data quality"
        )
    
    with col2:
        database_name = st.text_input(
            "Provide database name:",
            value="",
            placeholder="Enter database name",
            help="Snowflake database where control tables will be created"
        )
    
    with col3:
        serverless_enabled = st.radio(
            "Run onboarding with serverless?",
            options=[True, False],
            index=0,
            help="Use Snowflake serverless compute for pipeline execution"
        )
    
    st.divider()
    
    # Create two columns for file paths
    col1, col2 = st.columns(2)
    
    with col1:
        onboarding_file_path = st.text_input(
            "Provide onboarding file path:",
            value="demo/conf/onboarding.template",
            help="Path to onboarding configuration file in Snowflake stage (e.g., @MY_STAGE/conf/onboarding.json)"
        )
    
    with col2:
        onboarding_files_directory = st.text_input(
            "Provide onboarding files local directory:",
            value="/app/python/source_code/snow-meta/demo/",
            help="Local directory path containing onboarding configuration files"
        )
    
    st.divider()
    
    # Create two columns for schema names
    col1, col2 = st.columns(2)
    
    with col1:
        meta_schema_name = st.text_input(
            "Provide snow meta schema name:",
            value="snowmeta_dataflowspecs_4e6c360d3e5c4b5ca6687fec8f8e2e14",
            help="Schema name for Snowflake-META metadata tables"
        )
    
    with col2:
        bronze_schema_name = st.text_input(
            "Provide snow meta bronze layer schema name:",
            value="snowmeta_bronze_9c1aa383b36a49198d3e99d25f7180a4",
            help="Schema name for bronze (raw) layer tables"
        )
    
    st.divider()
    
    # Create three columns for layer configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        silver_schema_name = st.text_input(
            "Provide snow meta silver layer schema name:",
            value="snowmeta_silver_7b4e981029b843c799bf6fa0a121b3ca",
            help="Schema name for silver (cleansed) layer tables"
        )
    
    with col2:
        meta_layer = st.selectbox(
            "Provide snow meta layer:",
            options=["bronze", "silver", "gold"],
            index=0,
            help="Select the data layer for this configuration"
        )
    
    with col3:
        st.write("")  # Spacer
    
    st.divider()
    
    # Create three columns for dataflow spec configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        bronze_dataflow_table = st.text_input(
            "Provide bronze dataflow spec table name:",
            value="bronze_dataflowspec",
            help="Table name for bronze layer dataflow specifications"
        )
    
    with col2:
        silver_dataflow_table = st.text_input(
            "Provide silver dataflow spec table name:",
            value="silver_dataflowspec",
            help="Table name for silver layer dataflow specifications"
        )
    
    with col3:
        overwrite_dataflow = st.radio(
            "Overwrite dataflow spec?",
            options=[False, True],
            index=1,
            help="Overwrite existing dataflow specifications if they exist"
        )
    
    st.divider()
    
    # Create two columns for version and environment
    col1, col2 = st.columns(2)
    
    with col1:
        dataflow_version = st.text_input(
            "Provide dataflow spec version:",
            value="v1",
            help="Version identifier for the dataflow specification"
        )
    
    with col2:
        environment_name = st.text_input(
            "Provide environment name:",
            value="prod",
            help="Target environment (dev, staging, prod)"
        )
    
    st.divider()
    
    # Import author
    import_author = st.text_input(
        "Provide import author name:",
        value="app-402bx9 meta-dlt",
        help="Name or identifier of the person/system performing the import"
    )
    
    st.divider()
    
    # Update paths checkbox
    update_paths = st.radio(
        "Update account/stage volume paths, database name, bronze/silver schema names in onboarding file?",
        options=[False, True],
        index=0,
        horizontal=True,
        help="Automatically update file paths and names in the onboarding configuration"
    )
    
    st.divider()
    
    # Run Onboarding button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Run Onboarding", type="primary", use_container_width=True):
            # Collect all onboarding data
            onboarding_data = {
                "horizon_enabled": horizon_enabled,
                "database": database_name,
                "serverless_enabled": serverless_enabled,
                "onboarding_file_path": onboarding_file_path,
                "onboarding_files_directory": onboarding_files_directory,
                "meta_schema_name": meta_schema_name,
                "bronze_schema_name": bronze_schema_name,
                "silver_schema_name": silver_schema_name,
                "meta_layer": meta_layer,
                "bronze_dataflow_table": bronze_dataflow_table,
                "silver_dataflow_table": silver_dataflow_table,
                "overwrite_dataflow": overwrite_dataflow,
                "dataflow_version": dataflow_version,
                "environment": environment_name,
                "import_author": import_author,
                "update_paths": update_paths,
                "timestamp": datetime.now().isoformat()
            }
            
            # Validate required fields
            if not database_name:
                st.error("❌ Database name is required!")
            else:
                # Store in session state
                st.session_state.onboarding_data = onboarding_data
                st.session_state.onboarding_complete = True
                
                # Show success message
                st.success("✅ Onboarding configuration completed successfully!")
                st.info("📋 Onboarding data saved. You can now proceed to the Deployment tab.")
                
                # Show configuration summary in expander
                with st.expander("View Onboarding Configuration", expanded=False):
                    st.json(onboarding_data)


def render_deployment_tab():
    """Render the deployment configuration tab"""
    st.header("Step 2: Deployment")
    
    if not st.session_state.onboarding_complete:
        st.warning("⚠️ Please complete the Onboarding step first before proceeding with deployment.")
        return
    
    st.markdown("### Deploying Pipeline:")
    st.info("Configure pipeline deployment parameters based on your onboarding configuration")
    
    # Create three columns for the first row
    col1, col2, col3 = st.columns(3)
    
    with col1:
        horizon_enabled = st.radio(
            "Run deployment with Snowflake Horizon enabled?",
            options=[True, False],
            index=0,
            help="Enable Snowflake Horizon for governance and data quality"
        )
    
    with col2:
        database_name = st.text_input(
            "Provide database name:",
            value=st.session_state.onboarding_data.get("database", ""),
            placeholder="Enter database name",
            help="Snowflake database for pipeline deployment"
        )
    
    with col3:
        serverless_enabled = st.radio(
            "Run deployment with serverless?",
            options=[True, False],
            index=0,
            help="Use Snowflake serverless compute for pipeline execution"
        )
    
    st.divider()
    
    # Create two columns for schema names
    col1, col2 = st.columns(2)
    
    with col1:
        dataflow_schema_name = st.text_input(
            "DataFlow Schema Name:",
            value=st.session_state.onboarding_data.get("meta_schema_name", ""),
            placeholder="Enter spec schema name",
            help="Schema containing dataflow specifications"
        )
    
    with col2:
        target_schema_name = st.text_input(
            "Target Schema Name:",
            value="",
            placeholder="Enter target schema name",
            help="Target schema for data pipeline output"
        )
    
    st.divider()
    
    # Create three columns for group configuration
    col1, col2, col3 = st.columns(3)
    
    with col1:
        bronze_group_name = st.text_input(
            "Onboard Bronze Group Name:",
            value="A1",
            help="Group identifier for bronze layer ingestion"
        )
    
    with col2:
        silver_group_name = st.text_input(
            "Onboard Silver Group Name:",
            value="A1",
            help="Group identifier for silver layer transformation"
        )
    
    with col3:
        meta_layer = st.selectbox(
            "Provide snow meta layer:",
            options=["bronze", "silver", "gold"],
            index=0,
            help="Select the data layer for pipeline deployment"
        )
    
    st.divider()
    
    # Create three columns for table names and pipeline
    col1, col2, col3 = st.columns(3)
    
    with col1:
        bronze_table_name = st.text_input(
            "Dataflow Spec Bronze Table Name:",
            value=st.session_state.onboarding_data.get("bronze_dataflow_table", ""),
            placeholder="Enter bronze spec table name",
            help="Bronze layer dataflow specification table"
        )
    
    with col2:
        silver_table_name = st.text_input(
            "Dataflow Spec Silver Table Name:",
            value=st.session_state.onboarding_data.get("silver_dataflow_table", ""),
            placeholder="Enter silver spec table name",
            help="Silver layer dataflow specification table"
        )
    
    with col3:
        pipeline_name = st.text_input(
            "Pipeline Name:",
            value="",
            placeholder="Enter pipeline name",
            help="Unique name for the Snowflake pipeline"
        )
    
    st.divider()
    
    # Deploy button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Deploy", type="primary", use_container_width=True):
            # Collect all deployment data
            deployment_data = {
                "horizon_enabled": horizon_enabled,
                "database": database_name,
                "serverless_enabled": serverless_enabled,
                "dataflow_schema_name": dataflow_schema_name,
                "target_schema_name": target_schema_name,
                "bronze_group_name": bronze_group_name,
                "silver_group_name": silver_group_name,
                "meta_layer": meta_layer,
                "bronze_table_name": bronze_table_name,
                "silver_table_name": silver_table_name,
                "pipeline_name": pipeline_name,
                "timestamp": datetime.now().isoformat()
            }
            
            # Validate required fields
            if not all([database_name, pipeline_name, target_schema_name]):
                st.error("❌ Database name, Pipeline name, and Target schema name are required!")
            else:
                # Show success message
                st.success("✅ Pipeline deployment initiated successfully!")
                st.info(f"📊 Pipeline '{pipeline_name}' is being deployed to database '{database_name}'")
                
                # Show deployment summary in expander
                with st.expander("View Deployment Configuration", expanded=True):
                    st.json(deployment_data)
                
                # Show combined configuration
                with st.expander("View Complete Configuration (Onboarding + Deployment)", expanded=False):
                    combined_config = {
                        "onboarding": st.session_state.onboarding_data,
                        "deployment": deployment_data
                    }
                    st.json(combined_config)


def render_demo_tab():
    """Render the demo/examples tab"""
    st.header("Demo & Examples")
    st.markdown("### Quick Start Examples")
    
    st.markdown("""
    #### Sample Onboarding Configuration
    
    Below is a sample onboarding metadata JSON structure for Snowflake-META:
    """)
    
    sample_onboarding = {
        "data_flow_id": "id_1001",
        "data_flow_group": "A1",
        "source_system": "AWS_S3",
        "source_format": "parquet",
        "source_details": {
            "source_database": "RAW",
            "source_schema": "SNOWMETA_CONFIG",
            "source_table": "users",
            "source_path_dev": "@RAW.SNOWMETA_CONFIG.S3_STAGE/users/full/"
        },
        "bronze_database_dev": "ANALYTICS",
        "bronze_schema": "SNOWMETA_BRONZE",
        "bronze_table": "users",
        "silver_database_dev": "ANALYTICS",
        "silver_schema": "SNOWMETA_SILVER",
        "silver_table": "users",
        "silver_cdc_apply_changes": {
            "keys": ["user_id"],
            "sequence_by": "landing_timestamp",
            "scd_type": "1"
        }
    }
    
    st.json(sample_onboarding)
    
    st.markdown("""
    #### Sample Silver Transformation Configuration
    
    Silver transformations define column selections and transformations:
    """)
    
    sample_transformation = [
        {
            "target_table": "users",
            "select_exp": [
                "user_id",
                "username",
                "email",
                "first_name",
                "last_name",
                "created_date",
                "updated_date",
                "is_active",
                "input_file_modification_time AS landing_timestamp",
                "_rescued_data"
            ]
        }
    ]
    
    st.json(sample_transformation)
    
    st.divider()
    
    st.markdown("""
    #### Key Snowflake Concepts
    
    - **Database**: Top-level container for schemas and tables
    - **Schema**: Logical grouping of database objects
    - **Stage**: Location for storing data files (internal or external)
    - **Streams**: Track changes in tables for CDC (Change Data Capture)
    - **Tasks**: Scheduled or triggered SQL execution units
    - **Snowflake Horizon**: Unified governance solution for data
    - **Serverless**: Compute resources managed automatically by Snowflake
    
    #### Pipeline Layers
    
    - **Bronze Layer**: Raw data ingestion (minimal transformation)
    - **Silver Layer**: Cleansed and conformed data
    - **Gold Layer**: Business-level aggregates and features
    """)


def render_cli_tab():
    """Render the CLI commands tab"""
    st.header("CLI Commands")
    st.markdown("### Command Line Interface Reference")
    
    st.markdown("""
    #### Installation
    
    Install Snowflake-META using pip:
    """)
    
    st.code("""
pip install snowflake-connector-python
pip install snowflake-snowpark-python
# Clone and install snow-meta
git clone <repository-url>
cd snow-meta
pip install -e .
    """, language="bash")
    
    st.divider()
    
    st.markdown("""
    #### Onboarding via CLI
    
    Run onboarding from the command line:
    """)
    
    onboarding_command = """
python -c "
from snowflake.snowpark import Session
from src.onboard_controltable import OnboardControlTable

# Create Snowflake session
session = Session.builder.configs({
    'account': '<account>',
    'user': '<user>',
    'password': '<password>',
    'warehouse': '<warehouse>',
    'database': '<database>',
    'schema': '<schema>'
}).create()

# Configure onboarding
params = {
    'onboarding_file_path': '@MY_STAGE/configs/onboarding.json',
    'database': 'ANALYTICS',
    'schema': 'SNOWMETA_CONFIG',
    'bronze_control_table': 'bronze_dataflowspec',
    'silver_control_table': 'silver_dataflowspec',
    'env': 'prod',
    'version': 'v1',
    'import_author': 'cli-user',
    'overwrite': True
}

# Run onboarding
onboard = OnboardControlTable(session, params, horizon_enabled=True)
onboard.onboard_controltable_specs()
"
    """
    
    st.code(onboarding_command, language="python")
    
    st.divider()
    
    st.markdown("""
    #### Deployment via CLI
    
    Deploy pipelines using Snowflake Tasks and Streams:
    """)
    
    deployment_command = """
# Create a Snowflake task for bronze layer ingestion
CREATE OR REPLACE TASK bronze_ingestion_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    CALL bronze_ingestion_procedure();

# Create a stream on bronze table
CREATE OR REPLACE STREAM bronze_stream ON TABLE ANALYTICS.SNOWMETA_BRONZE.users;

# Create a task for silver layer transformation
CREATE OR REPLACE TASK silver_transform_task
    WAREHOUSE = COMPUTE_WH
    AFTER bronze_ingestion_task
    WHEN SYSTEM$STREAM_HAS_DATA('bronze_stream')
AS
    CALL silver_transformation_procedure();

# Start the tasks
ALTER TASK silver_transform_task RESUME;
ALTER TASK bronze_ingestion_task RESUME;
    """
    
    st.code(deployment_command, language="sql")
    
    st.divider()
    
    st.markdown("""
    #### Monitoring & Troubleshooting
    
    Monitor your pipelines:
    """)
    
    monitoring_commands = """
-- Check task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'bronze_ingestion_task'
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- Check stream status
SHOW STREAMS IN SCHEMA ANALYTICS.SNOWMETA_BRONZE;

-- View control table contents
SELECT * FROM ANALYTICS.SNOWMETA_CONFIG.bronze_dataflowspec;
SELECT * FROM ANALYTICS.SNOWMETA_CONFIG.silver_dataflowspec;
    """
    
    st.code(monitoring_commands, language="sql")


def main():
    """Main application entry point"""
    render_header()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["UI", "Demo", "CLI"])
    
    with tab1:
        # Create subtabs for onboarding and deployment
        subtab1, subtab2 = st.tabs(["Step 1: Onboarding", "Step 2: Deployment"])
        
        with subtab1:
            render_onboarding_tab()
        
        with subtab2:
            render_deployment_tab()
    
    with tab2:
        render_demo_tab()
    
    with tab3:
        render_cli_tab()
    
    # Sidebar
    with st.sidebar:
        st.markdown("## ❄️ Snowflake-META")
        st.markdown("### Configuration Summary")
        
        if st.session_state.onboarding_complete:
            st.success("✅ Onboarding Complete")
            onb_data = st.session_state.onboarding_data
            st.markdown(f"""
            **Database:** {onb_data.get('database', 'N/A')}  
            **Environment:** {onb_data.get('environment', 'N/A')}  
            **Layer:** {onb_data.get('meta_layer', 'N/A')}  
            **Version:** {onb_data.get('dataflow_version', 'N/A')}
            """)
        else:
            st.info("📋 No onboarding completed yet")
        
        st.divider()
        
        st.markdown("### Quick Links")
        st.markdown("""
        - [Snowflake Documentation](https://docs.snowflake.com/)
        - [Snowpark Python API](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
        - [Snowflake Horizon](https://www.snowflake.com/en/data-cloud/horizon/)
        """)
        
        st.divider()
        
        if st.button("Reset Configuration"):
            st.session_state.onboarding_complete = False
            st.session_state.onboarding_data = {}
            st.rerun()


if __name__ == "__main__":
    main()

