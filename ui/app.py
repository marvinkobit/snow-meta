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
    """Render the columnar onboarding configuration tab"""
    st.header("Step 1: Pipeline Configuration")
    st.markdown("Configure your bronze and silver pipeline metadata using the forms below")
    
    # Initialize session state for pipeline entries
    if 'bronze_entries' not in st.session_state:
        st.session_state.bronze_entries = []
    if 'silver_entries' not in st.session_state:
        st.session_state.silver_entries = []
    
    # Global configuration section
    st.subheader("🔧 Global Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        horizon_enabled = st.radio(
            "Snowflake Horizon enabled?",
            options=[True, False],
            index=0,
            help="Enable Snowflake Horizon for governance and data quality"
        )
    
    with col2:
        warehouse_name = st.selectbox(
            "Warehouse:",
            options=["COMPUTE_WH", "SNOWFLAKE_LEARNING_WH", "SNOW_COMPUTE", "TRANSFORMING"],
            index=3,  # TRANSFORMING as default
            help="Select Snowflake warehouse for pipeline execution"
        )
    
    st.divider()
    
    # Pipeline Configuration Form
    st.subheader("📊 Pipeline Configuration")
    st.markdown("Configure bronze and silver pipeline entries together")
    
    with st.form("pipeline_form", clear_on_submit=True):
        # Create two main columns for bronze and silver
        bronze_col, silver_col = st.columns(2)
        
        with bronze_col:
            st.markdown("### 🥉 Bronze Layer Configuration")
            st.markdown("Configure raw data ingestion pipeline")
            
            col1, col2 = st.columns(2)
            with col1:
                source_table = st.text_input(
                    "Source Table:",
                    placeholder="e.g., Banks_2022_2023_raw",
                    help="Name of the source table"
                )
                source_path_dev = st.text_input(
                    "Source Path:",
                    placeholder="@RAW.ETBANKSFINANCIAL.LANDING/",
                    help="Stage path to source data"
                )
                reader_format = st.selectbox(
                    "Reader Format:",
                    options=["CSV", "PARQUET", "JSON", "AVRO", "ORC"],
                    index=0,  # CSV as default
                    help="Data format for reading"
                )
            
            with col2:
                bronze_database_dev = st.text_input(
                    "Bronze Database:",
                    value="ANALYTICS",
                    help="Target database for bronze layer"
                )
                bronze_schema = st.text_input(
                    "Bronze Schema:",
                    value="FINANCIAL_BRONZE",
                    help="Target schema for bronze layer"
                )
                bronze_table = st.text_input(
                    "Bronze Table:",
                    placeholder="Banks_2022_2023",
                    help="Target table name for bronze layer"
                )
        
        with silver_col:
            st.markdown("### 🥈 Silver Layer Configuration")
            st.markdown("Configure cleansed data transformation pipeline")
            
            col1, col2 = st.columns(2)
            with col1:
                silver_database_dev = st.text_input(
                    "Silver Database:",
                    value="ANALYTICS",
                    help="Target database for silver layer"
                )
                silver_schema = st.text_input(
                    "Silver Schema:",
                    value="FINANCIAL_SILVER",
                    help="Target schema for silver layer"
                )
                silver_table = st.text_input(
                    "Silver Table:",
                    placeholder="Banks_2022_2023",
                    help="Target table name for silver layer"
                )
                scd_type = st.selectbox(
                    "SCD Type:",
                    options=["1", "2", "3"],
                    index=1,
                    help="Slowly Changing Dimension type"
                )
            
            with col2:
                sequence_by = st.text_input(
                    "Sequence By:",
                    value="load_timestamp",
                    help="Column to use for change tracking"
                )
                keys_input = st.text_input(
                    "Keys (comma-separated):",
                    value="bank_id",
                    help="Primary keys for CDC (comma-separated)"
                )
                except_columns = st.text_input(
                    "Except Columns (comma-separated):",
                    value="Op,dmsTimestamp,_rescued_data",
                    help="Columns to exclude from CDC"
                )
            
            # Additional CDC Configuration
            col1, col2 = st.columns(2)
            with col1:
                columns_to_flatten = st.text_input(
                    "Columns to Flatten:",
                    value="src_products",
                    help="JSON columns to flatten (comma-separated)"
                )
            
            with col2:
                select_exp = st.text_input(
                    "Select Expressions:",
                    value="",
                    help="Custom select expressions (comma-separated)"
                )
        
        # Single Add Entry Button
        if st.form_submit_button("➕ Add Pipeline Entry", type="primary", use_container_width=True):
            # Validate required fields
            if not all([source_table, bronze_table, silver_table]):
                st.error("❌ Source table, Bronze table, and Silver table are required!")
            else:
                # Parse keys and except columns
                keys = [k.strip() for k in keys_input.split(",") if k.strip()]
                except_list = [c.strip() for c in except_columns.split(",") if c.strip()]
                flatten_list = [c.strip() for c in columns_to_flatten.split(",") if c.strip()]
                select_list = [s.strip() for s in select_exp.split(",") if s.strip()]
                
                # Create bronze entry
                bronze_entry = {
                    "source_table": source_table,
                    "source_path_dev": source_path_dev,
                    "reader_format": reader_format,
                    "bronze_database_dev": bronze_database_dev,
                    "bronze_schema": bronze_schema,
                    "bronze_table": bronze_table
                }
                
                # Create silver entry
                silver_entry = {
                    "bronze_database_dev": bronze_database_dev,
                    "bronze_schema": bronze_schema,
                    "bronze_table": bronze_table,
                    "silver_database_dev": silver_database_dev,
                    "silver_schema": silver_schema,
                    "silver_table": silver_table,
                    "silver_cdc_apply_changes": {
                        "keys": keys,
                        "sequence_by": sequence_by,
                        "scd_type": scd_type,
                        "except_column_list": except_list
                    },
                    "silver_transformation": {
                        "columns_to_flatten": flatten_list,
                        "select_exp": select_list
                    }
                }
                
                # Add both entries
                st.session_state.bronze_entries.append(bronze_entry)
                st.session_state.silver_entries.append(silver_entry)
                
                st.success(f"✅ Added pipeline entry: {source_table} → {bronze_table} → {silver_table}")
                st.rerun()
    
    st.divider()
    
    # Display current entries in table format
    if st.session_state.bronze_entries or st.session_state.silver_entries:
        st.subheader("📋 Current Pipeline Entries")
        
        # Create a table with two columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🥉 Bronze Pipeline Control")
            if st.session_state.bronze_entries:
                for i, entry in enumerate(st.session_state.bronze_entries):
                    with st.expander(f"Entry {i+1}: {entry['source_table']} → {entry['bronze_table']}", expanded=False):
                        st.json(entry)
                        if st.button(f"🗑️ Remove Bronze Entry {i+1}", key=f"remove_bronze_{i}"):
                            st.session_state.bronze_entries.pop(i)
                            st.rerun()
            else:
                st.info("No bronze entries added yet")
        
        with col2:
            st.markdown("### 🥈 Silver Pipeline Control")
            if st.session_state.silver_entries:
                for i, entry in enumerate(st.session_state.silver_entries):
                    with st.expander(f"Entry {i+1}: {entry['bronze_table']} → {entry['silver_table']}", expanded=False):
                        st.json(entry)
                        if st.button(f"🗑️ Remove Silver Entry {i+1}", key=f"remove_silver_{i}"):
                            st.session_state.silver_entries.pop(i)
                            st.rerun()
            else:
                st.info("No silver entries added yet")
        
        # Show paired entries summary
        if st.session_state.bronze_entries and st.session_state.silver_entries:
            st.markdown("### 📊 Pipeline Summary")
            st.markdown(f"**Total Pipeline Entries:** {len(st.session_state.bronze_entries)}")
            
            # Create a summary table
            import pandas as pd
            
            summary_data = []
            for i in range(min(len(st.session_state.bronze_entries), len(st.session_state.silver_entries))):
                bronze = st.session_state.bronze_entries[i]
                silver = st.session_state.silver_entries[i]
                summary_data.append({
                    "Entry": i + 1,
                    "Source Table": bronze['source_table'],
                    "Bronze Table": bronze['bronze_table'],
                    "Silver Table": silver['silver_table'],
                    "SCD Type": silver['silver_cdc_apply_changes']['scd_type']
                })
            
            if summary_data:
                df = pd.DataFrame(summary_data)
                st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # Generate JSON and Export
    if st.session_state.bronze_entries or st.session_state.silver_entries:
        st.subheader("📤 Export Configuration")
        
        # Generate the JSON structures
        pipeline_bronze_control_table = st.session_state.bronze_entries.copy()
        pipeline_silver_control_table = st.session_state.silver_entries.copy()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Bronze Control Table JSON**")
            st.code(json.dumps(pipeline_bronze_control_table, indent=2), language="json")
        
        with col2:
            st.markdown("**Silver Control Table JSON**")
            st.code(json.dumps(pipeline_silver_control_table, indent=2), language="json")
        
        # Complete configuration
        complete_config = {
            "global_config": {
                "horizon_enabled": horizon_enabled,
                "warehouse": warehouse_name,
                "timestamp": datetime.now().isoformat()
            },
            "pipeline_bronze_control_table": pipeline_bronze_control_table,
            "pipeline_silver_control_table": pipeline_silver_control_table
        }
        
        st.markdown("**Complete Configuration**")
        st.code(json.dumps(complete_config, indent=2), language="json")
        
        # Export buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("💾 Download Bronze JSON", use_container_width=True):
                st.download_button(
                    label="Download Bronze Configuration",
                    data=json.dumps(pipeline_bronze_control_table, indent=2),
                    file_name=f"bronze_control_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        with col2:
            if st.button("💾 Download Silver JSON", use_container_width=True):
                st.download_button(
                    label="Download Silver Configuration",
                    data=json.dumps(pipeline_silver_control_table, indent=2),
                    file_name=f"silver_control_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        with col3:
            if st.button("💾 Download Complete Config", use_container_width=True):
                st.download_button(
                    label="Download Complete Configuration",
                    data=json.dumps(complete_config, indent=2),
                    file_name=f"complete_pipeline_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        # Save to session state for deployment
        if st.button("✅ Save Configuration", type="primary", use_container_width=True):
            st.session_state.onboarding_data = complete_config
            st.session_state.onboarding_complete = True
            st.success("✅ Pipeline configuration saved successfully!")
            st.info("📋 Configuration saved. You can now proceed to the Deployment tab.")
    
    else:
        st.info("👆 Add at least one bronze or silver pipeline entry to generate configuration")


def render_deployment_tab():
    """Render the deployment configuration tab"""
    st.header("Step 2: Deployment")
    
    if not st.session_state.onboarding_complete:
        st.warning("⚠️ Please complete the Pipeline Configuration step first before proceeding with deployment.")
        return
    
    st.markdown("### Deploy Pipeline Configuration")
    
    # Show current configuration summary
    if st.session_state.onboarding_data:
        config = st.session_state.onboarding_data
        global_config = config.get("global_config", {})
        bronze_entries = config.get("pipeline_bronze_control_table", [])
        silver_entries = config.get("pipeline_silver_control_table", [])
        
        st.info(f"📊 Ready to deploy: {len(bronze_entries)} bronze entries, {len(silver_entries)} silver entries")
        
        # Display configuration summary
        with st.expander("📋 View Current Configuration", expanded=False):
            st.json(config)
    
    st.divider()
    
    # Deployment configuration
    st.subheader("🚀 Deployment Settings")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        horizon_enabled = st.radio(
            "Snowflake Horizon enabled?",
            options=[True, False],
            index=0 if global_config.get("horizon_enabled", True) else 1,
            help="Enable Snowflake Horizon for governance and data quality"
        )
    
    with col2:
        database_name = st.text_input(
            "Database name:",
            value="ANALYTICS",
            placeholder="Enter database name",
            help="Snowflake database for pipeline deployment"
        )
    
    with col3:
        warehouse_name = st.selectbox(
            "Warehouse:",
            options=["COMPUTE_WH", "SNOWFLAKE_LEARNING_WH", "SNOW_COMPUTE", "TRANSFORMING"],
            index=3,  # TRANSFORMING as default
            help="Select Snowflake warehouse for pipeline execution"
        )
    
    st.divider()
    
    # Pipeline deployment options
    st.subheader("⚙️ Pipeline Deployment Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        bronze_group_name = st.text_input(
            "Bronze Group Name:",
            value="A1",
            help="Group identifier for bronze layer ingestion"
        )
        
        bronze_table_name = st.text_input(
            "Bronze Control Table:",
            value="bronze_dataflowspec",
            help="Bronze layer dataflow specification table"
        )
    
    with col2:
        silver_group_name = st.text_input(
            "Silver Group Name:",
            value="A1",
            help="Group identifier for silver layer transformation"
        )
        
        silver_table_name = st.text_input(
            "Silver Control Table:",
            value="silver_dataflowspec",
            help="Silver layer dataflow specification table"
        )
    
    st.divider()
    
    # Pipeline naming and environment
    st.subheader("🏷️ Pipeline Naming")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        pipeline_name = st.text_input(
            "Pipeline Name:",
            value="",
            placeholder="e.g., financial_data_pipeline",
            help="Unique name for the Snowflake pipeline"
        )
    
    with col2:
        environment_name = st.selectbox(
            "Environment:",
            options=["dev", "staging", "prod"],
            index=2,  # prod as default
            help="Target environment for deployment"
        )
    
    with col3:
        dataflow_version = st.text_input(
            "Version:",
            value="v1",
            help="Version identifier for the dataflow specification"
        )
    
    st.divider()
    
    # Deployment actions
    st.subheader("🎯 Deployment Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🥉 Deploy Bronze Layer", type="primary", use_container_width=True):
            if not bronze_entries:
                st.error("❌ No bronze entries configured!")
            elif not pipeline_name:
                st.error("❌ Pipeline name is required!")
            else:
                st.success(f"✅ Bronze layer deployment initiated for {len(bronze_entries)} entries!")
                st.info(f"📊 Pipeline: {pipeline_name} | Database: {database_name} | Group: {bronze_group_name}")
                
                # Show bronze deployment details
                with st.expander("Bronze Deployment Details", expanded=True):
                    bronze_deployment = {
                        "pipeline_name": f"{pipeline_name}_bronze",
                        "database": database_name,
                        "warehouse": warehouse_name,
                        "group_name": bronze_group_name,
                        "control_table": bronze_table_name,
                        "entries": bronze_entries,
                        "horizon_enabled": horizon_enabled,
                        "environment": environment_name,
                        "version": dataflow_version,
                        "timestamp": datetime.now().isoformat()
                    }
                    st.json(bronze_deployment)
    
    with col2:
        if st.button("🥈 Deploy Silver Layer", type="primary", use_container_width=True):
            if not silver_entries:
                st.error("❌ No silver entries configured!")
            elif not pipeline_name:
                st.error("❌ Pipeline name is required!")
            else:
                st.success(f"✅ Silver layer deployment initiated for {len(silver_entries)} entries!")
                st.info(f"📊 Pipeline: {pipeline_name} | Database: {database_name} | Group: {silver_group_name}")
                
                # Show silver deployment details
                with st.expander("Silver Deployment Details", expanded=True):
                    silver_deployment = {
                        "pipeline_name": f"{pipeline_name}_silver",
                        "database": database_name,
                        "warehouse": warehouse_name,
                        "group_name": silver_group_name,
                        "control_table": silver_table_name,
                        "entries": silver_entries,
                        "horizon_enabled": horizon_enabled,
                        "environment": environment_name,
                        "version": dataflow_version,
                        "timestamp": datetime.now().isoformat()
                    }
                    st.json(silver_deployment)
    
    with col3:
        if st.button("🚀 Deploy Complete Pipeline", type="primary", use_container_width=True):
            if not pipeline_name:
                st.error("❌ Pipeline name is required!")
            elif not bronze_entries and not silver_entries:
                st.error("❌ No pipeline entries configured!")
            else:
                st.success("✅ Complete pipeline deployment initiated!")
                st.info(f"📊 Pipeline: {pipeline_name} | Database: {database_name}")
                
                # Show complete deployment details
                with st.expander("Complete Pipeline Deployment", expanded=True):
                    complete_deployment = {
                        "pipeline_name": pipeline_name,
                        "database": database_name,
                        "warehouse": warehouse_name,
                        "bronze_group": bronze_group_name,
                        "silver_group": silver_group_name,
                        "bronze_control_table": bronze_table_name,
                        "silver_control_table": silver_table_name,
                        "bronze_entries": bronze_entries,
                        "silver_entries": silver_entries,
                        "horizon_enabled": horizon_enabled,
                        "environment": environment_name,
                        "version": dataflow_version,
                        "timestamp": datetime.now().isoformat()
                    }
                    st.json(complete_deployment)
    
    st.divider()
    
    # SQL Generation
    if st.button("📝 Generate Deployment SQL", use_container_width=True):
        st.subheader("🔧 Generated SQL Commands")
        
        # Bronze layer SQL
        if bronze_entries:
            st.markdown("**Bronze Layer SQL:**")
            bronze_sql = f"""
-- Bronze Layer Deployment
CREATE OR REPLACE TASK {pipeline_name}_bronze_ingestion
    WAREHOUSE = {warehouse_name}
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    CALL bronze_ingestion_procedure('{bronze_group_name}');

-- Create streams for bronze tables
"""
            for entry in bronze_entries:
                bronze_sql += f"CREATE OR REPLACE STREAM {entry['bronze_schema']}.{entry['bronze_table']}_stream ON TABLE {entry['bronze_database_dev']}.{entry['bronze_schema']}.{entry['bronze_table']};\n"
            
            st.code(bronze_sql, language="sql")
        
        # Silver layer SQL
        if silver_entries:
            st.markdown("**Silver Layer SQL:**")
            silver_sql = f"""
-- Silver Layer Deployment
CREATE OR REPLACE TASK {pipeline_name}_silver_transformation
    WAREHOUSE = {warehouse_name}
    AFTER {pipeline_name}_bronze_ingestion
AS
    CALL silver_transformation_procedure('{silver_group_name}');

-- Start the tasks
ALTER TASK {pipeline_name}_bronze_ingestion RESUME;
ALTER TASK {pipeline_name}_silver_transformation RESUME;
"""
            st.code(silver_sql, language="sql")
        
        # Control table population
        st.markdown("**Control Table Population:**")
        control_sql = f"""
-- Populate bronze control table
INSERT INTO {database_name}.SNOWMETA_CONFIG.{bronze_table_name} VALUES
"""
        for entry in bronze_entries:
            control_sql += f"('{entry['source_table']}', '{entry['source_path_dev']}', '{entry['reader_format']}', '{entry['bronze_database_dev']}', '{entry['bronze_schema']}', '{entry['bronze_table']}'),\n"
        
        control_sql = control_sql.rstrip(",\n") + ";"
        st.code(control_sql, language="sql")


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
            st.success("✅ Pipeline Configuration Complete")
            config = st.session_state.onboarding_data
            global_config = config.get("global_config", {})
            bronze_entries = config.get("pipeline_bronze_control_table", [])
            silver_entries = config.get("pipeline_silver_control_table", [])
            
            st.markdown(f"""
            **Warehouse:** {global_config.get('warehouse', 'N/A')}  
            **Horizon:** {'✅' if global_config.get('horizon_enabled', False) else '❌'}  
            **Bronze Entries:** {len(bronze_entries)}  
            **Silver Entries:** {len(silver_entries)}
            """)
            
            if bronze_entries:
                st.markdown("**Bronze Tables:**")
                for entry in bronze_entries[:3]:  # Show first 3
                    st.markdown(f"- {entry.get('source_table', 'N/A')} → {entry.get('bronze_table', 'N/A')}")
                if len(bronze_entries) > 3:
                    st.markdown(f"... and {len(bronze_entries) - 3} more")
            
            if silver_entries:
                st.markdown("**Silver Tables:**")
                for entry in silver_entries[:3]:  # Show first 3
                    st.markdown(f"- {entry.get('bronze_table', 'N/A')} → {entry.get('silver_table', 'N/A')}")
                if len(silver_entries) > 3:
                    st.markdown(f"... and {len(silver_entries) - 3} more")
        else:
            st.info("📋 No pipeline configuration completed yet")
        
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
            st.session_state.bronze_entries = []
            st.session_state.silver_entries = []
            st.rerun()


if __name__ == "__main__":
    main()

