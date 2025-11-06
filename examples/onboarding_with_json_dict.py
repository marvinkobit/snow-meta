# Snowflake-META Onboarding Example with JSON Dict (Columnar Format)
# This script demonstrates how to onboard control table specs using a JSON dict directly
# in columnar format (one row per table)

import json
from snowflake.snowpark import Session
from snowmeta.onboard_controltable import OnboardControlTable

# Example: Load config from a JSON file (or you can define it directly as a dict)
with open('config.json', 'r') as f:
    config_data = json.load(f)

# Get Snowpark session
session = Session.builder.getOrCreate()
print("Snowpark session established successfully!")

# Define onboarding parameters
onboarding_params = {
    "database": "RAW",
    "schema": "SNOWMETA_CONFIG",
    "bronze_control_table": "bronze_control_table",
    "silver_control_table": "silver_control_table",
    "global_config_table": "global_config_table",  # Optional
    "overwrite": "True"  # "True" to overwrite, "False" to append
}

# Create the OnboardControlTable instance with onboarding_data parameter
# onboarding_data is required and must be a dict with columnar format
onboard = OnboardControlTable(
    session=session,
    dict_obj=onboarding_params,
    onboarding_data=config_data  # Pass the JSON dict directly (required)
)

print("OnboardControlTable instance created successfully!")

# Run the onboarding process
try:
    print("Starting onboarding process...")
    onboard.onboard_controltable_specs()
    print("✅ Onboarding completed successfully!")
    print("   - Bronze control table specs written in columnar format")
    print("   - Silver control table specs written in columnar format")
    if "global_config_table" in onboarding_params:
        print("   - Global config written")
except Exception as e:
    print(f"❌ Onboarding failed: {str(e)}")
    raise

# Alternative: You can also define config_data directly as a dict
# config_data = {
#     "global_config": {
#         "pipeline_name": "my_pipeline",
#         "warehouse": "MY_WAREHOUSE",
#         "table_properties": ["ENABLE_SCHEMA_EVOLUTION=TRUE"],
#         "metadata_option": ["METADATA$FILENAME"],
#         "timestamp": "2025-01-01T00:00:00"
#     },
#     "pipeline_bronze_control_config": [
#         {
#             "source_table": "customer",
#             "source_path_dev": "@RAW.PUBLIC.LANDING/customers/",
#             "reader_format": "CSV",
#             "bronze_database_dev": "ANALYTICS",
#             "bronze_schema": "BRONZE",
#             "bronze_table": "CUSTOMER"
#         }
#     ],
#     "pipeline_silver_control_config": [
#         {
#             "bronze_database_dev": "ANALYTICS",
#             "bronze_schema": "BRONZE",
#             "bronze_table": "CUSTOMER",
#             "silver_database_dev": "ANALYTICS",
#             "silver_schema": "SILVER",
#             "silver_table": "CUSTOMER",
#             "silver_cdc_apply_changes": {
#                 "keys": ["CUSTOMER_ID"],
#                 "sequence_by": "_INGEST_TIMESTAMP",
#                 "scd_type": "1"
#             }
#         }
#     ]
# }

