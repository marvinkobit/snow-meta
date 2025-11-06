"""OnboardControlTable class provides bronze/silver onboarding features for Snowflake using Snowpark."""

import dataclasses
import logging
from typing import Dict, Any, List
from datetime import datetime

from snowflake.snowpark import Session
import pandas as pd

from snowmeta.controltable_spec import BronzeColumnarSpec, SilverColumnarSpec, GlobalConfig

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


class OnboardControlTable:
    """OnboardControlTable class provides bronze/silver onboarding features for Snowflake.
    
    This class writes control table specs in columnar format (one row per table).
    
    Example:
        config_data = {
            "global_config": {
                "pipeline_name": "my_pipeline",
                "warehouse": "MY_WAREHOUSE",
                "table_properties": ["ENABLE_SCHEMA_EVOLUTION=TRUE"],
                "metadata_option": ["METADATA$FILENAME"],
                "timestamp": "2025-01-01T00:00:00"
            },
            "pipeline_bronze_control_config": [
                {
                    "source_table": "customer",
                    "source_path_dev": "@RAW.PUBLIC.LANDING/customers/",
                    "reader_format": "CSV",
                    "bronze_database_dev": "ANALYTICS",
                    "bronze_schema": "BRONZE",
                    "bronze_table": "CUSTOMER"
                }
            ],
            "pipeline_silver_control_config": [
                {
                    "bronze_database_dev": "ANALYTICS",
                    "bronze_schema": "BRONZE",
                    "bronze_table": "CUSTOMER",
                    "silver_database_dev": "ANALYTICS",
                    "silver_schema": "SILVER",
                    "silver_table": "CUSTOMER",
                    "silver_cdc_apply_changes": {
                        "keys": ["CUSTOMER_ID"],
                        "sequence_by": "_INGEST_TIMESTAMP",
                        "scd_type": "1"
                    }
                }
            ]
        }
        
        params = {
            "database": "RAW",
            "schema": "SNOWMETA_CONFIG",
            "bronze_control_table": "bronze_control_table",
            "silver_control_table": "silver_control_table",
            "global_config_table": "global_config_table",
            "overwrite": "True"
        }
        
        onboard = OnboardControlTable(session, params, onboarding_data=config_data)
        onboard.onboard_controltable_specs()
    """

    def __init__(self, session: Session, dict_obj: Dict[str, Any], 
                 onboarding_data: Dict[str, Any], horizon_enabled=True):
        """Onboard ControlTable Constructor for Snowflake using Snowpark.
        
        Args:
            session: Snowflake Snowpark session
            dict_obj: Dictionary containing onboarding configuration:
                - database: Target database name
                - schema: Target schema name
                - bronze_control_table: Bronze control table name
                - silver_control_table: Silver control table name
                - global_config_table: Global config table name (optional)
                - overwrite: Whether to overwrite existing data ("True" or "False")
            onboarding_data: JSON dict with pipeline config in columnar format.
                            Must have structure: {
                                "global_config": {...},
                                "pipeline_bronze_control_config": [...],
                                "pipeline_silver_control_config": [...]
                            }
            horizon_enabled: Whether Horizon is enabled (default: True)
        """
        if onboarding_data is None:
            raise ValueError("onboarding_data parameter is required")
        
        if not isinstance(onboarding_data, dict):
            raise ValueError("onboarding_data must be a dictionary")
        
        if "pipeline_bronze_control_config" not in onboarding_data:
            raise ValueError("onboarding_data must contain 'pipeline_bronze_control_config'")
        
        if "pipeline_silver_control_config" not in onboarding_data:
            raise ValueError("onboarding_data must contain 'pipeline_silver_control_config'")
        
        self.session = session
        self.dict_obj = dict_obj
        self.onboarding_data = onboarding_data
        self.horizon_enabled = horizon_enabled

    @staticmethod
    def __validate_dict_attributes(attributes: List[str], dict_obj: Dict[str, Any]):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes: List of required attributes
            dict_obj: Dictionary to validate

        Raises:
            ValueError: If required attributes are missing
        """
        missing_attrs = set(attributes).difference(set(dict_obj.keys()))
        
        if missing_attrs:
            logger.error(f"Missing required attributes: {missing_attrs}")
            raise ValueError(f"Missing required attributes: {missing_attrs}")

    def onboard_controltable_specs(self):
        """
        Onboard control table specs for bronze, silver, and global config in columnar format.
        
        This method writes the onboarding data directly to control tables in columnar format
        (one row per table).
        """
        required_attributes = [
            "database",
            "schema",
            "bronze_control_table",
            "silver_control_table",
            "overwrite",
        ]
        self.__validate_dict_attributes(required_attributes, self.dict_obj)
        
        # Onboard bronze, silver, and global config
        self.onboard_bronze_control_table_spec()
        self.onboard_silver_control_table_spec()
        
        # Onboard global config if provided
        if "global_config_table" in self.dict_obj and "global_config" in self.onboarding_data:
            self.onboard_global_config()

    def onboard_bronze_control_table_spec(self):
        """Onboard bronze control table spec in columnar format."""
        bronze_configs = self.onboarding_data.get("pipeline_bronze_control_config", [])
        
        if not bronze_configs:
            logger.warning("No bronze control configs found in onboarding_data")
            return
        
        # Convert to DataFrame with BronzeColumnarSpec structure
        bronze_rows = []
        for bronze_config in bronze_configs:
            # Create BronzeColumnarSpec object
            bronze_spec = BronzeColumnarSpec(
                source_table=bronze_config.get("source_table"),
                source_path_dev=bronze_config.get("source_path_dev"),
                reader_format=bronze_config.get("reader_format", "CSV"),
                load_strategy=bronze_config.get("load_strategy"),
                table_properties=bronze_config.get("table_properties"),
                bronze_database_dev=bronze_config.get("bronze_database_dev"),
                bronze_schema=bronze_config.get("bronze_schema"),
                bronze_table=bronze_config.get("bronze_table"),
                variant_load=bronze_config.get("variant_load"),
                variant_column_name=bronze_config.get("variant_column_name")
            )
            
            # Convert dataclass to dict
            bronze_dict = dataclasses.asdict(bronze_spec)
            bronze_rows.append(bronze_dict)
        
        # Create DataFrame
        bronze_df = pd.DataFrame(bronze_rows)
        
        # Write to Snowflake
        database = self.dict_obj["database"]
        schema = self.dict_obj["schema"]
        table = self.dict_obj["bronze_control_table"]
        full_table_name = f"{database}.{schema}.{table}"
        
        mode = "overwrite" if self.dict_obj.get("overwrite", "False") == "True" else "append"
        self.__write_dataframe_to_snowflake(bronze_df, full_table_name, mode)
        
        logger.info(f"Onboarded {len(bronze_rows)} bronze control table specs to {full_table_name}")

    def onboard_silver_control_table_spec(self):
        """Onboard silver control table spec in columnar format."""
        silver_configs = self.onboarding_data.get("pipeline_silver_control_config", [])
        
        if not silver_configs:
            logger.warning("No silver control configs found in onboarding_data")
            return
        
        # Convert to DataFrame with SilverColumnarSpec structure
        silver_rows = []
        for silver_config in silver_configs:
            # Handle CDC apply changes - map columns_to_track to column_list if present
            cdc_apply_changes = silver_config.get("silver_cdc_apply_changes")
            if cdc_apply_changes and isinstance(cdc_apply_changes, dict):
                if "columns_to_track" in cdc_apply_changes and "column_list" not in cdc_apply_changes:
                    cdc_apply_changes = cdc_apply_changes.copy()
                    cdc_apply_changes["column_list"] = cdc_apply_changes.pop("columns_to_track")
            
            # Create SilverColumnarSpec object
            silver_spec = SilverColumnarSpec(
                bronze_database_dev=silver_config.get("bronze_database_dev"),
                bronze_schema=silver_config.get("bronze_schema"),
                bronze_table=silver_config.get("bronze_table"),
                silver_database_dev=silver_config.get("silver_database_dev"),
                silver_schema=silver_config.get("silver_schema"),
                silver_table=silver_config.get("silver_table"),
                silver_cdc_apply_changes=cdc_apply_changes,
                silver_transformation_json=silver_config.get("silver_transformation_json")
            )
            
            # Convert dataclass to dict
            silver_dict = dataclasses.asdict(silver_spec)
            silver_rows.append(silver_dict)
        
        # Create DataFrame
        silver_df = pd.DataFrame(silver_rows)
        
        # Write to Snowflake
        database = self.dict_obj["database"]
        schema = self.dict_obj["schema"]
        table = self.dict_obj["silver_control_table"]
        full_table_name = f"{database}.{schema}.{table}"
        
        mode = "overwrite" if self.dict_obj.get("overwrite", "False") == "True" else "append"
        self.__write_dataframe_to_snowflake(silver_df, full_table_name, mode)
        
        logger.info(f"Onboarded {len(silver_rows)} silver control table specs to {full_table_name}")

    def onboard_global_config(self):
        """Onboard global config table."""
        global_config_data = self.onboarding_data.get("global_config", {})
        
        if not global_config_data:
            logger.warning("No global_config found in onboarding_data")
            return
        
        # Create GlobalConfig object
        # Check if timestamp is in the data, otherwise use current timestamp
        timestamp = global_config_data.get("timestamp")
        if not timestamp:
            timestamp = datetime.now().isoformat()
        
        global_config = GlobalConfig(
            pipeline_name=global_config_data.get("pipeline_name", ""),
            warehouse=global_config_data.get("warehouse", ""),
            table_properties=global_config_data.get("table_properties", []),
            metadata_option=global_config_data.get("metadata_option", []),
            timestamp=timestamp
        )
        
        # Convert to dict and create DataFrame
        global_dict = dataclasses.asdict(global_config)
        global_df = pd.DataFrame([global_dict])
        
        # Write to Snowflake
        database = self.dict_obj["database"]
        schema = self.dict_obj["schema"]
        table = self.dict_obj["global_config_table"]
        full_table_name = f"{database}.{schema}.{table}"
        
        mode = "overwrite" if self.dict_obj.get("overwrite", "False") == "True" else "append"
        self.__write_dataframe_to_snowflake(global_df, full_table_name, mode)
        
        logger.info(f"Onboarded global config to {full_table_name}")

    def __write_dataframe_to_snowflake(self, df: pd.DataFrame, table_name: str, mode: str):
        """Write DataFrame to Snowflake table using Snowpark.
        
        Args:
            df: pandas DataFrame to write
            table_name: Full table name (database.schema.table)
            mode: Write mode ("overwrite" or "append")
        """
        try:
            if mode == "overwrite":
                # Drop and recreate table
                self.session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
            
            # Convert pandas DataFrame to Snowpark DataFrame and write to table
            snowpark_df = self.session.create_dataframe(df)
            snowpark_df.write.mode("overwrite" if mode == "overwrite" else "append").save_as_table(table_name)
            logger.info(f"DataFrame written to {table_name} with mode {mode}")
        except Exception as e:
            logger.error(f"Error writing DataFrame to {table_name}: {str(e)}")
            raise
