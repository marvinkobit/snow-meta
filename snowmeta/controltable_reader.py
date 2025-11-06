"""ControlTableReader class provides bronze/silver controltable reading features for Snowflake using Snowpark."""

import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from snowflake.snowpark import Session

from snowmeta.controltable_spec import (
    BronzeControlTableSpec, 
    SilverControlTableSpec, 
    ControlTableSpecUtils,
    BronzeColumnarSpec,
    SilverColumnarSpec,
    GlobalConfig
)

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


class ControlTableReader:
    """ControlTableReader reads bronze/silver control tables and returns lists of ControlTableSpec objects.
    
    Supports both columnar format (one row per table) and legacy format.
    
    Example:
        reader = ControlTableReader(
            session=session,
            bronze_control_table="RAW.SNOWMETA_CONFIG.sample_bronze_control_table",
            silver_control_table="RAW.SNOWMETA_CONFIG.sample_silver_control_table",
            global_config_table="RAW.SNOWMETA_CONFIG.global_config_table"
        )
        
        # Legacy format
        bronze_specs = reader.get_bronze_control_table()
        silver_specs = reader.get_silver_control_table()
        
        # Columnar format - returns pipeline config structure
        pipeline_config = reader.get_pipeline_config(pipeline_name="vista_puller")
    """

    def __init__(self, session: Session, bronze_control_table: str = None, 
                 silver_control_table: str = None, global_config_table: str = None):
        """Initialize ControlTableReader.
        
        Args:
            session: Snowflake Snowpark session
            bronze_control_table: Full table name for bronze control table
            silver_control_table: Full table name for silver control table
            global_config_table: Full table name for global config table (optional)
        """
        self.session = session
        self.bronze_control_table = bronze_control_table
        self.silver_control_table = silver_control_table
        self.global_config_table = global_config_table

    def get_bronze_control_table(self) -> List[BronzeControlTableSpec]:
        """Read bronze control table and return list of BronzeControlTableSpec objects.
        
        Returns:
            List of BronzeControlTableSpec objects
        """
        if not self.bronze_control_table:
            raise ValueError("bronze_control_table not configured")
        
        # Read the table
        df = self.session.table(self.bronze_control_table)
        rows = df.collect()
        
        # Convert rows to BronzeControlTableSpec objects
        bronze_specs = []
        for row in rows:
            row_dict = row.asDict()
            # Populate additional columns that may not be present
            row_dict = ControlTableSpecUtils.populate_additional_df_cols(
                row_dict,
                ControlTableSpecUtils.additional_bronze_df_columns
            )
            bronze_specs.append(BronzeControlTableSpec(**row_dict))
        
        logger.info(f"Retrieved {len(bronze_specs)} bronze control table specs")
        return bronze_specs

    def get_silver_control_table(self) -> List[SilverControlTableSpec]:
        """Read silver control table and return list of SilverControlTableSpec objects.
        
        Returns:
            List of SilverControlTableSpec objects
        """
        if not self.silver_control_table:
            raise ValueError("silver_control_table not configured")
        
        # Read the table
        df = self.session.table(self.silver_control_table)
        rows = df.collect()
        
        # Convert rows to SilverControlTableSpec objects
        silver_specs = []
        for row in rows:
            row_dict = row.asDict()
            # Populate additional columns that may not be present
            row_dict = ControlTableSpecUtils.populate_additional_df_cols(
                row_dict,
                ControlTableSpecUtils.additional_silver_df_columns
            )
            silver_specs.append(SilverControlTableSpec(**row_dict))
        
        logger.info(f"Retrieved {len(silver_specs)} silver control table specs")
        return silver_specs

    def get_bronze_columnar_control_table(self) -> List[BronzeColumnarSpec]:
        """Read bronze control table in columnar format and return list of BronzeColumnarSpec objects.
        
        Returns:
            List of BronzeColumnarSpec objects (one per table/row)
        """
        if not self.bronze_control_table:
            raise ValueError("bronze_control_table not configured")
        
        # Read the table
        df = self.session.table(self.bronze_control_table)
        rows = df.collect()
        
        # Convert rows to BronzeColumnarSpec objects
        bronze_specs = []
        for row in rows:
            row_dict = row.asDict()
            # Normalize column names - create mapping of lowercase to original
            row_dict_normalized = {k.lower(): v for k, v in row_dict.items()}
            
            # Create BronzeColumnarSpec, handling optional fields
            spec_dict = {}
            for field_name in BronzeColumnarSpec.__dataclass_fields__:
                # Try exact match first (lowercase)
                if field_name in row_dict_normalized:
                    value = row_dict_normalized[field_name]
                    # Handle list fields that might be stored as JSON strings
                    if field_name == 'table_properties' and isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    spec_dict[field_name] = value
                else:
                    # Try uppercase version without underscores
                    field_upper = field_name.upper().replace('_', '')
                    for key, value in row_dict.items():
                        if key.upper().replace('_', '') == field_upper:
                            spec_dict[field_name] = value
                            break
            
            bronze_specs.append(BronzeColumnarSpec(**spec_dict))
        
        logger.info(f"Retrieved {len(bronze_specs)} bronze columnar control table specs")
        return bronze_specs

    def get_silver_columnar_control_table(self) -> List[SilverColumnarSpec]:
        """Read silver control table in columnar format and return list of SilverColumnarSpec objects.
        
        Returns:
            List of SilverColumnarSpec objects (one per table/row)
        """
        if not self.silver_control_table:
            raise ValueError("silver_control_table not configured")
        
        # Read the table
        df = self.session.table(self.silver_control_table)
        rows = df.collect()
        
        # Convert rows to SilverColumnarSpec objects
        silver_specs = []
        for row in rows:
            row_dict = row.asDict()
            # Normalize column names - create mapping of lowercase to original
            row_dict_normalized = {k.lower(): v for k, v in row_dict.items()}
            
            # Handle nested JSON fields (silver_cdc_apply_changes, silver_transformation_json)
            spec_dict = {}
            for field_name in SilverColumnarSpec.__dataclass_fields__:
                # Try exact match first (lowercase)
                if field_name in row_dict_normalized:
                    value = row_dict_normalized[field_name]
                    # Parse JSON strings if needed
                    if field_name in ['silver_cdc_apply_changes', 'silver_transformation_json']:
                        if isinstance(value, str):
                            try:
                                spec_dict[field_name] = json.loads(value)
                            except (json.JSONDecodeError, TypeError):
                                spec_dict[field_name] = value
                        elif isinstance(value, dict):
                            spec_dict[field_name] = value
                        else:
                            spec_dict[field_name] = value
                    else:
                        spec_dict[field_name] = value
                else:
                    # Try uppercase version without underscores
                    field_upper = field_name.upper().replace('_', '')
                    for key, value in row_dict.items():
                        if key.upper().replace('_', '') == field_upper:
                            # Parse JSON strings for nested fields
                            if field_name in ['silver_cdc_apply_changes', 'silver_transformation_json']:
                                if isinstance(value, str):
                                    try:
                                        spec_dict[field_name] = json.loads(value)
                                    except (json.JSONDecodeError, TypeError):
                                        spec_dict[field_name] = value
                                else:
                                    spec_dict[field_name] = value
                            else:
                                spec_dict[field_name] = value
                            break
            
            silver_specs.append(SilverColumnarSpec(**spec_dict))
        
        logger.info(f"Retrieved {len(silver_specs)} silver columnar control table specs")
        return silver_specs

    def get_global_config(self, pipeline_name: str) -> Optional[GlobalConfig]:
        """Get global config for a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            GlobalConfig object or None if not found
        """
        if self.global_config_table:
            try:
                df = self.session.table(self.global_config_table)
                # Filter by pipeline_name
                df = df.filter(df["pipeline_name"] == pipeline_name)
                rows = df.collect()
                
                if rows:
                    row_dict = rows[0].asDict()
                    row_dict_lower = {k.lower(): v for k, v in row_dict.items()}
                    
                    # Parse list fields if they're stored as JSON strings
                    if 'table_properties' in row_dict_lower and isinstance(row_dict_lower['table_properties'], str):
                        row_dict_lower['table_properties'] = json.loads(row_dict_lower['table_properties'])
                    if 'metadata_option' in row_dict_lower and isinstance(row_dict_lower['metadata_option'], str):
                        row_dict_lower['metadata_option'] = json.loads(row_dict_lower['metadata_option'])
                    
                    return GlobalConfig(**{k: v for k, v in row_dict_lower.items() 
                                          if k in GlobalConfig.__dataclass_fields__})
            except Exception as e:
                logger.warning(f"Could not read global config table: {e}")
        
        # If no global config table or not found, create a default one
        return GlobalConfig(
            pipeline_name=pipeline_name,
            warehouse="",
            table_properties=[],
            metadata_option=[],
            timestamp=datetime.now().isoformat()
        )

    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get complete pipeline configuration in the desired JSON structure.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Dictionary with structure:
            {
                "global_config": {...},
                "pipeline_bronze_control_config": [...],
                "pipeline_silver_control_config": [...]
            }
        """
        # Get global config
        global_config = self.get_global_config(pipeline_name)
        
        # Get bronze columnar specs
        bronze_specs = self.get_bronze_columnar_control_table()
        bronze_config_list = []
        for spec in bronze_specs:
            spec_dict = {
                "source_table": spec.source_table,
                "source_path_dev": spec.source_path_dev,
                "reader_format": spec.reader_format,
            }
            # Only include fields that have values (not None)
            if spec.load_strategy is not None:
                spec_dict["load_strategy"] = spec.load_strategy
            if spec.table_properties is not None:
                spec_dict["table_properties"] = spec.table_properties
            if spec.bronze_database_dev is not None:
                spec_dict["bronze_database_dev"] = spec.bronze_database_dev
            if spec.bronze_schema is not None:
                spec_dict["bronze_schema"] = spec.bronze_schema
            if spec.bronze_table is not None:
                spec_dict["bronze_table"] = spec.bronze_table
            if spec.variant_load is not None:
                spec_dict["variant_load"] = spec.variant_load
            if spec.variant_column_name is not None:
                spec_dict["variant_column_name"] = spec.variant_column_name
            bronze_config_list.append(spec_dict)
        
        # Get silver columnar specs
        silver_specs = self.get_silver_columnar_control_table()
        silver_config_list = []
        for spec in silver_specs:
            spec_dict = {
                "bronze_database_dev": spec.bronze_database_dev,
                "bronze_schema": spec.bronze_schema,
                "bronze_table": spec.bronze_table,
                "silver_database_dev": spec.silver_database_dev,
                "silver_schema": spec.silver_schema,
                "silver_table": spec.silver_table,
            }
            # Only include optional fields if they have values
            if spec.silver_cdc_apply_changes is not None:
                spec_dict["silver_cdc_apply_changes"] = spec.silver_cdc_apply_changes
            if spec.silver_transformation_json is not None:
                spec_dict["silver_transformation_json"] = spec.silver_transformation_json
            silver_config_list.append(spec_dict)
        
        # Build the output structure
        result = {
            "global_config": {
                "pipeline_name": global_config.pipeline_name,
                "warehouse": global_config.warehouse,
                "table_properties": global_config.table_properties,
                "metadata_option": global_config.metadata_option,
                "timestamp": global_config.timestamp
            },
            "pipeline_bronze_control_config": bronze_config_list,
            "pipeline_silver_control_config": silver_config_list
        }
        
        logger.info(f"Retrieved pipeline config for {pipeline_name}: "
                   f"{len(bronze_config_list)} bronze configs, "
                   f"{len(silver_config_list)} silver configs")
        
        return result

    def bringyourownschema(self, stage_path: str) -> Dict[str, Any]:
        """Read a JSON schema file from a Snowflake stage and return it as a struct object.
        
        Args:
            stage_path: Full path to the JSON schema file in the stage (e.g., '@RAW.ETBANKSFINANCIAL.S3_LANDING_CI/myschemafiles/sample_customer_schema.json')
            
        Returns:
            Dictionary containing the parsed JSON schema structure
            
        Example:
            >>> reader = ControlTableReader(session)
            >>> schema = reader.bringyourownschema('@RAW.ETBANKSFINANCIAL.S3_LANDING_CI/myschemafiles/sample_customer_schema.json')
            >>> print(schema['type'])  # 'struct'
            >>> print(len(schema['fields']))  # Number of fields
        """
        try:
            # Read the JSON file from the stage
            logger.info(f"Reading schema from stage: {stage_path}")
            
            # Use Snowpark DataFrame to read JSON from stage
            df = self.session.read.json(stage_path)
            
            # Collect the data
            result = df.collect()
            
            if not result:
                raise ValueError(f"No content found in stage file: {stage_path}")
            
            # Convert the first row to dictionary
            row_dict = result[0].asDict()
            
            # The keys might be uppercase (TYPE, FIELDS), so normalize them
            schema_dict = {}
            for key, value in row_dict.items():
                lower_key = key.lower()
                
                # If the value is a string representation of JSON, parse it
                if isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
                    try:
                        schema_dict[lower_key] = json.loads(value)
                    except:
                        schema_dict[lower_key] = value
                else:
                    schema_dict[lower_key] = value
            
            logger.info(f"Successfully read schema with {len(schema_dict.get('fields', []))} fields")
            return schema_dict["$1"]
            
        except Exception as e:
            logger.error(f"Failed to read schema from {stage_path}: {e}")
            raise
    
    def generate_create_table_from_schema(self, schema_dict: Dict[str, Any], table_name: str) -> str:
        """
        Generate CREATE TABLE SQL from JSON schema dictionary.
        
        Args:
            schema_dict: Dictionary containing the parsed JSON schema
            table_name: Full table name (database.schema.table)
            
        Returns:
            SQL string for creating the table
        """
        columns = []
        
        # Process each field from the schema
        for field in schema_dict.get('fields', []):
            column_name = field['name']
            column_type = field['type']  # Already in Snowflake format
            nullable = "" if field.get('nullable', True) else " NOT NULL"
            columns.append(f'  "{column_name}" {column_type}{nullable}')
        
        # Add metadata columns (these are typically added by the pipeline)
        columns.append('  "_SRC_FILENAME" VARCHAR')
        columns.append('  "_SRC_FILE_ROW_NUMBER" NUMBER')
        columns.append('  "_FILE_RECEIVED_AT" TIMESTAMP_NTZ')
        columns.append('  "_INGESTED_AT" TIMESTAMP_NTZ')
        
        
        # Deduplicate columns based on column name (case insensitive)
        seen = set()
        deduped_columns = []
        for col in columns:
            # Assume column name is inside double quotes at start of col string
            name_start = col.find('"') + 1
            name_end = col.find('"', name_start)
            col_name = col[name_start:name_end].lower() if name_start > 0 and name_end > 0 else col.lower()
            if col_name not in seen:
                deduped_columns.append(col)
                seen.add(col_name)
        columns = deduped_columns
        
        columns_sql = ',\n'.join(columns)
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                            {columns_sql}
                            );
                            
                            ALTER TABLE {table_name} SET ENABLE_SCHEMA_EVOLUTION = TRUE; 

                            CREATE STREAM IF NOT EXISTS stream_{table_name} ON TABLE {table_name};
                            """
        
        return create_table_sql      