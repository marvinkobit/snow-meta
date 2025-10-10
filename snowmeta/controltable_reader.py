"""ControlTableReader class provides bronze/silver controltable reading features for Snowflake using Snowpark."""

import logging
import json
from typing import List, Dict, Any
from snowflake.snowpark import Session

from snowmeta.controltable_spec import BronzeControlTableSpec, SilverControlTableSpec, ControlTableSpecUtils

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


class ControlTableReader:
    """ControlTableReader reads bronze/silver control tables and returns lists of ControlTableSpec objects.
    
    Example:
        reader = ControlTableReader(
            session=session,
            bronze_control_table="RAW.SNOWMETA_CONFIG.sample_bronze_control_table",
            silver_control_table="RAW.SNOWMETA_CONFIG.sample_silver_control_table"
        )
        
        bronze_specs = reader.get_bronze_control_table()
        silver_specs = reader.get_silver_control_table()
    """

    def __init__(self, session: Session, bronze_control_table: str = None, silver_control_table: str = None):
        """Initialize ControlTableReader.
        
        Args:
            session: Snowflake Snowpark session
            bronze_control_table: Full table name for bronze control table
            silver_control_table: Full table name for silver control table
        """
        self.session = session
        self.bronze_control_table = bronze_control_table
        self.silver_control_table = silver_control_table

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
        columns.append('  "SRC_FILENAME" VARCHAR')
        columns.append('  "SRC_FILE_ROW_NUMBER" NUMBER')
        
        columns_sql = ',\n'.join(columns)
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
);"""
        
        return create_table_sql      