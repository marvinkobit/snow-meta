"""ControlTableReader class provides bronze/silver controltable reading features for Snowflake using Snowpark."""

import copy
import dataclasses
import json
import yaml
import logging
import ast
from typing import Dict, Any, List, Optional

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, VariantType, TimestampType

from src.controltable_spec import BronzeControlTableSpec, SilverControlTableSpec, ControlTableSpecUtils

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


class ControlTableReader:
    """ControlTableReader class provides bronze/silver controltable reading features for Snowflake using Snowpark.
    
    This class takes a pipeline_reader_config dictionary with bronze_control_table and/or silver_control_table
    keys and provides methods to return lists of BronzeControlTableSpec or SilverControlTableSpec objects.
    
    Example:
        # Create a reader with config
        pipeline_reader_config = {
            "bronze_control_table": "RAW.SNOWMETA_CONFIG.sample_bronze_control_table",
            "silver_control_table": "RAW.SNOWMETA_CONFIG.sample_silver_control_table",
            "group": "A1"
        }
        
        reader = ControlTableReader(session=session, pipeline_reader_config=pipeline_reader_config)
        
        # Get all bronze specs
        bronze_specs = reader.get_bronze_specs()
        
        # Get bronze specs filtered by group (uses "group" from config)
        bronze_specs_filtered = reader.get_bronze_specs()  # automatically uses group="A1" from config
        
        # Get all silver specs
        silver_specs = reader.get_silver_specs()
        
        # Override group from config
        silver_specs_other_group = reader.get_silver_specs(group="B1")
    """

    def __init__(self, session: Session, pipeline_reader_config: Dict[str, Any]):
        """Initialize ControlTableReader.
        
        Args:
            session: Snowflake Snowpark session
            pipeline_reader_config: Configuration dictionary with keys:
                - bronze_control_table: Full table name for bronze control table (optional)
                - silver_control_table: Full table name for silver control table (optional)
                - group: Data flow group to filter by (optional)
                - dataflow_ids: Comma-separated list of dataflow IDs to filter by (optional)
        
        Raises:
            ValueError: If neither bronze_control_table nor silver_control_table is provided
        """
        self.session = session
        self.pipeline_reader_config = pipeline_reader_config
        
        # Extract control table names
        self.bronze_control_table = pipeline_reader_config.get("bronze_control_table")
        self.silver_control_table = pipeline_reader_config.get("silver_control_table")
        
        # Extract filter parameters
        self.default_group = pipeline_reader_config.get("group")
        self.default_dataflow_ids = pipeline_reader_config.get("dataflow_ids")
        
        # Validate that at least one control table is provided
        if not self.bronze_control_table and not self.silver_control_table:
            raise ValueError(
                "pipeline_reader_config must contain at least one of 'bronze_control_table' or 'silver_control_table'"
            )
        
        logger.info(f"ControlTableReader initialized with config: {pipeline_reader_config}")

    def _get_control_table_specs(
        self,
        controltable_name: str,
        group: Optional[str] = None,
        dataflow_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get control table specs from Snowflake table.
        
        Args:
            controltable_name: Full table name to read from
            group: Optional data flow group to filter by
            dataflow_ids: Optional comma-separated list of dataflow IDs to filter by
            
        Returns:
            List of dictionaries containing control table spec data
        """
        # Build the query to get latest version for each dataFlowId
        query = f"""
        WITH ranked_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY dataFlowGroup, dataFlowId ORDER BY version DESC) as rn
            FROM {controltable_name}
        )
        SELECT * FROM ranked_data WHERE rn = 1
        """
        
        # Add filters if provided
        conditions = []
        if group:
            conditions.append(f"dataFlowGroup = '{group}'")
        
        if dataflow_ids:
            ids_list = dataflow_ids.split(',')
            ids_str = "', '".join([id.strip() for id in ids_list])
            conditions.append(f"dataFlowId IN ('{ids_str}')")
        
        if conditions:
            query = f"""
            WITH ranked_data AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY dataFlowGroup, dataFlowId ORDER BY version DESC) as rn
                FROM {controltable_name}
                WHERE {' AND '.join(conditions)}
            )
            SELECT * FROM ranked_data WHERE rn = 1
            """
        
        try:
            results = self.session.sql(query).collect()
            return [row.asDict() for row in results]
        except Exception as e:
            logger.error(f"Error reading control table {controltable_name}: {str(e)}")
            raise Exception(
                f"Failed to read control table '{controltable_name}': {str(e)}\n"
                f"Please check:\n"
                f"  1. The table exists and is accessible\n"
                f"  2. You have SELECT permission on the table\n"
                f"  3. The table has the expected schema"
            )

    def get_bronze_specs(
        self,
        group: Optional[str] = None,
        dataflow_ids: Optional[str] = None,
    ) -> List[BronzeControlTableSpec]:
        """Get bronze control table specs from the table.
        
        Args:
            group: Optional data flow group to filter by (overrides config value)
            dataflow_ids: Optional comma-separated list of dataflow IDs to filter by (overrides config value)
            
        Returns:
            List of BronzeControlTableSpec objects
            
        Raises:
            ValueError: If bronze_control_table is not configured
        """
        if not self.bronze_control_table:
            raise ValueError(
                "bronze_control_table is not configured in pipeline_reader_config. "
                "Cannot get bronze specs."
            )
        
        # Use provided parameters or fall back to config defaults
        filter_group = group if group is not None else self.default_group
        filter_dataflow_ids = dataflow_ids if dataflow_ids is not None else self.default_dataflow_ids
        
        control_table_spec_rows = self._get_control_table_specs(
            self.bronze_control_table,
            group=filter_group,
            dataflow_ids=filter_dataflow_ids
        )
        bronze_control_table_spec_list: List[BronzeControlTableSpec] = []
        
        for row in control_table_spec_rows:
            # Populate additional columns that may not be present
            target_row = ControlTableSpecUtils.populate_additional_df_cols(
                row,
                ControlTableSpecUtils.additional_bronze_df_columns
            )
            bronze_control_table_spec_list.append(BronzeControlTableSpec(**target_row))
        
        logger.info(f"Retrieved {len(bronze_control_table_spec_list)} bronze control table specs")
        return bronze_control_table_spec_list

    def get_silver_specs(
        self,
        group: Optional[str] = None,
        dataflow_ids: Optional[str] = None,
    ) -> List[SilverControlTableSpec]:
        """Get silver control table specs from the table.
        
        Args:
            group: Optional data flow group to filter by (overrides config value)
            dataflow_ids: Optional comma-separated list of dataflow IDs to filter by (overrides config value)
            
        Returns:
            List of SilverControlTableSpec objects
            
        Raises:
            ValueError: If silver_control_table is not configured
        """
        if not self.silver_control_table:
            raise ValueError(
                "silver_control_table is not configured in pipeline_reader_config. "
                "Cannot get silver specs."
            )
        
        # Use provided parameters or fall back to config defaults
        filter_group = group if group is not None else self.default_group
        filter_dataflow_ids = dataflow_ids if dataflow_ids is not None else self.default_dataflow_ids
        
        control_table_spec_rows = self._get_control_table_specs(
            self.silver_control_table,
            group=filter_group,
            dataflow_ids=filter_dataflow_ids
        )
        silver_control_table_spec_list: List[SilverControlTableSpec] = []
        
        for row in control_table_spec_rows:
            # Populate additional columns that may not be present
            target_row = ControlTableSpecUtils.populate_additional_df_cols(
                row,
                ControlTableSpecUtils.additional_silver_df_columns
            )
            silver_control_table_spec_list.append(SilverControlTableSpec(**target_row))
        
        logger.info(f"Retrieved {len(silver_control_table_spec_list)} silver control table specs")
        return silver_control_table_spec_list