"""ControlTableReader class provides bronze/silver controltable reading features for Snowflake using Snowpark."""

import logging
from typing import List

from snowflake.snowpark import Session

from src.controltable_spec import BronzeControlTableSpec, SilverControlTableSpec, ControlTableSpecUtils

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