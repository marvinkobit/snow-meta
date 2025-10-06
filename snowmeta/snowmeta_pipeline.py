
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



class SnowmetaPipeline:
    
    def __init__(self, session: Session):
        """
        Initialize the SnowmetaPipeline.
        
        Args:
            session: Active Snowflake Snowpark session
        """
        self.session = session
        self.logger = logging.getLogger(__name__)
    
    def invoke_bronze_pipeline(self, pipeline_data: List[Dict[str, str]]) -> None:
        """
        Execute a data pipeline that creates tables and loads data in Snowflake.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration.
                          Each dict should have:
                          - source_table: Name of the source table/file
                          - source_path_dev: Stage path for source files
                          - reader_format: File format (e.g., 'CSV', 'JSON', 'PARQUET')
                          - bronze_database_dev: Target database name
                          - bronze_schema: Target schema name
                          - bronze_table: Target table name
            
        Returns:
            None
            
        Example:
            >>> pipeline = SnowmetaPipeline(session)
            >>> pipeline_data = [
            ...     {
            ...         "source_table": "Banks_2022_2023_raw",
            ...         "source_path_dev": "@RAW.ETBANKSFINANCIAL.LANDING/",
            ...         "reader_format": "CSV",
            ...         "bronze_database_dev": "ANALYTICS",
            ...         "bronze_schema": "FINANCIAL_BRONZE",
            ...         "bronze_table": "Banks_2022_2023_raw"
            ...     }
            ... ]
            >>> pipeline.invoke_pipeline(pipeline_data)
        """
        for pipeline_index, pipeline_config in enumerate(pipeline_data, 1):
            source_table = pipeline_config["source_table"]
            source_path = pipeline_config["source_path_dev"]
            file_format = pipeline_config["reader_format"]
            bronze_database = pipeline_config["bronze_database_dev"]
            bronze_schema = pipeline_config["bronze_schema"]
            bronze_table = pipeline_config["bronze_table"]
            
            fully_qualified_table = f"{bronze_database}.{bronze_schema}.{bronze_table}"
            
            self.logger.info(f"Processing pipeline {pipeline_index}/{len(pipeline_data)}: {fully_qualified_table}")
            
            # SQL to create table using template + infer_schema
            sql_create = f"""
            CREATE OR REPLACE TABLE {bronze_database}.{bronze_schema}.{bronze_table}
            USING TEMPLATE (
              SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
              FROM TABLE(
                INFER_SCHEMA(
                  LOCATION => '{source_path}',
                  FILE_FORMAT => 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT_INFER',
                  FILES => '{source_table}.{file_format.lower()}'
                )
              )
            );
            """
            
            try:
                self.logger.info(f"Creating table {fully_qualified_table}")
                self.session.sql(sql_create).collect()
                self.logger.info(f"Successfully created table {fully_qualified_table}")
            except Exception as e:
                self.logger.error(f"Failed to create table {fully_qualified_table}: {e}")
                raise
            
            # SQL to copy data
            sql_copy = f"""
            COPY INTO {bronze_database}.{bronze_schema}.{bronze_table}
            FROM {source_path}
            FILES = ('{source_table}.{file_format.lower()}')
            FILE_FORMAT = (FORMAT_NAME = 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT')
            ON_ERROR = 'CONTINUE';
            """
            
            try:
                self.logger.info(f"Copying data into {fully_qualified_table}")
                copy_result = self.session.sql(sql_copy).collect()
                self.logger.info(f"Successfully copied data into {fully_qualified_table}")
                
                # Log copy statistics if available
                if copy_result:
                    self.logger.info(f"Copy result: {copy_result}")
            except Exception as e:
                self.logger.error(f"Failed to copy data into {fully_qualified_table}: {e}")
                raise
        
        self.logger.info(f"Pipeline execution completed. Processed {len(pipeline_data)} table(s).")
    
    def invoke_silver_pipeline(self, pipeline_silver_data: List[Dict[str, Any]], warehouse_name: str = "COMPUTE_WH") -> None:
        """
        Execute a silver layer pipeline that creates dynamic tables with SCD Type 1 logic.
        
        Args:
            pipeline_silver_data: List of dictionaries containing silver pipeline configuration.
                                 Each dict should have:
                                 - bronze_database_dev: Source bronze database name
                                 - bronze_schema: Source bronze schema name
                                 - bronze_table: Source bronze table name
                                 - silver_database_dev: Target silver database name
                                 - silver_schema: Target silver schema name
                                 - silver_table: Target silver table name
                                 - silver_cdc_apply_changes: Dict with CDC config containing:
                                   - keys: List of key columns for deduplication
                                   - sequence_by: Column to order by for getting latest record
                                   - scd_type: SCD type (currently only "1" is supported)
            warehouse_name: Warehouse to use for dynamic table refresh (default: "COMPUTE_WH")
            
        Returns:
            None
            
        Raises:
            ValueError: If SCD type is not "1"
            
        Example:
            >>> pipeline = SnowmetaPipeline(session)
            >>> pipeline_silver_data = [
            ...     {
            ...         "bronze_database_dev": "ANALYTICS",
            ...         "bronze_schema": "FINANCIAL_BRONZE",
            ...         "bronze_table": "Banks_2022_2023_raw",
            ...         "silver_database_dev": "ANALYTICS",
            ...         "silver_schema": "FINANCIAL_SILVER",
            ...         "silver_table": "Banks_current",
            ...         "silver_cdc_apply_changes": {
            ...             "keys": ["bank_id"],
            ...             "sequence_by": "updated_at",
            ...             "scd_type": "1"
            ...         }
            ...     }
            ... ]
            >>> pipeline.invoke_silver_pipeline(pipeline_silver_data)
        """
        
        def quote_identifier(name: str) -> str:
            """
            Quote a SQL identifier to handle special characters and reserved words.
            
            Args:
                name: The identifier name to quote
                
            Returns:
                The quoted identifier
            """
            # Simplistic quoting - wrap in double quotes
            # In production, you might want more sophisticated handling
            return f'"{name}"'
        
        for pipeline_index, silver_config in enumerate(pipeline_silver_data, 1):
            bronze_database = silver_config["bronze_database_dev"]
            bronze_schema = silver_config["bronze_schema"]
            bronze_table = silver_config["bronze_table"]
            silver_database = silver_config["silver_database_dev"]
            silver_schema = silver_config["silver_schema"]
            silver_table = silver_config["silver_table"]
            cdc_config = silver_config["silver_cdc_apply_changes"]
            
            key_columns = cdc_config["keys"]
            sequence_by_column = cdc_config["sequence_by"]
            scd_type = cdc_config["scd_type"]
            
            fully_qualified_bronze_table = f"{bronze_database}.{bronze_schema}.{bronze_table}"
            fully_qualified_silver_table = f"{silver_database}.{silver_schema}.{silver_table}"
            
            self.logger.info(f"Processing silver pipeline {pipeline_index}/{len(pipeline_silver_data)}: {fully_qualified_silver_table}")
            
            # Validate SCD type
            if scd_type != "1":
                error_msg = f"Only SCD type 1 supported; got {scd_type} for {silver_table}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            # For now, assume a single key. If multiple keys, join them with commas
            if len(key_columns) > 1:
                self.logger.warning(f"Multiple keys detected for {silver_table}: {key_columns}. Using first key only.")
            
            key_column = key_columns[0]
            
            # Quote identifiers for SQL injection protection and special character handling
            key_column_quoted = quote_identifier(key_column)
            sequence_by_column_quoted = quote_identifier(sequence_by_column)
            
            # Build the dynamic table SQL
            sql_create_dynamic_table = f"""
            CREATE OR REPLACE DYNAMIC TABLE {silver_database}.{silver_schema}.{silver_table}
              TARGET_LAG = '5 minutes'
              WAREHOUSE = {warehouse_name}
              REFRESH_MODE = FULL
              INITIALIZE = ON_CREATE
            AS
              SELECT
                bronze.*
              FROM
                {bronze_database}.{bronze_schema}.{bronze_table} AS bronze
              QUALIFY ROW_NUMBER() OVER (
                PARTITION BY bronze.{key_column_quoted}
                ORDER BY bronze.{sequence_by_column_quoted} DESC
              ) = 1
            ;
            """
            
            try:
                self.logger.info(f"Creating dynamic table {fully_qualified_silver_table} (SCD Type {scd_type} on {key_column})")
                self.session.sql(sql_create_dynamic_table).collect()
                self.logger.info(f"Successfully created dynamic table {fully_qualified_silver_table}")
            except Exception as e:
                self.logger.error(f"Failed to create dynamic table {fully_qualified_silver_table}: {e}")
                raise
        
        self.logger.info(f"Silver pipeline execution completed. Processed {len(pipeline_silver_data)} dynamic table(s).")