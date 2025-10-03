
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
    
    def invoke_pipeline(self, pipeline_data: List[Dict[str, str]]) -> None:
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