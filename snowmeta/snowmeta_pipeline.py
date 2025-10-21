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
from snowmeta.controltable_reader import ControlTableReader
from snowmeta.snowmeta_sql import SnowmetaSQL


class SnowmetaPipeline:
    
    def __init__(self, session: Session):
        """
        Initialize the SnowmetaPipeline.
        
        Args:
            session: Active Snowflake Snowpark session
        """
        self.session = session
        self.logger = logging.getLogger(__name__)
        self.controltable_reader = ControlTableReader(session)
    
    def create_unified_bronze_stored_procedure(self, pipeline_data: List[Dict[str, str]]) -> str:
        """
        Generate SQL for creating a unified stored procedure for all bronze ingestion tables.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration
            
        Returns:
            SQL string for creating the unified stored procedure
        """
        if not pipeline_data:
            return ""
        
        # Get common database and schema from first config
        bronze_database = pipeline_data[0]["bronze_database_dev"]
        bronze_schema = pipeline_data[0]["bronze_schema"]
        
        # Generate procedure name
        procedure_name = f"SP_INGEST_ALL_BRONZE"
        
        # Build the procedure body with all tables
        procedure_body = ""
        
        for pipeline_config in pipeline_data:
            source_table = pipeline_config["source_table"]
            source_path = pipeline_config["source_path_dev"]
            file_format = pipeline_config["reader_format"]
            variantload = pipeline_config.get("variant_load", False)
            variant_column_name = pipeline_config.get("variant_column_name", "SRC")
            bronze_table = pipeline_config["bronze_table"]
            byos_schema_location = pipeline_config.get("byos_schema")
            
            if byos_schema_location:

                schema_dict = self.controltable_reader.bringyourownschema(byos_schema_location)
                create_table_sql = self.controltable_reader.generate_create_table_from_schema(schema_dict, f"{bronze_database}.{bronze_schema}.{bronze_table}")
                # Use custom schema from JSON file
                procedure_body += f""" {create_table_sql} """

                procedure_body += f"""
                -- Copy data into table
                COPY INTO {bronze_database}.{bronze_schema}.{bronze_table}
                    FROM '{source_path}'
                    FILE_FORMAT = (FORMAT_NAME = 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT')
                    PATTERN = '.*\\.{file_format.lower()}'
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    INCLUDE_METADATA = (
                    _SRC_FILENAME=METADATA$FILENAME,
                    _SRC_FILE_ROW_NUMBER=METADATA$FILE_ROW_NUMBER
                    );
                    
                UPDATE {bronze_database}.{bronze_schema}.{bronze_table}
                SET _LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE _LOAD_TIMESTAMP IS NULL;

                """
            
            elif variantload:
                procedure_body += f"""
                    -- Processing {bronze_table}
                    -- Create variant table to ingest semi-structured data if it doesn't exist
                    CREATE TABLE IF NOT EXISTS {bronze_database}.{bronze_schema}.{bronze_table} (
                        {variant_column_name} VARIANT,
                        _SRC_FILENAME VARCHAR,
                        _SRC_FILE_ROW_NUMBER VARCHAR,
                        _LOAD_TIMESTAMP TIMESTAMP_NTZ
                    );

                     """
                procedure_body += f"""
                    -- Copy data into table
                    COPY INTO {bronze_database}.{bronze_schema}.{bronze_table} 
                        FROM (
                            SELECT
                                $1 AS {variant_column_name},
                                METADATA$FILENAME AS _SRC_FILENAME,
                                METADATA$FILE_ROW_NUMBER AS _SRC_FILE_ROW_NUMBER,
                                CURRENT_TIMESTAMP() AS _LOAD_TIMESTAMP
                            FROM '{source_path}'
                            )
                        FILE_FORMAT = (FORMAT_NAME = 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT')
                        PATTERN = '.*\\.{file_format.lower()}';
                    """

            else:
                procedure_body += f"""
                    -- Processing {bronze_table}
                    -- Create table from inferred schema if it doesn't exist
                    CREATE TABLE IF NOT EXISTS {bronze_database}.{bronze_schema}.{bronze_table}
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '{source_path}',
                            FILE_FORMAT => 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT',
                            IGNORE_CASE => TRUE
                            
                        )
                        )
                    );

                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} ADD COLUMN IF NOT EXISTS _SRC_FILENAME VARCHAR;
                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} ADD COLUMN IF NOT EXISTS _SRC_FILE_ROW_NUMBER NUMBER;
                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} ADD COLUMN IF NOT EXISTS _LOAD_TIMESTAMP TIMESTAMP_NTZ;
                    """
                procedure_body += f"""
                -- Copy data into table
                COPY INTO {bronze_database}.{bronze_schema}.{bronze_table}
                    FROM '{source_path}'
                    FILE_FORMAT = (FORMAT_NAME = 'RAW.SNOWMETA_CONFIG.{file_format}_FILE_FORMAT')
                    PATTERN = '.*\\.{file_format.lower()}'
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    INCLUDE_METADATA = (
                    _SRC_FILENAME=METADATA$FILENAME,
                    _SRC_FILE_ROW_NUMBER=METADATA$FILE_ROW_NUMBER
                    );
                    
                UPDATE {bronze_database}.{bronze_schema}.{bronze_table}
                SET _LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE _LOAD_TIMESTAMP IS NULL;

                """
        
        sql_procedure = f"""
            CREATE OR REPLACE PROCEDURE {bronze_database}.{bronze_schema}.{procedure_name}()
            RETURNS STRING
            LANGUAGE SQL
            EXECUTE AS OWNER
            AS
            $$
            BEGIN
            {procedure_body}
            RETURN 'SUCCESS';
            END;
            $$;
            """
        return sql_procedure
    
    def create_unified_bronze_task(self, pipeline_data: List[Dict[str, str]], warehouse_name: str = "COMPUTE_WH") -> str:
        """
        Generate SQL for creating a unified task for all bronze ingestion tables.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration
            warehouse_name: Warehouse to use for the task
            
        Returns:
            SQL string for creating the unified task
        """
        if not pipeline_data:
            return ""
        
        # Get common database and schema from first config
        bronze_database = pipeline_data[0]["bronze_database_dev"]
        bronze_schema = pipeline_data[0]["bronze_schema"]
        
        # Generate procedure and task names
        procedure_name = f"SP_INGEST_ALL_BRONZE"
        task_name = f"INGEST_ALL_BRONZE"
        
        sql_task = f"""
                    CREATE OR REPLACE TASK {bronze_database}.{bronze_schema}.{task_name}
                    WAREHOUSE = {warehouse_name}
                    AS
                    CALL {bronze_database}.{bronze_schema}.{procedure_name}();
                    """
        return sql_task
    
    def generate_bronze_sql_scripts(self, pipeline_data: List[Dict[str, str]], warehouse_name: str = "COMPUTE_WH") -> Dict[str, str]:
        """
        Generate standalone SQL scripts for unified stored procedure and task.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration
            warehouse_name: Warehouse to use for tasks
            
        Returns:
            Dictionary with 'procedure', 'task', 'procedure_name', and 'task_name' keys
        """
        if not pipeline_data:
            return {'procedure': '', 'task': '', 'procedure_name': '', 'task_name': ''}
        
        # Generate unified stored procedure SQL
        procedure_sql = self.create_unified_bronze_stored_procedure(pipeline_data)
        
        # Generate unified task SQL
        task_sql = self.create_unified_bronze_task(pipeline_data, warehouse_name)
        
        # Get names
        bronze_database = pipeline_data[0]["bronze_database_dev"]
        bronze_schema = pipeline_data[0]["bronze_schema"]
        procedure_name = f"SP_INGEST_ALL_BRONZE"
        task_name = f"INGEST_ALL_BRONZE"
        
        return {
            'procedure': procedure_sql,
            'task': task_sql,
            'procedure_name': f"{bronze_database}.{bronze_schema}.{procedure_name}",
            'task_name': f"{bronze_database}.{bronze_schema}.{task_name}"
        }
    
    def generate_execute_task_sql(self, pipeline_data: List[Dict[str, str]]) -> str:
        """
        Generate EXECUTE TASK SQL statement for manual execution.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration
            
        Returns:
            EXECUTE TASK SQL statement
        """
        if not pipeline_data:
            return ""
        
        bronze_database = pipeline_data[0]["bronze_database_dev"]
        bronze_schema = pipeline_data[0]["bronze_schema"]
        task_name = f"INGEST_ALL_BRONZE"
        
        return f"EXECUTE TASK {bronze_database}.{bronze_schema}.{task_name};"

    def invoke_bronze_pipeline(self, pipeline_data: List[Dict[str, str]], warehouse_name: str = "COMPUTE_WH", use_stored_procedures: bool = True) -> None:
        """
        Execute a data pipeline that creates tables and loads data in Snowflake.
        Can use either direct SQL execution or stored procedures with tasks.
        
        Args:
            pipeline_data: List of dictionaries containing pipeline configuration.
                          Each dict should have:
                          - source_table: Name of the source table/file
                          - source_path_dev: Stage path for source files
                          - reader_format: File format (e.g., 'CSV', 'JSON', 'PARQUET')
                          - bronze_database_dev: Target database name
                          - bronze_schema: Target schema name
                          - bronze_table: Target table name
            warehouse_name: Warehouse to use for tasks (default: "COMPUTE_WH")
            use_stored_procedures: Whether to use stored procedures and tasks (default: True)
            
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
            >>> pipeline.invoke_bronze_pipeline(pipeline_data)
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
            
            if use_stored_procedures:
                # Create unified stored procedure for all tables
                sql_procedure = self.create_unified_bronze_stored_procedure(pipeline_data)
                
                try:
                    self.logger.info(f"Creating unified stored procedure for all bronze tables")
                    self.session.sql(sql_procedure).collect()
                    self.logger.info(f"Successfully created unified stored procedure")
                except Exception as e:
                    self.logger.error(f"Failed to create unified stored procedure: {e}")
                    raise
                
                # Create unified task
                sql_task = self.create_unified_bronze_task(pipeline_data, warehouse_name)
                
                try:
                    self.logger.info(f"Creating unified task for all bronze tables")
                    self.session.sql(sql_task).collect()
                    self.logger.info(f"Successfully created unified task")
                except Exception as e:
                    self.logger.error(f"Failed to create unified task: {e}")
                    raise
                
                # Execute the unified task
                bronze_database = pipeline_data[0]["bronze_database_dev"]
                bronze_schema = pipeline_data[0]["bronze_schema"]
                task_name = f"INGEST_ALL_BRONZE"
                try:
                    self.logger.info(f"Executing unified task {bronze_database}.{bronze_schema}.{task_name}")
                    result = self.session.sql(f"EXECUTE TASK {bronze_database}.{bronze_schema}.{task_name}").collect()
                    self.logger.info(f"Successfully executed unified task. Result: {result}")
                    return result
                except Exception as e:
                    self.logger.error(f"Failed to execute unified task: {e}")
                    
                    
                
                # Break after creating unified procedure and task (only once)
                break
           
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
    
    def create_scd1_stored_procedure(self, silver_config: Dict[str, Any], flattened_view_name: Optional[str] = None) -> str:    
        """
        Generate SQL for creating a stored procedure for SCD Type 1 silver table.
        """
        bronze_database = silver_config["bronze_database_dev"]
        bronze_schema = silver_config["bronze_schema"]
        bronze_table = silver_config["bronze_table"]

        if flattened_view_name:
            bronze_database = silver_config["silver_database_dev"]
            bronze_schema = silver_config["silver_schema"]
            bronze_table = flattened_view_name

        
        silver_database = silver_config["silver_database_dev"]
        silver_schema = silver_config["silver_schema"]
        silver_table = silver_config["silver_table"]
        cdc_config = silver_config["silver_cdc_apply_changes"]
        
        key_columns = cdc_config["keys"]
        sequence_by_column = cdc_config["sequence_by"]

        # Quote identifiers
        key_column = key_columns[0]
        key_column_quoted = f'"{key_column.upper()}"'
        sequence_by_quoted = f'"{sequence_by_column.upper()}"'

        # Build procedure name
        procedure_name = f"SP_UPSERT_SCD1_{silver_table.upper()}"

        # Compose the SQL for the procedure
        sql_procedure = f"""
                        CREATE OR REPLACE PROCEDURE {silver_database}.{silver_schema}.{procedure_name}()
                        RETURNS VARCHAR
                        LANGUAGE SQL
                        EXECUTE AS OWNER
                        AS
                        $$
                        BEGIN
                            -- Create silver table if it doesn't exist (using bronze table structure)
                            CREATE TABLE IF NOT EXISTS {silver_database}.{silver_schema}.{silver_table.upper()} 
                            AS SELECT * FROM {bronze_database}.{bronze_schema}.{bronze_table.upper()} WHERE 1=0;
                            
                            -- Simple SCD1 merge with UPDATE ALL BY NAME and INSERT ALL BY NAME
                            MERGE INTO {silver_database}.{silver_schema}.{silver_table.upper()} t
                            USING (
                                SELECT * FROM {bronze_database}.{bronze_schema}.{bronze_table.upper()} b
                                QUALIFY ROW_NUMBER() OVER (PARTITION BY b.{key_column_quoted} ORDER BY b.{sequence_by_quoted} DESC) = 1
                            ) s
                            ON t.{key_column_quoted} = s.{key_column_quoted}
                            WHEN MATCHED THEN
                                UPDATE ALL BY NAME
                            WHEN NOT MATCHED THEN
                                INSERT ALL BY NAME;
                            RETURN 'SCD1 merge completed on {silver_table.upper()}';
                        END;
                        $$;
                        """
        return sql_procedure
               
    
    def create_scd2_stored_procedure(self, silver_config: Dict[str, Any], flattened_view_name: Optional[str] = None) -> str:    
        """
        Generate SQL for creating a stored procedure for SCD Type 2 silver table.
        """
        bronze_database = silver_config["bronze_database_dev"]
        bronze_schema = silver_config["bronze_schema"]
        bronze_table = silver_config["bronze_table"]

        if flattened_view_name:
            bronze_database = silver_config["silver_database_dev"]
            bronze_schema = silver_config["silver_schema"]
            bronze_table = flattened_view_name


        silver_database = silver_config["silver_database_dev"]
        silver_schema = silver_config["silver_schema"]
        silver_table = silver_config["silver_table"]
        cdc_config = silver_config["silver_cdc_apply_changes"]
        
        key_columns = cdc_config["keys"]
        sequence_by_column = cdc_config["sequence_by"]
        except_columns = cdc_config.get("except_column_list", [])
        
        # Quote identifiers
        key_column = key_columns[0]
        
        # FIX: Ensure quoted identifiers are UPPERCASE to match Snowflake's default table storage
        key_column_quoted = f'"{key_column.upper()}"'
        sequence_by_quoted = f'"{sequence_by_column.upper()}"'
        
        # Build procedure name
        procedure_name = f"SP_UPSERT_SCD2_{silver_table.upper()}"
        
        # Prepare list of excluded columns for dynamic SQL generation
        excluded_cols_list = except_columns + ['VALID_FROM', 'VALID_TO', 'IS_CURRENT', 'RN']
        excluded_cols_sql = ', '.join([f"'{col.upper()}'" for col in excluded_cols_list])

        sql_procedure = f"""
                            CREATE OR REPLACE PROCEDURE {silver_database}.{silver_schema}.{procedure_name}()
                            RETURNS VARCHAR
                            LANGUAGE SQL
                            EXECUTE AS OWNER
                            AS
                            $$
                            DECLARE
                                columns_list VARCHAR;
                                select_columns_list VARCHAR;
                                update_conditions VARCHAR;
                            BEGIN
                                -- Create temporary deduped source table
                                CREATE OR REPLACE VIEW {bronze_database}.{bronze_schema}.deduped_{bronze_table} AS
                                SELECT *
                                FROM (
                                    SELECT *,
                                            ROW_NUMBER() OVER (PARTITION BY {key_column_quoted} ORDER BY {sequence_by_quoted} DESC) AS rn
                                    FROM {bronze_database}.{bronze_schema}.{bronze_table}
                                )
                                WHERE rn = 1;

                                -- Create silver table if it doesn't exist (schema only, no data)
                                CREATE TABLE IF NOT EXISTS {silver_database}.{silver_schema}.{silver_table}
                                AS
                                SELECT
                                    *,
                                    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS VALID_FROM,
                                    CAST(NULL AS TIMESTAMP_NTZ) AS VALID_TO,
                                    TRUE AS IS_CURRENT
                                FROM {bronze_database}.{bronze_schema}.deduped_{bronze_table}
                                WHERE 1=0;

                                -- Get column list dynamically for the INSERT INTO target list (unprefixed)
                                SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                                INTO :columns_list
                                FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                                AND TABLE_NAME = '{silver_table.upper()}'
                                AND COLUMN_NAME NOT IN ({excluded_cols_sql})
                                ;

                                -- Get column list dynamically for the SELECT statement (prefixed with 'source.')
                                SELECT LISTAGG('source."' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                                INTO :select_columns_list
                                FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                                AND TABLE_NAME = '{silver_table.upper()}'
                                AND COLUMN_NAME NOT IN ({excluded_cols_sql})
                                ;

                                -- Build update conditions for detecting changes (Exclude Key, SCD2 fields, and excluded cols)
                                SELECT LISTAGG('target."' || COLUMN_NAME || '" IS DISTINCT FROM source."' || COLUMN_NAME || '"', ' OR ')
                                INTO :update_conditions
                                FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                                AND TABLE_NAME = '{silver_table.upper()}'
                                AND COLUMN_NAME NOT IN ('{key_column.upper()}', {excluded_cols_sql})
                                ;

                                -- Expire existing records where changes are detected
                                EXECUTE IMMEDIATE '
                                UPDATE {silver_database}.{silver_schema}.{silver_table} AS target
                                SET VALID_TO = CURRENT_TIMESTAMP(),
                                    IS_CURRENT = FALSE
                                FROM {bronze_database}.{bronze_schema}.deduped_{bronze_table} AS source
                                WHERE target.{key_column_quoted} = source.{key_column_quoted}
                                AND target.IS_CURRENT = TRUE
                                AND (' || :update_conditions || ')
                                ';

                                -- Insert new and changed records
                                EXECUTE IMMEDIATE '
                                INSERT INTO {silver_database}.{silver_schema}.{silver_table} (' || :columns_list || ', VALID_FROM, VALID_TO, IS_CURRENT)
                                SELECT ' || :select_columns_list || ', CURRENT_TIMESTAMP(), NULL, TRUE
                                FROM {bronze_database}.{bronze_schema}.deduped_{bronze_table} AS source
                                LEFT JOIN {silver_database}.{silver_schema}.{silver_table} AS target
                                    ON source.{key_column_quoted} = target.{key_column_quoted} AND target.IS_CURRENT = TRUE
                                WHERE
                                    target.{key_column_quoted} IS NULL
                                    OR (' || :update_conditions || ')
                                ';

                                RETURN 'SCD2 upsert complete for {silver_table.upper()}';
                            END;
                            $$;
                            """
        return sql_procedure
    
    def create_master_silver_task(self, pipeline_silver_data: Dict[str, Any], 
                         warehouse_name: str = "COMPUTE_WH", after_task: Optional[str] = None) -> str:
        """
        Generate SQL for creating a task for SCD Type 2 stored procedure.
        
        Args:
            silver_config: Dictionary containing silver pipeline configuration
            bronze_database: Bronze database name for task creation
            bronze_schema: Bronze schema name for task creation
            warehouse_name: Warehouse to use for the task
            after_task: Optional predecessor task name
            
        Returns:
            SQL string for creating the task
        """
        silver_database_prime = pipeline_silver_data[0]["silver_database_dev"]
        silver_schema_prime = pipeline_silver_data[0]["silver_schema"]
        silver_table_prime = pipeline_silver_data[0]["silver_table"]
        bronze_database_prime = pipeline_silver_data[0]["bronze_database_dev"]
        bronze_schema_prime = pipeline_silver_data[0]["bronze_schema"]
        
        master_silver_procedure_name = f"SP_SNOWMETA_SILVER_MASTER_{silver_schema_prime.upper()}"
        task_name = f"TASK_SILVER_SCD_{silver_schema_prime.upper()}"
        
        after_clause = f"AFTER {after_task}" if after_task else ""
        
        master_sql_task = f"""
                            CREATE OR REPLACE TASK {bronze_database_prime}.{bronze_schema_prime}.{task_name}
                            WAREHOUSE = {warehouse_name}
                            {after_clause}
                            AS
                            CALL {silver_database_prime}.{silver_schema_prime}.{master_silver_procedure_name}();
                            """
        return master_sql_task
    
    
    def invoke_silver_scd_pipeline(self, pipeline_silver_data: List[Dict[str, Any]], 
                                     pipeline_bronze_data: List[Dict[str, str]],
                                     warehouse_name: str = "COMPUTE_WH",
                                     bronze_task_name: Optional[str] = None,
                                     execute_tasks: bool = True) -> Any:
        """
        Execute a silver layer pipeline that creates stored procedures and tasks for SCD Type 2 logic.
        
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
                                   - keys: List of key columns
                                   - sequence_by: Column to order by for getting latest record
                                   - scd_type: Must be "2" for this method
                                   - except_column_list: Optional list of columns to exclude
            pipeline_bronze_data: List of dictionaries containing bronze pipeline configuration.
                                 Used to get the bronze database and schema for task creation.
            warehouse_name: Warehouse to use for tasks (default: "COMPUTE_WH")
            bronze_task_name: Optional bronze task name to chain after
            execute_tasks: Whether to execute tasks immediately (default: True)
            
        Returns:
            None
            
        
            
        Example:
            >>> pipeline = SnowmetaPipeline(session)
            >>> pipeline_silver_data = [
            ...     {
            ...         "bronze_database_dev": "ANALYTICS",
            ...         "bronze_schema": "FINANCIAL_BRONZE",
            ...         "bronze_table": "Banks_2022_2023",
            ...         "silver_database_dev": "ANALYTICS",
            ...         "silver_schema": "FINANCIAL_SILVER",
            ...         "silver_table": "Banks_2022_2023",
            ...         "silver_cdc_apply_changes": {
            ...             "keys": ["customer_id"],
            ...             "sequence_by": "dmsTimestamp",
            ...             "scd_type": "2",
            ...             "except_column_list": ["Op", "dmsTimestamp", "_rescued_data"]
            ...         }
            ...     }
            ... ]
            >>> pipeline.invoke_silver_scd2_pipeline(pipeline_silver_data)
        """
        
        # Get bronze database and schema from pipeline_bronze_data
        bronze_database_prime = pipeline_silver_data[0]["bronze_database_dev"]
        bronze_schema_prime = pipeline_silver_data[0]["bronze_schema"]
        silver_database_prime = pipeline_silver_data[0]["silver_database_dev"]
        silver_schema_prime = pipeline_silver_data[0]["silver_schema"]
        silver_table_prime = pipeline_silver_data[0]["silver_table"]

        master_silver_procedure_name = f"SP_SNOWMETA_SILVER_MASTER_{silver_schema_prime.upper()}"
        master_procedure_body = f""
           
        for pipeline_index, silver_config in enumerate(pipeline_silver_data, 1):
            cdc_config = silver_config["silver_cdc_apply_changes"]
            scd_type = cdc_config["scd_type"]
            silver_database = silver_config["silver_database_dev"]
            silver_schema = silver_config["silver_schema"]
            silver_table = silver_config["silver_table"]
            bronze_database = silver_config["bronze_database_dev"]
            bronze_schema = silver_config["bronze_schema"]
            bronze_table = silver_config["bronze_table"]

            silver_transformations = silver_config.get("silver_transformation_json")
            if silver_transformations:
                select_expressions = silver_transformations.get("select_exp")
                columns_to_flatten = silver_transformations.get("columns_to_flatten")


                if columns_to_flatten:
                    sql_gen = SnowmetaSQL()
                    flattening_sql_gen = sql_gen.flatten_json(
                        bronze_database=bronze_database,
                        bronze_schema=bronze_schema,
                        bronze_table=bronze_table,
                        silver_database=silver_database,
                        silver_schema=silver_schema,
                        columns_to_flatten=columns_to_flatten
                    )
                    flattening_procedure_sql = flattening_sql_gen['sql']
                    flattening_procedure_name = flattening_sql_gen['procedure_name']
                    flattened_view_name = flattening_sql_gen['view_name']
                    self.session.sql(flattening_procedure_sql).collect()
                    self.logger.info(f"Successfully created flattening procedure: {flattening_procedure_name}")
                    master_procedure_body += f"""
                    
                    CALL {silver_database}.{silver_schema}.{flattening_procedure_name}();
                    
                    """
                    self.logger.info(f"Successfully created flattening view: {flattened_view_name}")
                    
                    transformed_view_name = flattened_view_name
                    
                if select_expressions:
                    sql_gen = SnowmetaSQL()
                    if columns_to_flatten:
                        select_expression_sql_gen = sql_gen.select_expression(
                            bronze_database=silver_database,
                            bronze_schema=silver_schema,
                            bronze_table=flattened_view_name,
                            silver_database=silver_database,
                            silver_schema=silver_schema,
                            select_expression=select_expressions
                        )

                    else:
                        select_expression_sql_gen = sql_gen.select_expression(
                            bronze_database=bronze_database,
                            bronze_schema=bronze_schema,
                            bronze_table=bronze_table,
                            silver_database=silver_database,
                            silver_schema=silver_schema,
                            select_expression=select_expressions
                        )
                    select_expression_procedure_sql = select_expression_sql_gen['sql']
                    select_expression_procedure_name = select_expression_sql_gen['procedure_name']
                    select_expression_view_name = select_expression_sql_gen['view_name']
                    self.session.sql(select_expression_procedure_sql).collect()
                    self.logger.info(f"Successfully created select expression procedure: {select_expression_procedure_name}")
                    self.logger.info(f"Successfully created select expression view: {select_expression_view_name}")
                    master_procedure_body += f"""
                    
                    CALL {silver_database}.{silver_schema}.{select_expression_procedure_name}();
                    
                    """

                    transformed_view_name = select_expression_view_name
                
                if scd_type == "2":
                    scd2_procedure_sql = self.create_scd2_stored_procedure(silver_config, transformed_view_name)
                if scd_type == "1":
                    scd1_procedure_sql = self.create_scd1_stored_procedure(silver_config, transformed_view_name)

                master_procedure_body += f"""
                    
                    CALL {silver_database}.{silver_schema}.SP_UPSERT_SCD{scd_type}_{silver_table.upper()}();
                    
                    """
            else:
                if scd_type == "2":
                    scd2_procedure_sql = self.create_scd2_stored_procedure(silver_config)
                if scd_type == "1":
                    scd1_procedure_sql = self.create_scd1_stored_procedure(silver_config)

                master_procedure_body += f"""
                
                CALL {silver_database}.{silver_schema}.SP_UPSERT_SCD{scd_type}_{silver_table.upper()}();
                
                """

            fully_qualified_silver_table = f"{silver_database}.{silver_schema}.{silver_table}"
            
            self.logger.info(f"Processing SCD2 silver pipeline {pipeline_index}/{len(pipeline_silver_data)}: {fully_qualified_silver_table}")                      
            # Create stored procedure           
            try:
                self.logger.info(f"Creating SCD2 stored procedure for {fully_qualified_silver_table}")
                if scd_type == "2":
                    self.session.sql(scd2_procedure_sql).collect()
                if scd_type == "1":
                    self.session.sql(scd1_procedure_sql).collect()
                self.logger.info(f"Successfully created SCD{scd_type} stored procedure")
            except Exception as e:
                self.logger.error(f"Failed to create SCD{scd_type} stored procedure: {e}")
                raise

        master_silver_procedure = f"""
            CREATE OR REPLACE PROCEDURE {silver_database_prime}.{silver_schema_prime}.{master_silver_procedure_name}()
            RETURNS STRING
            LANGUAGE SQL
            EXECUTE AS OWNER
            AS
            $$
            BEGIN
            {master_procedure_body}
            RETURN 'SUCCESS';
            END;
            $$;
            """

        self.session.sql(master_silver_procedure).collect()
        self.logger.info(f"Successfully created master silver procedure")
        # Create task
        task_sql = self.create_master_silver_task(pipeline_silver_data, warehouse_name, after_task=None)
        
        try:
            self.logger.info(f"Creating SCD2 task for Tables in  {silver_database_prime}.{silver_schema_prime}")
            self.session.sql(task_sql).collect()
            task_name = f"{bronze_database_prime}.{bronze_schema_prime}.TASK_SILVER_SCD_{silver_schema_prime.upper()}"
            self.logger.info(f"Successfully created SCD2 task: {task_name}")
        except Exception as e:
            self.logger.error(f"Failed to create SCD2 task: {e}")
            raise
        
        # Execute task if requested
        if execute_tasks:
            task_name = f"{bronze_database_prime}.{bronze_schema_prime}.TASK_SILVER_SCD_{silver_schema_prime.upper()}"
            try:
                self.logger.info(f"Executing SCD2 task {task_name}")
                result = self.session.sql(f"EXECUTE TASK {task_name};").collect()
                self.logger.info(f"Successfully executed task. Result: {result}")
                return result
            except Exception as e:
                self.logger.error(f"Failed to execute task {task_name}: {e}")
                raise    
        
        self.logger.info(f"SCD2 silver pipeline execution completed. Processed {len(pipeline_silver_data)} table(s).")