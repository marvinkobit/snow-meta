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
                    _SRC_FILE_ROW_NUMBER=METADATA$FILE_ROW_NUMBER,
                    _RECEIVED_TIMESTAMP=METADATA$FILE_LAST_MODIFIED
                    );
                    
                UPDATE {bronze_database}.{bronze_schema}.{bronze_table}
                SET _INGEST_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE _INGEST_TIMESTAMP IS NULL;

                """
            
            elif variantload:
                procedure_body += f"""
                    -- Processing {bronze_table}
                    -- Create variant table to ingest semi-structured data if it doesn't exist
                    CREATE TABLE IF NOT EXISTS {bronze_database}.{bronze_schema}.{bronze_table} (
                        {variant_column_name} VARIANT,
                        _SRC_FILENAME VARCHAR,
                        _SRC_FILE_ROW_NUMBER VARCHAR,
                        _RECEIVED_TIMESTAMP TIMESTAMP_NTZ,
                        _INGEST_TIMESTAMP TIMESTAMP_NTZ
                       
                    );

                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} SET ENABLE_SCHEMA_EVOLUTION = TRUE;

                    CREATE STREAM IF NOT EXISTS {bronze_database}.{bronze_schema}.STREAM_{bronze_table} ON TABLE {bronze_database}.{bronze_schema}.{bronze_table};

                     """
                procedure_body += f"""
                    -- Copy data into table
                    COPY INTO {bronze_database}.{bronze_schema}.{bronze_table} 
                        FROM (
                            SELECT
                                $1 AS {variant_column_name},
                                METADATA$FILENAME AS _SRC_FILENAME,
                                METADATA$FILE_ROW_NUMBER AS _SRC_FILE_ROW_NUMBER,
                                METADATA$FILE_LAST_MODIFIED AS _RECEIVED_TIMESTAMP,
                                CURRENT_TIMESTAMP() AS _INGEST_TIMESTAMP
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
                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} ADD COLUMN IF NOT EXISTS _INGEST_TIMESTAMP TIMESTAMP_NTZ;
                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} ADD COLUMN IF NOT EXISTS _RECEIVED_TIMESTAMP TIMESTAMP_NTZ;

                    ALTER TABLE {bronze_database}.{bronze_schema}.{bronze_table} SET ENABLE_SCHEMA_EVOLUTION = TRUE;

                    CREATE STREAM IF NOT EXISTS {bronze_database}.{bronze_schema}.STREAM_{bronze_table} ON TABLE {bronze_database}.{bronze_schema}.{bronze_table};

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
                    _SRC_FILE_ROW_NUMBER=METADATA$FILE_ROW_NUMBER,
                    _RECEIVED_TIMESTAMP=METADATA$FILE_LAST_MODIFIED
                    );
                    
                UPDATE {bronze_database}.{bronze_schema}.{bronze_table}
                SET _INGEST_TIMESTAMP = CURRENT_TIMESTAMP()
                WHERE _INGEST_TIMESTAMP IS NULL;

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
    
   
    
    def create_scd1_stored_procedure(self, silver_config: Dict[str, Any], flattened_view_name: Optional[str] = None, stream_name=None) -> str:    
        """
        Generate SQL for creating a stored procedure for SCD Type 1 silver table.
        """
        bronze_database = silver_config["bronze_database_dev"]
        bronze_schema = silver_config["bronze_schema"]
        bronze_table = silver_config["bronze_table"]
        if stream_name:
            bronze_table=stream_name

        columns_to_track = silver_config.get("columns_to_track", [])
        columns_to_exclude = silver_config.get("columns_to_exclude", [])

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

        # Determine columns to track for SCD1 updates
        columns_to_track: List[str] = cdc_config.get("columns_to_track", [])

        # Build dynamic change detection predicate and update set list using tracked columns
        change_predicate = " OR ".join([
            f't."{col.upper()}" <> s."{col.upper()}"' for col in columns_to_track
        ]) if columns_to_track else "FALSE"

        update_set_list = ",\n    ".join([
            f't."{col.upper()}" = s."{col.upper()}"' for col in columns_to_track
        ]) if columns_to_track else ""

        # Compose the SQL for the procedure using dedupe, targeted SCD1 update, dynamic insert-by-list, and soft delete
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
                        BEGIN
                         
                          CREATE OR REPLACE TEMP TABLE deduped_source AS
                            SELECT *
                            FROM (
                                SELECT *,
                                        ROW_NUMBER() OVER (PARTITION BY {key_column_quoted} ORDER BY {sequence_by_quoted} DESC) AS rn
                                FROM {bronze_database}.{bronze_schema}.{bronze_table}
                            )
                            WHERE rn = 1;

                           CREATE TABLE IF NOT EXISTS {silver_database}.{silver_schema}.{silver_table}
                                AS
                                SELECT
                                    *,
                                    CAST(NULL AS STRING) AS OPERATION
                                FROM deduped_source
                                WHERE 1=0;

                          -- Build dynamic column lists for INSERT (exclude OPERATION)
                          SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                          INTO :columns_list
                          FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                          WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                            AND TABLE_NAME = '{silver_table.upper()}'
                            AND COLUMN_NAME <> 'OPERATION';

                          SELECT LISTAGG('s."' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                          INTO :select_columns_list
                          FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                          WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                            AND TABLE_NAME = '{silver_table.upper()}'
                            AND COLUMN_NAME <> 'OPERATION';

                          -- Dynamic MERGE with explicit INSERT column list like the attached SQL
                          EXECUTE IMMEDIATE '
                            MERGE INTO {silver_database}.{silver_schema}.{silver_table.upper()} AS t
                            USING deduped_source AS s
                            ON t.{key_column_quoted} = s.{key_column_quoted}
                            
                            WHEN MATCHED AND (
                                {change_predicate}
                            )
                            THEN UPDATE SET
                                {update_set_list}{"," if update_set_list else ""}
                                t."_INGEST_TIMESTAMP" = s."_INGEST_TIMESTAMP",
                                t."_SRC_FILENAME" = s."_SRC_FILENAME",
                                t."OPERATION" = ''UPDATED''
                            
                            WHEN NOT MATCHED THEN INSERT
                                (' || :columns_list || ', "OPERATION")
                            VALUES
                                (' || :select_columns_list || ', ''INSERTED'')
                          ';

                          UPDATE {silver_database}.{silver_schema}.{silver_table.upper()} t
                          SET t."OPERATION" = 'SOFT_DELETED',
                              t."_INGEST_TIMESTAMP" = CURRENT_TIMESTAMP()
                          WHERE t.{key_column_quoted} NOT IN (
                              SELECT {key_column_quoted} FROM deduped_source
                          )
                          AND t."OPERATION" <> 'SOFT_DELETED';
                         
                          RETURN 'SCD1 merge with soft delete completed successfully.';
                         
                        END;
                        $$;
                        """
        return sql_procedure
               
    
    def create_scd2_stored_procedure(self, silver_config: Dict[str, Any], flattened_view_name: Optional[str] = None, stream_name=None) -> str:    
        """
        Generate SQL for creating a stored procedure for SCD Type 2 silver table.
        """
        bronze_database = silver_config["bronze_database_dev"]
        bronze_schema = silver_config["bronze_schema"]
        bronze_table = silver_config["bronze_table"]
        if stream_name:
            bronze_table=stream_name

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
        track_history_column_list = cdc_config.get("columns_to_track", [])
        track_history_except_column_list = cdc_config.get("except_column_list", [])
        except_columns = cdc_config.get("except_column_list", [])
        
        # Quote identifiers
        key_column = key_columns[0]
        
        # FIX: Ensure quoted identifiers are UPPERCASE to match Snowflake's default table storage
        key_column_quoted = f'"{key_column.upper()}"'
        sequence_by_quoted = f'"{sequence_by_column.upper()}"'
        
        # Build procedure name
        procedure_name = f"SP_UPSERT_SCD2_{silver_table.upper()}"
        
        # Prepare list of excluded columns for dynamic SQL generation
        excluded_cols_list = except_columns + ['VALID_FROM', 'VALID_TO', 'IS_CURRENT', 'OPERATION', 'RN']
        excluded_cols_sql = ', '.join([f"'{col.upper()}'" for col in excluded_cols_list])

        # Precompute tracked change expression text if provided
        tracked_change_expr = ' OR '.join([f'target."{c.upper()}" IS DISTINCT FROM source."{c.upper()}"' for c in track_history_column_list]) if track_history_column_list else ''

        # Build the SQL snippet to compute :update_conditions (precompute to avoid complex f-string expressions)
        if tracked_change_expr:
            escaped = tracked_change_expr.replace("'", "''")
            change_conditions_expr_sql = f"SET update_conditions := '{escaped}';"
        else:
            change_conditions_expr_sql = (
                "SELECT LISTAGG('target.\"' || COLUMN_NAME || '\" IS DISTINCT FROM source.\"' || COLUMN_NAME || '\"', ' OR ') "
                "INTO :update_conditions "
                f"FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{silver_schema.upper()}' "
                f"AND TABLE_NAME = '{silver_table.upper()}' "
                f"AND COLUMN_NAME NOT IN ('{key_column.upper()}', {excluded_cols_sql});"
            )

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
                                -- Step 1: Create deduplicated source from STREAM/VIEW SOURCE with latest by key
                                CREATE OR REPLACE TEMP TABLE deduped_source AS
                                SELECT *
                                FROM (
                                    SELECT *,
                                           ROW_NUMBER() OVER (PARTITION BY {key_column_quoted} ORDER BY {sequence_by_quoted} DESC) AS rn
                                    FROM {bronze_database}.{bronze_schema}.{bronze_table}
                                )
                                WHERE rn = 1;

                                -- Ensure silver table exists with SCD2 columns
                                CREATE TABLE IF NOT EXISTS {silver_database}.{silver_schema}.{silver_table}
                                AS
                                SELECT
                                    *,
                                    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS VALID_FROM,
                                    CAST(NULL AS TIMESTAMP_NTZ) AS VALID_TO,
                                    TRUE AS IS_CURRENT,
                                    CAST(NULL AS STRING) AS OPERATION
                                FROM deduped_source
                                WHERE 1=0;

                                -- Build dynamic column lists for inserts (exclude SCD2/except columns)
                                SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                                INTO :columns_list
                                FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                                  AND TABLE_NAME = '{silver_table.upper()}'
                                  AND COLUMN_NAME NOT IN ({excluded_cols_sql});

                                SELECT LISTAGG('source."' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                                INTO :select_columns_list
                                FROM {silver_database}.INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{silver_schema.upper()}'
                                  AND TABLE_NAME = '{silver_table.upper()}'
                                  AND COLUMN_NAME NOT IN ({excluded_cols_sql});

                                -- Compute change detection predicate
                                {change_conditions_expr_sql}

                                -- Step 2: MERGE - expire changed current rows and insert brand new keys (dynamic)
                                EXECUTE IMMEDIATE '
                                  MERGE INTO {silver_database}.{silver_schema}.{silver_table} AS target
                                  USING deduped_source AS source
                                  ON target.{key_column_quoted} = source.{key_column_quoted} AND target.IS_CURRENT = TRUE
                                  WHEN MATCHED AND (' || :update_conditions || ') THEN
                                    UPDATE SET
                                      VALID_TO = CURRENT_TIMESTAMP(),
                                      IS_CURRENT = FALSE,
                                      OPERATION = ''Expired''
                                  WHEN NOT MATCHED THEN
                                    INSERT (' || :columns_list || ', VALID_FROM, VALID_TO, IS_CURRENT, OPERATION)
                                    VALUES (' || :select_columns_list || ', CURRENT_TIMESTAMP(), NULL, TRUE, ''Newly_Inserted'')
                                ';

                                -- Step 3: Insert new versions for changed records (dynamic)
                                EXECUTE IMMEDIATE '
                                  INSERT INTO {silver_database}.{silver_schema}.{silver_table} (' || :columns_list || ', VALID_FROM, VALID_TO, IS_CURRENT, OPERATION)
                                  SELECT ' || :select_columns_list || ', CURRENT_TIMESTAMP(), NULL, TRUE, ''Updated''
                                  FROM deduped_source AS source
                                  LEFT JOIN {silver_database}.{silver_schema}.{silver_table} AS target
                                    ON source.{key_column_quoted} = target.{key_column_quoted} AND target.IS_CURRENT = TRUE
                                  WHERE ' || :update_conditions || '
                                ';

                                -- Step 4: Soft delete rows not present in source
                                UPDATE {silver_database}.{silver_schema}.{silver_table} AS target
                                SET
                                    VALID_TO = CURRENT_TIMESTAMP(),
                                    IS_CURRENT = FALSE,
                                    OPERATION = 'Soft_Delete'
                                WHERE target.IS_CURRENT = TRUE
                                  AND target.{key_column_quoted} NOT IN (SELECT {key_column_quoted} FROM deduped_source);

                                RETURN 'SCD2 upsert complete with operation logging';
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

        if len(pipeline_silver_data) != len(pipeline_bronze_data):
            raise ValueError("pipeline_silver_data and pipeline_bronze_data must have the same length")
           
        for pipeline_index, (silver_config, bronze_config) in enumerate(zip(pipeline_silver_data, pipeline_bronze_data), 1):
            cdc_config = silver_config["silver_cdc_apply_changes"]
            scd_type = cdc_config["scd_type"]
            silver_database = silver_config["silver_database_dev"]
            silver_schema = silver_config["silver_schema"]
            silver_table = silver_config["silver_table"]
            bronze_database = silver_config["bronze_database_dev"]
            bronze_schema = silver_config["bronze_schema"]
            bronze_table = silver_config["bronze_table"]
            if bronze_config.get("load_strategy") == "full":
                bronze_table = f"STREAM_{bronze_table}"

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
                    scd2_procedure_sql = self.create_scd2_stored_procedure(silver_config,stream_name=bronze_table)
                if scd_type == "1":
                    scd1_procedure_sql = self.create_scd1_stored_procedure(silver_config,stream_name=bronze_table)

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