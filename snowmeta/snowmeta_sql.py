"""
SnowmetaSQL: SQL generation utilities for Snowflake data operations.

This module provides SQL generation functionality that can be used across
the snowmeta package for creating stored procedures and views.
"""


class SnowmetaSQL:
    """
    A utility class for generating Snowflake SQL statements and stored procedures.
    
    This class provides methods to generate dynamic SQL for common Snowflake operations
    such as flattening JSON/VARIANT columns, creating stored procedures, and more.
    
    Usage:
        sql_gen = SnowmetaSQL()
        sql = sql_gen.flatten_json(...)
    """
    
    def __init__(self):
        """Initialize the SnowmetaSQL utility class."""
        pass

    def select_expression(
        self,
        bronze_database: str,
        bronze_schema: str,
        bronze_table: str,
        silver_database: str,
        silver_schema: str,
        select_expression: list[str]
        ) -> dict:
        """
        Generate a Snowflake stored procedure to create a view with a custom SELECT expression.

        Args:
            bronze_database: Source database (e.g., 'ANALYTICS')
            bronze_schema: Source schema (e.g., 'FINANCIAL_BRONZE')
            bronze_table: Source table name (e.g., 'products')
            silver_database: Target database (e.g., 'ANALYTICS')
            silver_schema: Target schema (e.g., 'FINANCIAL_SILVER')
            select_expression: List of select expressions for the view, e.g.,
                [
                    "product_id",
                    "product_details:name        AS product_name",
                    "product_details:brand       AS brand",
                    ...
                ]

        Returns:
            Dictionary containing:
                - sql: SQL string of the stored procedure
                - procedure_name: Name of the procedure
                - view_name: Name of the view generated

        Example:
            sql_gen = SnowmetaSQL()
            result = sql_gen.select_expression(
                bronze_database='ANALYTICS',
                bronze_schema='FINANCIAL_BRONZE',
                bronze_table='products',
                silver_database='ANALYTICS',
                silver_schema='FINANCIAL_SILVER',
                select_expression=[
                    "product_id",
                    "product_details:name        AS product_name",
                    "product_details:brand       AS brand",
                    "price:base                  AS base_price"
                ]
            )
            print(result['sql'])
            print(f"Procedure: {result['procedure_name']}")
            print(f"View: {result['view_name']}")
        """

        # Process the select_expression list to create the SQL SELECT statement
        select_sql = ",\n    ".join(select_expression)
        source_table = f"{bronze_database}.{bronze_schema}.{bronze_table}"
        target_schema = f"{silver_database}.{silver_schema}"
        view_name = f"FILTERED_{bronze_table.upper()}"
        procedure_name = f"AUTO_SELECT_{bronze_table}"

        sql = f"""
            CREATE OR REPLACE PROCEDURE {target_schema}.{procedure_name}()
            RETURNS STRING
            LANGUAGE SQL
            EXECUTE AS OWNER
            AS
            $$
            BEGIN
                -- Drop and recreate the view with the custom SELECT expression
                EXECUTE IMMEDIATE '
                    CREATE OR REPLACE VIEW {target_schema}.{view_name} AS
                    SELECT
                        {select_sql}
                    FROM {source_table}
                ';
                RETURN 'View {view_name} created with custom select expression.';
            END;
            $$;
        """

        return {
            "sql": sql,
            "procedure_name": procedure_name,
            "view_name": view_name
        }

    def where_expression(
        self,
        bronze_database: str,
        bronze_schema: str,
        bronze_table: str,
        silver_database: str,
        silver_schema: str,
        where_expression: list[str]
        ) -> dict:
        """
        Generate a Snowflake stored procedure to create a view with a custom WHERE clause.

        Args:
            bronze_database: Source database (e.g., 'ANALYTICS')
            bronze_schema: Source schema (e.g., 'FINANCIAL_BRONZE')
            bronze_table: Source table name (e.g., 'products')
            silver_database: Target database (e.g., 'ANALYTICS')
            silver_schema: Target schema (e.g., 'FINANCIAL_SILVER')
            where_expression: List of WHERE predicates combined with AND, e.g.,
                [
                    "product_details:name IS NOT NULL",
                    "price:base > 0",
                    "status = 'ACTIVE'"
                ]

        Returns:
            Dictionary containing:
                - sql: SQL string of the stored procedure
                - procedure_name: Name of the procedure
                - view_name: Name of the view generated

        Example:
            sql_gen = SnowmetaSQL()
            result = sql_gen.where_expression(
                bronze_database='ANALYTICS',
                bronze_schema='FINANCIAL_BRONZE',
                bronze_table='products',
                silver_database='ANALYTICS',
                silver_schema='FINANCIAL_SILVER',
                where_expression=[
                    "product_details:name IS NOT NULL",
                    "price:base > 0"
                ]
            )
            print(result['sql'])
            print(f"Procedure: {result['procedure_name']}")
            print(f"View: {result['view_name']}")
        """

        # Process the where_expression list to create the SQL WHERE clause
        where_sql = " AND\n    ".join(where_expression) if where_expression else "1=1"
        source_table = f"{bronze_database}.{bronze_schema}.{bronze_table}"
        target_schema = f"{silver_database}.{silver_schema}"
        view_name = f"FILTERED_{bronze_table.upper()}_WHERE"
        procedure_name = f"AUTO_WHERE_{bronze_table}"

        sql = f"""
            CREATE OR REPLACE PROCEDURE {target_schema}.{procedure_name}()
            RETURNS STRING
            LANGUAGE SQL
            EXECUTE AS OWNER
            AS
            $$
            BEGIN
                -- Drop and recreate the view with the custom WHERE clause
                EXECUTE IMMEDIATE '
                    CREATE OR REPLACE VIEW {target_schema}.{view_name} AS
                    SELECT
                        *
                    FROM {source_table}
                    WHERE
                        {where_sql}
                ';
                RETURN 'View {view_name} created with custom where expression.';
            END;
            $$;
        """

        return {
            "sql": sql,
            "procedure_name": procedure_name,
            "view_name": view_name
        }

    def flatten_json(
        self,
        bronze_database: str,
        bronze_schema: str,
        bronze_table: str,
        silver_database: str,
        silver_schema: str,
        columns_to_flatten: list[str]
    ) -> dict:
        """
        Generate a Snowflake stored procedure to flatten multiple JSON/VARIANT columns into one view
        
        Creates a single stored procedure that flattens all specified columns into one unified view.
        Each flattened column is prefixed by its source column name (e.g., PRODUCT_DETAILS_NAME).
        
        Args:
            bronze_database: Source database (e.g., 'ANALYTICS')
            bronze_schema: Source schema (e.g., 'FINANCIAL_BRONZE')
            bronze_table: Source table name (e.g., 'products')
            silver_database: Target database (e.g., 'ANALYTICS')
            silver_schema: Target schema (e.g., 'FINANCIAL_SILVER')
            columns_to_flatten: List of JSON/VARIANT column names to flatten (e.g., ['product_details', 'shipping_info'])
        
        Returns:
            Dictionary containing:
                - sql: SQL string containing the stored procedure
                - procedure_name: Name of the generated procedure
                - view_name: Name of the generated view
        
        Example:
            sql_gen = SnowmetaSQL()
            result = sql_gen.flatten_json(
                bronze_database='ANALYTICS',
                bronze_schema='FINANCIAL_BRONZE',
                bronze_table='products',
                silver_database='ANALYTICS',
                silver_schema='FINANCIAL_SILVER',
                columns_to_flatten=['product_details', 'shipping_info']
            )
            print(result['sql'])
            print(f"Procedure: {result['procedure_name']}")
            print(f"View: {result['view_name']}")
        """
        source_table = f"{bronze_database}.{bronze_schema}.{bronze_table}"
        target_schema = f"{silver_database}.{silver_schema}"
        procedure_name = f"auto_flatten_{bronze_table}"
        view_name = f"v_{bronze_table}_flat"
        
        # Build DECLARE section
        declare_vars = []
        for i, column in enumerate(columns_to_flatten):
            declare_vars.append(f"keys_{i} ARRAY;")
            declare_vars.append(f"key_list_{i} STRING;")
        declare_vars.append("sql_stmt STRING;")
        
        # Build key extraction section
        key_extractions = []
        for i, column in enumerate(columns_to_flatten):
            column_prefix = column.upper()
            key_extractions.append(f"""
        -- Extract keys for {column}
        SELECT ARRAY_AGG(DISTINCT f.key) INTO :keys_{i}
        FROM {source_table} p,
             LATERAL FLATTEN(input => p.{column}) f
        WHERE p.{column} IS NOT NULL;

        IF (keys_{i} IS NOT NULL AND ARRAY_SIZE(keys_{i}) > 0) THEN
            SELECT LISTAGG(CONCAT('''', value, ''' AS ', value), ', ') INTO :key_list_{i}
            FROM TABLE(FLATTEN(input => :keys_{i}));
        ELSE
            key_list_{i} := NULL;
        END IF;""")
        
        # Build the view construction SQL
        # Strategy: Use CTEs to flatten each column, then join them all together
        sql = f"""CREATE OR REPLACE PROCEDURE {target_schema}.{procedure_name}()
            RETURNS STRING
            LANGUAGE SQL
            AS
            $$
            DECLARE
                {chr(10).join(['        ' + v for v in declare_vars])}
            BEGIN
            {chr(10).join(key_extractions)}

                -- Build the view SQL with CTEs
                sql_stmt := 'CREATE OR REPLACE VIEW {target_schema}.{view_name} AS WITH ';
        """
        
        # Build CTE for each column
        # All CTEs should exclude ALL columns being flattened to avoid them appearing in the final result
        all_exclude_cols = ', '.join(columns_to_flatten)
        
        for i, column in enumerate(columns_to_flatten):
            if i > 0:
                sql += f"""
                IF (key_list_{i} IS NOT NULL) THEN
                    sql_stmt := sql_stmt || ', ';
                END IF;
                """
            
            # All CTEs exclude ALL columns being flattened
            sql += f"""
            IF (key_list_{i} IS NOT NULL) THEN
                sql_stmt := sql_stmt || 'cte_{i} AS (
                    SELECT * FROM (
                        SELECT
                            p.* EXCLUDE ({all_exclude_cols}),
                            f.key AS key_{i},
                            f.value AS value_{i}
                        FROM {source_table} p,
                            LATERAL FLATTEN(input => p.{column}) f
                    )
                    PIVOT (MAX(value_{i}) FOR key_{i} IN (' || key_list_{i} || '))
                )';
            END IF;
            """
        
        # Build the final SELECT - start with base table or first CTE
        sql += f"""
            -- Build final SELECT
            sql_stmt := sql_stmt || ' SELECT ';
        """
        
        # Build column selection - SELECT * with NATURAL JOIN auto-deduplicates common columns
        sql += f"""
            sql_stmt := sql_stmt || '*';
            
            -- Build FROM clause
            sql_stmt := sql_stmt || ' FROM cte_0';
        """
        
        # Build JOIN clauses using NATURAL JOIN (automatically joins on common columns and deduplicates)
        for i in range(1, len(columns_to_flatten)):
            sql += f"""
            IF (key_list_{i} IS NOT NULL) THEN
                sql_stmt := sql_stmt || ' NATURAL JOIN cte_{i}';
            END IF;
            """
        
        sql += f"""
            sql_stmt := sql_stmt || ';';

            -- Execute the dynamic SQL
            EXECUTE IMMEDIATE :sql_stmt;

            RETURN '✅ View {target_schema}.{view_name} created with flattened columns: {", ".join(columns_to_flatten)}';
        END;
        $$;
        """
        
        return {
            'sql': sql,
            'procedure_name': procedure_name,
            'view_name': view_name
        }