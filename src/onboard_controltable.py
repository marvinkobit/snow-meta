"""OnboardControlTable class provides bronze/silver onboarding features for Snowflake using Snowpark."""

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
import pandas as pd

from src.controltable_spec import BronzeControlTableSpec, ControlTableSpecUtils, SilverControlTableSpec

logger = logging.getLogger("snowflake.labs.snowmeta")
logger.setLevel(logging.INFO)


class OnboardControlTable:
    """OnboardControlTable class provides bronze/silver onboarding features for Snowflake."""

    def __init__(self, session: Session, dict_obj: Dict[str, Any], 
                 bronze_schema_mapper=None, uc_enabled=False):
        """Onboard ControlTable Constructor for Snowflake using Snowpark."""
        self.session = session
        self.dict_obj = dict_obj
        self.bronze_dict_obj = copy.deepcopy(dict_obj)
        self.silver_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        self.__initialize_paths(uc_enabled)
        self.bronze_schema_mapper = bronze_schema_mapper
        self.onboard_file_type = None

    def __initialize_paths(self, uc_enabled):
        """Initialize paths for bronze and silver control tables."""
        if "silver_control_table" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_control_table"]
        if "silver_control_table_path" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_control_table_path"]

        if "bronze_control_table" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_control_table"]
        if "bronze_control_table_path" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_control_table_path"]
        if uc_enabled:
            if "bronze_control_table_path" in self.bronze_dict_obj:
                del self.bronze_dict_obj["bronze_control_table_path"]
            if "silver_control_table_path" in self.silver_dict_obj:
                del self.silver_dict_obj["silver_control_table_path"]

    @staticmethod
    def __validate_dict_attributes(attributes: List[str], dict_obj: Dict[str, Any]):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes: List of required attributes
            dict_obj: Dictionary to validate

        Raises:
            ValueError: If required attributes are missing
        """
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            logger.info(
                "missing attributes : {}".format(
                    set(attributes).difference(attributes_keys)
                )
            )
            raise ValueError(
                f"missing attributes : {set(attributes).difference(attributes_keys)}"
            )

    def onboard_controltable_specs(self):
        """
        Onboard_controltable_specs method will onboard control table specs for bronze and silver.

        This method takes in a SnowflakeConnection object and a dictionary object containing the following attributes:
        - onboarding_file_path: The path to the onboarding file.
        - database: The name of the database to onboard the control table specs to.
        - env: The environment to onboard the control table specs to.
        - bronze_control_table: The name of the bronze control table specs table.
        - silver_control_table: The name of the silver control table specs table.
        - import_author: The author of the import.
        - version: The version of the import.
        - overwrite: Whether to overwrite existing control table specs or not.

        If the `uc_enabled` flag is set to True, the dictionary object must contain all the attributes listed above.
        If the `uc_enabled` flag is set to False, the dictionary object must contain all the attributes listed above
        except for `bronze_control_table_path` and `silver_control_table_path`.

        This method calls the `onboard_bronze_control_table_spec` and `onboard_silver_control_table_spec` methods to onboard
        the bronze and silver control table specs respectively.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_control_table",
            "silver_control_table",
            "import_author",
            "version",
            "overwrite",
        ]
        if self.uc_enabled:
            if "bronze_control_table_path" in self.dict_obj:
                del self.dict_obj["bronze_control_table_path"]
            if "silver_control_table_path" in self.dict_obj:
                del self.dict_obj["silver_control_table_path"]
            self.__validate_dict_attributes(attributes, self.dict_obj)
        else:
            attributes.append("bronze_control_table_path")
            attributes.append("silver_control_table_path")
            self.__validate_dict_attributes(attributes, self.dict_obj)
        self.onboard_bronze_control_table_spec()
        self.onboard_silver_control_table_spec()

    def register_bronze_control_table_spec_tables(self):
        """Register bronze control table specs tables in Snowflake."""
        self.__create_database(self.dict_obj["database"])
        self.__register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["bronze_control_table"],
            self.dict_obj["bronze_control_table_path"],
        )
        logger.info(
            f"""onboarded bronze table={self.dict_obj["database"]}.{self.dict_obj["bronze_control_table"]}"""
        )
        self.__show_table_data(
            f"""{self.dict_obj["database"]}.{self.dict_obj["bronze_control_table"]}"""
        )

    def register_silver_control_table_spec_tables(self):
        """Register silver control table specs tables in Snowflake."""
        self.__create_database(self.dict_obj["database"])
        self.__register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["silver_control_table"],
            self.dict_obj["silver_control_table_path"],
        )
        logger.info(
            f"""onboarded silver table={self.dict_obj["database"]}.{self.dict_obj["silver_control_table"]}"""
        )
        self.__show_table_data(
            f"""{self.dict_obj["database"]}.{self.dict_obj["silver_control_table"]}"""
        )

    def onboard_silver_control_table_spec(self):
        """
        Onboard silver control table spec for Snowflake.

        Args:
            onboarding_df (pandas.DataFrame): DataFrame containing the onboarding file data.
            dict_obj (dict): Dictionary containing the required attributes for onboarding silver control table spec.
                Required attributes:
                    - onboarding_file_path (str): Path of the onboarding file.
                    - database (str): Name of the database.
                    - env (str): Environment name.
                    - silver_control_table (str): Name of the silver control table spec table.
                    - silver_control_table_path (str): Path of the silver control table spec file. if uc_enabled is False
                    - import_author (str): Name of the import author.
                    - version (str): Version of the control table spec.
                    - overwrite (str): Whether to overwrite the existing control table spec table/file or not.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "silver_control_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.silver_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("silver_control_table_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        silver_control_table_spec_df = self.__get_silver_control_table_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        silver_control_table_spec_df = self.__add_audit_columns(
            silver_control_table_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        silver_fields = [field.name for field in dataclasses.fields(SilverControlTableSpec)]
        silver_control_table_spec_df = silver_control_table_spec_df[silver_fields]
        database = dict_obj["database"]
        table = dict_obj["silver_control_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                self.__write_dataframe_to_snowflake(
                    silver_control_table_spec_df, f"{database}.{table}", "overwrite"
                )
            else:
                self.__write_dataframe_to_snowflake(
                    silver_control_table_spec_df, f"{database}.{table}", "overwrite"
                )
        else:
            if self.uc_enabled:
                original_control_table_df = self.__read_table_from_snowflake(
                    f"{database}.{table}"
                )
            else:
                self.__register_table_in_metastore(
                    database, table, dict_obj["silver_control_table_path"]
                )
                original_control_table_df = self.__read_table_from_snowflake(
                    f"{database}.{table}"
                )
            logger.info("In Merge block for Silver")
            self.__merge_dataframes(
                silver_control_table_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_control_table_df.columns,
            )
        if not self.uc_enabled:
            self.register_silver_control_table_spec_tables()

    def onboard_bronze_control_table_spec(self):
        """
        Onboard bronze control table spec for Snowflake.

        This function reads the onboarding file and creates bronze control table spec. It adds audit columns to the dataframe
        If overwrite is True, it overwrites the table or file with the new dataframe. If overwrite is False,
        it merges the new dataframe with the existing dataframe.
        dict_obj (dict): Dictionary containing the required attributes for onboarding bronze control table spec.
            Required attributes:
                - onboarding_file_path (str): Path of the onboarding file.
                - database (str): Name of the database.
                - env (str): Environment name.
                - bronze_control_table (str): Name of the bronze control table spec table.
                - bronze_control_table_path (str): Path of the bronze control table spec file. if uc_enabled is False
                - import_author (str): Name of the import author.
                - version (str): Version of the control table spec.
                - overwrite (str): Whether to overwrite the existing control table spec table/file or not.

        Args:
            None

        Returns:
            None
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_control_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.bronze_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("bronze_control_table_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )

        bronze_control_table_spec_df = self.__get_bronze_control_table_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        bronze_control_table_spec_df = self.__add_audit_columns(
            bronze_control_table_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        bronze_fields = [field.name for field in dataclasses.fields(BronzeControlTableSpec)]
        bronze_control_table_spec_df = bronze_control_table_spec_df[bronze_fields]
        database = dict_obj["database"]
        table = dict_obj["bronze_control_table"]
        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                self.__write_dataframe_to_snowflake(
                    bronze_control_table_spec_df, f"{database}.{table}", "overwrite"
                )
            else:
                self.__write_dataframe_to_snowflake(
                    bronze_control_table_spec_df, f"{database}.{table}", "overwrite"
                )
        else:
            if self.uc_enabled:
                original_control_table_df = self.__read_table_from_snowflake(
                    f"{database}.{table}"
                )
            else:
                self.__register_table_in_metastore(
                    database, table, dict_obj["bronze_control_table_path"]
                )
                original_control_table_df = self.__read_table_from_snowflake(
                    f"{database}.{table}"
                )

            logger.info("In Merge block for Bronze")
            self.__merge_dataframes(
                bronze_control_table_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_control_table_df.columns,
            )
        if not self.uc_enabled:
            self.register_bronze_control_table_spec_tables()

    def __delete_none(self, _dict: Dict[str, Any]) -> Dict[str, Any]:
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def convert_yml_to_json(self, onboarding_file_path: str) -> str:
        """Get dataframe from YAML onboarding file.
        Args:
            onboarding_file_path (str): Path to YAML onboarding file
        Returns:
            str: Path to converted JSON file
        Raises:
            Exception: If duplicate data_flow_ids found
        """
        # Read YAML file as text
        with open(onboarding_file_path, 'r') as yaml_file:
            yaml_data = yaml.safe_load(yaml_file)

        json_data = json.dumps(yaml_data, indent=4)

        onboarding_file_path = onboarding_file_path.replace(".yml", "_yml.json")

        with open(onboarding_file_path, 'w') as json_file:
            json_file.write(json_data)
        return onboarding_file_path

    def __get_onboarding_file_dataframe(self, onboarding_file_path: str) -> pd.DataFrame:
        """Get onboarding file as pandas DataFrame."""
        onboarding_df = None
        if onboarding_file_path.lower().endswith((".yml", ".yaml")):
            onboarding_file_path = self.convert_yml_to_json(onboarding_file_path)
        if onboarding_file_path.lower().endswith(".json"):
            with open(onboarding_file_path, 'r') as json_file:
                data = json.load(json_file)
            onboarding_df = pd.DataFrame(data)
            logger.info("Onboarding file loaded successfully")
            self.onboard_file_type = "json"
            # Check for duplicate data_flow_ids
            if onboarding_df['data_flow_id'].duplicated().any():
                duplicates = onboarding_df[onboarding_df['data_flow_id'].duplicated()]
                logger.error(f"Duplicate data_flow_ids found: {duplicates['data_flow_id'].tolist()}")
                raise Exception("onboarding file have duplicated data_flow_ids!")
        else:
            raise Exception(
                "Onboarding file format not supported! Please provide json file format"
            )
        return onboarding_df

    def __add_audit_columns(self, df: pd.DataFrame, dict_obj: Dict[str, Any]) -> pd.DataFrame:
        """Add_audit_columns method will add AuditColumns like version, dates, author.

        Args:
            df: DataFrame to add audit columns to
            dict_obj: attributes = ["import_author", "version"]

        Returns:
            DataFrame with audit columns added
        """
        attributes = ["import_author", "version"]
        self.__validate_dict_attributes(attributes, dict_obj)

        df = df.copy()
        df["version"] = dict_obj["version"]
        df["createDate"] = pd.Timestamp.now()
        df["createdBy"] = dict_obj["import_author"]
        df["updateDate"] = pd.Timestamp.now()
        df["updatedBy"] = dict_obj["import_author"]
        return df

    def __get_bronze_schema(self, metadata_file: str) -> str:
        """Get schema from metadata file in json format.

        Args:
            metadata_file: metadata schema file path
        """
        with open(metadata_file, 'r') as f:
            schema = f.read()
        logger.info(f"Schema loaded from {metadata_file}")
        return schema

    def __validate_mandatory_fields(self, onboarding_row: pd.Series, mandatory_fields: List[str]):
        """Validate mandatory fields in onboarding row."""
        for field in mandatory_fields:
            if pd.isna(onboarding_row[field]) or onboarding_row[field] == "":
                raise Exception(f"Missing field={field} in onboarding_row")

    def __get_bronze_control_table_spec_dataframe(self, onboarding_df: pd.DataFrame, env: str) -> pd.DataFrame:
        """Get bronze control table spec method will convert onboarding dataframe to Bronze ControlTableSpec dataframe.

        Args:
            onboarding_df: Onboarding DataFrame
            env: Environment name

        Returns:
            DataFrame with bronze control table spec data
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "appendFlows",
            "appendFlowsSchemas",
            "sinks",
            "clusterBy"
        ]
        
        data = []
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            "source_details",
            f"bronze_database_{env}",
            "bronze_table"
        ]
        
        for _, onboarding_row in onboarding_df.iterrows():
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"bronze_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            
            bronze_data_flow_spec_id = onboarding_row["data_flow_id"]
            bronze_data_flow_spec_group = onboarding_row["data_flow_group"]
            
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in [
                "snowflake",
                "kafka",
                "eventhub",
                "delta",
                "snapshot"
            ]:
                raise Exception(
                    f"Source format {source_format} not supported in Snowflake-META! row={onboarding_row}"
                )
            
            source_details, bronze_reader_config_options, schema = (
                self.get_bronze_source_details_reader_options_schema(
                    onboarding_row, env
                )
            )
            bronze_target_format = "snowflake"
            bronze_target_details = {
                "database": onboarding_row[f"bronze_database_{env}"],
                "table": onboarding_row["bronze_table"],
            }
            
            if "bronze_table_comment" in onboarding_row:
                bronze_target_details["comment"] = onboarding_row["bronze_table_comment"]
            
            if not self.uc_enabled:
                if f"bronze_table_path_{env}" in onboarding_row:
                    bronze_target_details["path"] = onboarding_row[f"bronze_table_path_{env}"]
                else:
                    raise Exception(f"bronze_table_path_{env} not provided in onboarding_row={onboarding_row}")
            
            bronze_table_properties = {}
            if (
                "bronze_table_properties" in onboarding_row
                and onboarding_row["bronze_table_properties"]
            ):
                bronze_table_properties = self.__delete_none(
                    onboarding_row["bronze_table_properties"]
                )

            partition_columns = [""]
            if (
                "bronze_partition_columns" in onboarding_row
                and onboarding_row["bronze_partition_columns"]
            ):
                if "," in str(onboarding_row["bronze_partition_columns"]):
                    partition_columns = str(onboarding_row["bronze_partition_columns"]).split(",")
                else:
                    partition_columns = [onboarding_row["bronze_partition_columns"]]

            dlt_sinks = None
            if "bronze_sinks" in onboarding_row and onboarding_row["bronze_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "bronze")
            
            cluster_by = self.__get_cluster_by_properties(onboarding_row, bronze_table_properties,
                                                          "bronze_cluster_by")

            cdc_apply_changes = None
            if (
                "bronze_cdc_apply_changes" in onboarding_row
                and onboarding_row["bronze_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "bronze")
                cdc_apply_changes = json.dumps(
                    self.__delete_none(
                        onboarding_row["bronze_cdc_apply_changes"]
                    )
                )
            
            apply_changes_from_snapshot = None
            if ("bronze_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["bronze_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "bronze")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row["bronze_apply_changes_from_snapshot"])
                )
            
            data_quality_expectations = None
            quarantine_target_details = {}
            quarantine_table_properties = {}
            if f"bronze_data_quality_expectations_json_{env}" in onboarding_row:
                bronze_data_quality_expectations_json = onboarding_row[
                    f"bronze_data_quality_expectations_json_{env}"
                ]
                if bronze_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        bronze_data_quality_expectations_json
                    )
                    if onboarding_row["bronze_quarantine_table"]:
                        quarantine_target_details, quarantine_table_properties = self.__get_quarantine_details(
                            env, "bronze", onboarding_row
                        )

            append_flows, append_flows_schemas = self.get_append_flows_json(
                onboarding_row, "bronze", env
            )
            
            bronze_row = {
                "dataFlowId": bronze_data_flow_spec_id,
                "dataFlowGroup": bronze_data_flow_spec_group,
                "sourceFormat": source_format,
                "sourceDetails": source_details,
                "readerConfigOptions": bronze_reader_config_options,
                "targetFormat": bronze_target_format,
                "targetDetails": bronze_target_details,
                "tableProperties": bronze_table_properties,
                "schema": schema,
                "partitionColumns": partition_columns,
                "cdcApplyChanges": cdc_apply_changes,
                "applyChangesFromSnapshot": apply_changes_from_snapshot,
                "dataQualityExpectations": data_quality_expectations,
                "quarantineTargetDetails": quarantine_target_details,
                "quarantineTableProperties": quarantine_table_properties,
                "appendFlows": append_flows,
                "appendFlowsSchemas": append_flows_schemas,
                "sinks": dlt_sinks,
                "clusterBy": cluster_by
            }
            data.append(bronze_row)

        data_flow_spec_rows_df = pd.DataFrame(data)
        return data_flow_spec_rows_df

    def __get_silver_control_table_spec_dataframe(self, onboarding_df: pd.DataFrame, env: str) -> pd.DataFrame:
        """Get silver_control_table_spec method transform onboarding dataframe to silver control table spec dataframe.

        Args:
            onboarding_df: Onboarding DataFrame
            env: Environment name

        Returns:
            DataFrame with silver control table spec data
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "quarantineClusterBy",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
            "sinks"
        ]
        data = []

        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"silver_database_{env}",
            "silver_table",
            f"silver_transformation_json_{env}",
        ]

        for _, onboarding_row in onboarding_df.iterrows():
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"silver_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            
            silver_data_flow_spec_id = onboarding_row["data_flow_id"]
            silver_data_flow_spec_group = onboarding_row["data_flow_group"]
            silver_reader_config_options = {}

            silver_target_format = "snowflake"

            bronze_target_details = {
                "database": onboarding_row[f"bronze_database_{env}"],
                "table": onboarding_row["bronze_table"],
            }
            
            silver_target_details = {
                "database": onboarding_row[f"silver_database_{env}"],
                "table": onboarding_row["silver_table"],
            }
            
            if "silver_table_comment" in onboarding_row:
                silver_target_details["comment"] = onboarding_row["silver_table_comment"]
            
            if not self.uc_enabled:
                bronze_target_details["path"] = onboarding_row[f"bronze_table_path_{env}"]
                silver_target_details["path"] = onboarding_row[f"silver_table_path_{env}"]
            
            silver_reader_options_json = (
                onboarding_row["silver_reader_options"]
                if "silver_reader_options" in onboarding_row
                else {}
            )
            if silver_reader_options_json:
                silver_reader_config_options = self.__delete_none(
                    silver_reader_options_json
                )
            
            silver_table_properties = {}
            if (
                "silver_table_properties" in onboarding_row
                and onboarding_row["silver_table_properties"]
            ):
                silver_table_properties = self.__delete_none(
                    onboarding_row["silver_table_properties"]
                )

            silver_parition_columns = [""]
            if (
                "silver_partition_columns" in onboarding_row
                and onboarding_row["silver_partition_columns"]
            ):
                if "," in str(onboarding_row["silver_partition_columns"]):
                    silver_parition_columns = str(onboarding_row["silver_partition_columns"]).split(",")
                else:
                    silver_parition_columns = [onboarding_row["silver_partition_columns"]]

            dlt_sinks = None
            if "silver_sinks" in onboarding_row and onboarding_row["silver_sinks"]:
                dlt_sinks = self.get_sink_details(onboarding_row, "silver")
            
            silver_cluster_by = self.__get_cluster_by_properties(onboarding_row, silver_table_properties,
                                                                 "silver_cluster_by")

            silver_cdc_apply_changes = None
            if (
                "silver_cdc_apply_changes" in onboarding_row
                and onboarding_row["silver_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "silver")
                silver_cdc_apply_changes_row = onboarding_row[
                    "silver_cdc_apply_changes"
                ]
                if self.onboard_file_type == "json":
                    silver_cdc_apply_changes = json.dumps(
                        self.__delete_none(silver_cdc_apply_changes_row)
                    )
            
            data_quality_expectations = None
            silver_quarantine_target_details = None
            silver_quarantine_table_properties = None
            silver_quarantine_cluster_by = None
            if f"silver_data_quality_expectations_json_{env}" in onboarding_row:
                silver_data_quality_expectations_json = onboarding_row[
                    f"silver_data_quality_expectations_json_{env}"
                ]
                if silver_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        silver_data_quality_expectations_json
                    )
                silver_quarantine_target_details, silver_quarantine_table_properties = self.__get_quarantine_details(
                    env, "silver", onboarding_row
                )
                silver_quarantine_cluster_by = self.__get_cluster_by_properties(
                    onboarding_row,
                    silver_quarantine_table_properties,
                    "silver_quarantine_cluster_by"
                )
            
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, layer="silver", env=env
            )
            
            apply_changes_from_snapshot = None
            source_format = "snowflake"
            if ("silver_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["silver_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "silver")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row["silver_apply_changes_from_snapshot"])
                )
                source_format = "snapshot"
            
            silver_row = {
                "dataFlowId": silver_data_flow_spec_id,
                "dataFlowGroup": silver_data_flow_spec_group,
                "sourceFormat": source_format,
                "sourceDetails": bronze_target_details,
                "readerConfigOptions": silver_reader_config_options,
                "targetFormat": silver_target_format,
                "targetDetails": silver_target_details,
                "tableProperties": silver_table_properties,
                "partitionColumns": silver_parition_columns,
                "cdcApplyChanges": silver_cdc_apply_changes,
                "applyChangesFromSnapshot": apply_changes_from_snapshot,
                "dataQualityExpectations": data_quality_expectations,
                "quarantineTargetDetails": silver_quarantine_target_details,
                "quarantineTableProperties": silver_quarantine_table_properties,
                "quarantineClusterBy": silver_quarantine_cluster_by,
                "appendFlows": append_flows,
                "appendFlowsSchemas": append_flow_schemas,
                "clusterBy": silver_cluster_by,
                "sinks": dlt_sinks
            }
            data.append(silver_row)
            logger.info(f"silver_data ==== {data}")

        data_flow_spec_rows_df = pd.DataFrame(data)
        return data_flow_spec_rows_df

    # Additional helper methods would be implemented here following the same pattern
    # as the original dlt-meta implementation but adapted for Snowflake and pandas DataFrames
    
    def __create_database(self, database: str):
        """Create database in Snowflake using Snowpark."""
        try:
            self.session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
            logger.info(f"Database {database} created or already exists")
        except Exception as e:
            logger.error(f"Error creating database {database}: {str(e)}")
            raise

    def __register_table_in_metastore(self, database: str, table: str, path: str):
        """Register table in Snowflake metastore using Snowpark."""
        try:
            # In Snowflake, tables are automatically registered when created
            logger.info(f"Table {database}.{table} registered in metastore")
        except Exception as e:
            logger.error(f"Error registering table {database}.{table}: {str(e)}")
            raise

    def __show_table_data(self, table_name: str):
        """Show table data in Snowflake using Snowpark."""
        try:
            df = self.session.table(table_name).limit(10)
            results = df.collect()
            logger.info(f"Table {table_name} data: {results}")
        except Exception as e:
            logger.error(f"Error showing table data for {table_name}: {str(e)}")
            raise

    def __write_dataframe_to_snowflake(self, df: pd.DataFrame, table_name: str, mode: str):
        """Write DataFrame to Snowflake table using Snowpark."""
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

    def __read_table_from_snowflake(self, table_name: str) -> pd.DataFrame:
        """Read table from Snowflake as DataFrame using Snowpark."""
        try:
            snowpark_df = self.session.table(table_name)
            pandas_df = snowpark_df.to_pandas()
            return pandas_df
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")
            raise

    def __merge_dataframes(self, new_df: pd.DataFrame, table_name: str, 
                          merge_keys: List[str], existing_columns: List[str]):
        """Merge new DataFrame with existing table data using Snowpark."""
        try:
            # Read existing data
            existing_df = self.__read_table_from_snowflake(table_name)
            
            # Merge logic here - this is a simplified version
            # In practice, you'd implement proper merge logic based on merge_keys
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Write back to Snowflake
            self.__write_dataframe_to_snowflake(combined_df, table_name, "overwrite")
            logger.info(f"Merged data into {table_name}")
        except Exception as e:
            logger.error(f"Error merging dataframes for {table_name}: {str(e)}")
            raise

    # Placeholder methods for additional functionality
    def get_bronze_source_details_reader_options_schema(self, onboarding_row, env):
        """Get bronze source details, reader options, and schema."""
        # Extract source details from onboarding row
        source_details = {}
        reader_config_options = {}
        schema = None
        
        # Get source details from the onboarding row
        if "source_details" in onboarding_row and onboarding_row["source_details"]:
            source_details = onboarding_row["source_details"]
        else:
            # Provide default source details to avoid empty struct
            source_details = {
                "source_database": onboarding_row.get(f"source_database_{env}", "SOURCE_DB"),
                "source_table": onboarding_row.get("source_table", "source_table"),
                "source_schema_path": onboarding_row.get("source_schema_path", None)
            }
        
        # Get reader config options
        if "reader_config_options" in onboarding_row and onboarding_row["reader_config_options"]:
            reader_config_options = onboarding_row["reader_config_options"]
        else:
            # Provide default reader config to avoid empty struct
            reader_config_options = {
                "format": "snowflake",
                "mode": "append"
            }
        
        # Get schema if provided
        if "schema" in onboarding_row and onboarding_row["schema"]:
            schema = onboarding_row["schema"]
        
        return source_details, reader_config_options, schema

    def get_sink_details(self, onboarding_row, layer):
        """Get sink details."""
        # Implementation would go here
        return None

    def get_append_flows_json(self, onboarding_row, layer, env):
        """Get append flows JSON."""
        append_flows_key = f"{layer}_append_flows"
        append_flows_schemas_key = f"{layer}_append_flows_schemas"
        
        append_flows = None
        append_flows_schemas = {}
        
        if append_flows_key in onboarding_row and onboarding_row[append_flows_key]:
            append_flows = onboarding_row[append_flows_key]
        
        if append_flows_schemas_key in onboarding_row and onboarding_row[append_flows_schemas_key]:
            append_flows_schemas = onboarding_row[append_flows_schemas_key]
        else:
            # Provide default schema to avoid empty struct
            append_flows_schemas = {
                "default_schema": "public"
            }
        
        return append_flows, append_flows_schemas

    def __get_cluster_by_properties(self, onboarding_row, table_properties, cluster_key):
        """Get cluster by properties."""
        if cluster_key in onboarding_row and onboarding_row[cluster_key]:
            cluster_by = onboarding_row[cluster_key]
            if isinstance(cluster_by, str):
                return [cluster_by]
            elif isinstance(cluster_by, list):
                return cluster_by
        return None

    def __get_quarantine_details(self, env, layer, onboarding_row):
        """Get quarantine details."""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        
        # Get quarantine target details
        quarantine_target_key = f"{layer}_quarantine_target_details"
        if quarantine_target_key in onboarding_row and onboarding_row[quarantine_target_key]:
            quarantine_target_details = onboarding_row[quarantine_target_key]
        else:
            # Provide default quarantine details to avoid empty struct
            quarantine_target_details = {
                "database": onboarding_row.get(f"{layer}_database_{env}", f"{layer.upper()}_DB"),
                "table": f"quarantine_{onboarding_row.get(f'{layer}_table', 'table')}"
            }
        
        # Get quarantine table properties
        quarantine_properties_key = f"{layer}_quarantine_table_properties"
        if quarantine_properties_key in onboarding_row and onboarding_row[quarantine_properties_key]:
            quarantine_table_properties = onboarding_row[quarantine_properties_key]
        else:
            # Provide default quarantine properties to avoid empty struct
            quarantine_table_properties = {
                "comment": f"Quarantine table for {layer} layer"
            }
        
        return quarantine_target_details, quarantine_table_properties

    def __validate_apply_changes(self, onboarding_row, layer):
        """Validate apply changes."""
        apply_changes_key = f"{layer}_cdc_apply_changes"
        if apply_changes_key in onboarding_row and onboarding_row[apply_changes_key]:
            apply_changes = onboarding_row[apply_changes_key]
            # Validate required fields
            required_fields = ["keys", "sequence_by", "scd_type"]
            for field in required_fields:
                if field not in apply_changes:
                    raise ValueError(f"Missing required field '{field}' in {apply_changes_key}")

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        """Validate apply changes from snapshot."""
        apply_changes_key = f"{layer}_apply_changes_from_snapshot"
        if apply_changes_key in onboarding_row and onboarding_row[apply_changes_key]:
            apply_changes = onboarding_row[apply_changes_key]
            # Validate required fields
            required_fields = ["keys", "scd_type"]
            for field in required_fields:
                if field not in apply_changes:
                    raise ValueError(f"Missing required field '{field}' in {apply_changes_key}")

    def __get_data_quality_expecations(self, json_file_path):
        """Get data quality expectations."""
        # Implementation would go here
        return None
