"""Control Table Spec related utilities for Snowflake."""
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger("snow-meta")
logger.setLevel(logging.INFO)


@dataclass
class BronzeControlTableSpec:
    """A schema to hold a control table spec used for writing to the bronze layer in Snowflake."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: Dict[str, Any]
    readerConfigOptions: Dict[str, Any]
    targetFormat: str
    targetDetails: Dict[str, Any]
    tableProperties: Dict[str, Any]
    schema: str
    partitionColumns: List[str]
    cdcApplyChanges: str
    applyChangesFromSnapshot: str
    dataQualityExpectations: str
    quarantineTargetDetails: Dict[str, Any]
    quarantineTableProperties: Dict[str, Any]
    appendFlows: str
    appendFlowsSchemas: Dict[str, Any]
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: List[str]
    sinks: str


@dataclass
class SilverControlTableSpec:
    """A schema to hold a control table spec used for writing to the silver layer in Snowflake."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: Dict[str, Any]
    readerConfigOptions: Dict[str, Any]
    targetFormat: str
    targetDetails: Dict[str, Any]
    tableProperties: Dict[str, Any]
    selectExp: List[str]
    whereClause: List[str]
    partitionColumns: List[str]
    cdcApplyChanges: str
    applyChangesFromSnapshot: str
    dataQualityExpectations: str
    quarantineTargetDetails: Dict[str, Any]
    quarantineTableProperties: Dict[str, Any]
    appendFlows: str
    appendFlowsSchemas: Dict[str, Any]
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: List[str]
    sinks: str


@dataclass
class CDCApplyChanges:
    """CDC ApplyChanges structure for Snowflake."""

    keys: List[str]
    sequence_by: str
    where: str
    ignore_null_updates: bool
    apply_as_deletes: str
    apply_as_truncates: str
    column_list: List[str]
    except_column_list: List[str]
    scd_type: str
    track_history_column_list: List[str]
    track_history_except_column_list: List[str]
    flow_name: str
    once: bool
    ignore_null_updates_column_list: List[str]
    ignore_null_updates_except_column_list: List[str]


@dataclass
class ApplyChangesFromSnapshot:
    """CDC ApplyChangesFromSnapshot structure for Snowflake."""
    keys: List[str]
    scd_type: str
    track_history_column_list: List[str]
    track_history_except_column_list: List[str]


@dataclass
class AppendFlow:
    """Append Flow structure for Snowflake."""
    name: str
    comment: str
    create_streaming_table: bool
    source_format: str
    source_details: Dict[str, Any]
    reader_options: Dict[str, Any]
    spark_conf: Dict[str, Any]
    once: bool


@dataclass
class SnowflakeSink:
    """Snowflake Sink structure."""
    name: str
    format: str
    options: Dict[str, Any]
    select_exp: List[str]
    where_clause: str


class ControlTableSpecUtils:
    """A collection of methods for working with ControlTableSpec in Snowflake."""

    cdc_applychanges_api_mandatory_attributes = ["keys", "sequence_by", "scd_type"]
    cdc_applychanges_api_attributes = [
        "keys",
        "sequence_by",
        "where",
        "ignore_null_updates",
        "apply_as_deletes",
        "apply_as_truncates",
        "column_list",
        "except_column_list",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list",
        "flow_name",
        "once",
        "ignore_null_updates_column_list",
        "ignore_null_updates_except_column_list"
    ]

    cdc_applychanges_api_attributes_defaults = {
        "where": None,
        "ignore_null_updates": False,
        "apply_as_deletes": None,
        "apply_as_truncates": None,
        "column_list": None,
        "except_column_list": None,
        "track_history_column_list": None,
        "track_history_except_column_list": None,
        "flow_name": None,
        "once": False,
        "ignore_null_updates_column_list": None,
        "ignore_null_updates_except_column_list": None
    }

    append_flow_mandatory_attributes = ["name", "source_format", "create_streaming_table", "source_details"]
    sink_mandatory_attributes = ["name", "format", "options"]
    supported_sink_formats = ["snowflake", "kafka", "eventhub"]

    append_flow_api_attributes_defaults = {
        "comment": None,
        "create_streaming_table": False,
        "reader_options": None,
        "spark_conf": None,
        "once": False
    }

    sink_attributes_defaults = {
        "select_exp": None,
        "where_clause": None
    }

    additional_bronze_df_columns = [
        "appendFlows",
        "appendFlowsSchemas",
        "applyChangesFromSnapshot",
        "clusterBy",
        "sinks"
    ]
    additional_silver_df_columns = [
        "dataQualityExpectations",
        "quarantineTargetDetails",
        "quarantineTableProperties",
        "appendFlows",
        "appendFlowsSchemas",
        "applyChangesFromSnapshot",
        "clusterBy",
        "sinks"
    ]
    additional_cdc_apply_changes_columns = ["flow_name", "once"]
    apply_changes_from_snapshot_api_attributes = [
        "keys",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list"
    ]
    apply_changes_from_snapshot_api_mandatory_attributes = ["keys", "scd_type"]
    additional_apply_changes_from_snapshot_columns = ["track_history_column_list", "track_history_except_column_list"]
    apply_changes_from_snapshot_api_attributes_defaults = {
        "track_history_column_list": None,
        "track_history_except_column_list": None
    }

    @staticmethod
    def _get_control_table_spec(
        connection: snowflake.connector.SnowflakeConnection,
        layer: str,
        control_table_spec_df: Optional[Any] = None,
        group: Optional[str] = None,
        dataflow_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get ControlTableSpec for given parameters in Snowflake.

        Can be configured using connection parameters, used for optionally filtering
        the returned data to a group or list of DataflowIDs
        """
        cursor = connection.cursor(DictCursor)
        
        # Build the query based on parameters
        if layer == "bronze":
            table_name = "bronze_control_table"
        elif layer == "silver":
            table_name = "silver_control_table"
        else:
            raise ValueError(f"Unsupported layer: {layer}")

        query = f"SELECT * FROM {table_name}"
        conditions = []
        
        if group:
            conditions.append(f"dataFlowGroup = '{group}'")
        
        if dataflow_ids:
            ids_list = dataflow_ids.split(',')
            ids_str = "', '".join(ids_list)
            conditions.append(f"dataFlowId IN ('{ids_str}')")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Get latest version for each dataFlowId
        query = f"""
        WITH ranked_data AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY dataFlowGroup, dataFlowId ORDER BY version DESC) as rn
            FROM {table_name}
        )
        SELECT * FROM ranked_data WHERE rn = 1
        """
        
        if conditions:
            query = f"""
            WITH ranked_data AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY dataFlowGroup, dataFlowId ORDER BY version DESC) as rn
                FROM {table_name}
                WHERE {' AND '.join(conditions)}
            )
            SELECT * FROM ranked_data WHERE rn = 1
            """
        
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return results

    @staticmethod
    def get_bronze_control_table_spec(connection: snowflake.connector.SnowflakeConnection) -> List[BronzeControlTableSpec]:
        """Get bronze control table spec from Snowflake."""
        ControlTableSpecUtils.check_snowflake_controltable_conf_params(connection, "bronze")
        control_table_spec_rows = ControlTableSpecUtils._get_control_table_spec(connection, "bronze")
        bronze_control_table_spec_list: List[BronzeControlTableSpec] = []
        
        for row in control_table_spec_rows:
            target_row = ControlTableSpecUtils.populate_additional_df_cols(
                row,
                ControlTableSpecUtils.additional_bronze_df_columns
            )
            bronze_control_table_spec_list.append(BronzeControlTableSpec(**target_row))
        
        logger.info(f"bronze_control_table_spec_list={bronze_control_table_spec_list}")
        return bronze_control_table_spec_list

    @staticmethod
    def populate_additional_df_cols(onboarding_row_dict: Dict[str, Any], additional_columns: List[str]) -> Dict[str, Any]:
        """Populate additional columns with None if not present."""
        for column in additional_columns:
            if column not in onboarding_row_dict.keys():
                onboarding_row_dict[column] = None
        return onboarding_row_dict

    @staticmethod
    def get_silver_control_table_spec(connection: snowflake.connector.SnowflakeConnection) -> List[SilverControlTableSpec]:
        """Get silver control table spec list from Snowflake."""
        ControlTableSpecUtils.check_snowflake_controltable_conf_params(connection, "silver")

        control_table_spec_rows = ControlTableSpecUtils._get_control_table_spec(connection, "silver")
        silver_control_table_spec_list: List[SilverControlTableSpec] = []
        
        for row in control_table_spec_rows:
            target_row = ControlTableSpecUtils.populate_additional_df_cols(
                row,
                ControlTableSpecUtils.additional_silver_df_columns
            )
            silver_control_table_spec_list.append(SilverControlTableSpec(**target_row))
        
        return silver_control_table_spec_list

    @staticmethod
    def check_snowflake_controltable_conf_params(connection: snowflake.connector.SnowflakeConnection, layer_arg: str):
        """Check control table config params for Snowflake."""
        cursor = connection.cursor()
        
        # Check if the control table exists
        if layer_arg == "bronze":
            table_name = "bronze_control_table"
        elif layer_arg == "silver":
            table_name = "silver_control_table"
        else:
            raise ValueError(f"Unsupported layer: {layer_arg}")
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            cursor.fetchone()
        except Exception as e:
            raise Exception(
                f"Control table {table_name} does not exist or is not accessible. "
                f"Please ensure the table is created and accessible. Error: {str(e)}"
            )
        finally:
            cursor.close()

    @staticmethod
    def get_partition_cols(partition_columns: Optional[List[str]]) -> Optional[List[str]]:
        """Get partition columns for Snowflake."""
        partition_cols = None
        if partition_columns:
            if isinstance(partition_columns, str):
                partition_cols = partition_columns.split(',')
            else:
                if len(partition_columns) == 1:
                    if partition_columns[0] == "" or partition_columns[0].strip() == "":
                        partition_cols = None
                    else:
                        partition_cols = partition_columns
                else:
                    partition_cols = list(filter(None, partition_columns))
        return partition_cols

    @staticmethod
    def get_apply_changes_from_snapshot(apply_changes_from_snapshot: str) -> ApplyChangesFromSnapshot:
        """Get Apply changes from snapshot metadata for Snowflake."""
        logger.info(apply_changes_from_snapshot)
        json_apply_changes_from_snapshot = json.loads(apply_changes_from_snapshot)
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = set(
            ControlTableSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes).difference(payload_keys)
        logger.info(
            f"missing apply changes from snapshot payload keys:"
            f"{missing_apply_changes_from_snapshot_payload_keys}"
        )
        if set(ControlTableSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(ControlTableSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for apply changes from snapshot")
        else:
            logger.info(
                f"""all mandatory keys
                {ControlTableSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

        for missing_apply_changes_from_snapshot_payload_key in missing_apply_changes_from_snapshot_payload_keys:
            json_apply_changes_from_snapshot[
                missing_apply_changes_from_snapshot_payload_key
            ] = ControlTableSpecUtils.apply_changes_from_snapshot_api_attributes_defaults[
                missing_apply_changes_from_snapshot_payload_key]

        logger.info(f"final applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        json_apply_changes_from_snapshot = ControlTableSpecUtils.populate_additional_df_cols(
            json_apply_changes_from_snapshot,
            ControlTableSpecUtils.additional_apply_changes_from_snapshot_columns
        )
        return ApplyChangesFromSnapshot(**json_apply_changes_from_snapshot)

    @staticmethod
    def get_cdc_apply_changes(cdc_apply_changes: str) -> CDCApplyChanges:
        """Get CDC Apply changes metadata for Snowflake."""
        logger.info(cdc_apply_changes)
        json_cdc_apply_changes = json.loads(cdc_apply_changes)
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(ControlTableSpecUtils.cdc_applychanges_api_attributes).difference(payload_keys)
        logger.info(f"missing cdc payload keys:{missing_cdc_payload_keys}")
        if set(ControlTableSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(ControlTableSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for merge info")
        else:
            logger.info(
                f"""all mandatory keys
                {ControlTableSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

        for missing_cdc_payload_key in missing_cdc_payload_keys:
            json_cdc_apply_changes[
                missing_cdc_payload_key
            ] = ControlTableSpecUtils.cdc_applychanges_api_attributes_defaults[missing_cdc_payload_key]

        logger.info(f"final mergeInfo={json_cdc_apply_changes}")
        json_cdc_apply_changes = ControlTableSpecUtils.populate_additional_df_cols(
            json_cdc_apply_changes,
            ControlTableSpecUtils.additional_cdc_apply_changes_columns
        )
        return CDCApplyChanges(**json_cdc_apply_changes)

    @staticmethod
    def get_append_flows(append_flows: str) -> List[AppendFlow]:
        """Get append flow metadata for Snowflake."""
        logger.info(append_flows)
        json_append_flows = json.loads(append_flows)
        logger.info(f"actual appendFlow={json_append_flows}")
        list_append_flows = []
        for json_append_flow in json_append_flows:
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = (
                set(ControlTableSpecUtils.append_flow_api_attributes_defaults)
                .difference(payload_keys)
            )
            logger.info(f"missing append flow payload keys:{missing_append_flow_payload_keys}")
            if set(ControlTableSpecUtils.append_flow_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = (
                    set(ControlTableSpecUtils.append_flow_mandatory_attributes)
                    - set(payload_keys)
                )
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for append flow")
            else:
                logger.info(
                    f"""all mandatory keys
                    {ControlTableSpecUtils.append_flow_mandatory_attributes} exists"""
                )

            for missing_append_flow_payload_key in missing_append_flow_payload_keys:
                json_append_flow[
                    missing_append_flow_payload_key
                ] = ControlTableSpecUtils.append_flow_api_attributes_defaults[missing_append_flow_payload_key]

            logger.info(f"final appendFlow={json_append_flow}")
            list_append_flows.append(AppendFlow(**json_append_flow))
        return list_append_flows

    @staticmethod
    def get_sinks(sinks: str) -> List[SnowflakeSink]:
        """Get sink metadata for Snowflake."""
        logger.info(sinks)
        json_sinks = json.loads(sinks)
        snowflake_sinks = []
        for json_sink in json_sinks:
            logger.info(f"actual sink={json_sink}")
            payload_keys = json_sink.keys()
            missing_sink_payload_keys = set(ControlTableSpecUtils.sink_mandatory_attributes).difference(payload_keys)
            logger.info(f"missing sink payload keys:{missing_sink_payload_keys}")
            if set(ControlTableSpecUtils.sink_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = set(ControlTableSpecUtils.sink_mandatory_attributes) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for sink")
            else:
                logger.info(
                    f"""all mandatory keys
                    {ControlTableSpecUtils.sink_mandatory_attributes} exists"""
                )
            format = json_sink['format']
            if format not in ControlTableSpecUtils.supported_sink_formats:
                raise Exception(f"Unsupported sink format: {format}")
            if 'options' in json_sink.keys():
                json_sink['options'] = json.loads(json_sink['options'])
            if 'select_exp' in json_sink.keys():
                json_sink['select_exp'] = json_sink['select_exp']
            if 'where_clause' in json_sink.keys():
                json_sink['where_clause'] = json_sink['where_clause']
            for missing_sink_payload_key in missing_sink_payload_keys:
                json_sink[
                    missing_sink_payload_key
                ] = ControlTableSpecUtils.sink_attributes_defaults[missing_sink_payload_key]
            logger.info(f"final sink={json_sink}")
            snowflake_sinks.append(SnowflakeSink(**json_sink))
        return snowflake_sinks
