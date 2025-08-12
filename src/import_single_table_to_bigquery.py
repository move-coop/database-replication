import os
from typing import List, Optional, Union

import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB, VARCHAR

from utilities.config import BIGQUERY_DESTINATION_CONFIG, SQL_SOURCE_CONFIG
from utilities.logger import logger
from utilities.setup import (
    get_jdbc_connection_string,
    set_dlt_environment_variables,
    validate_source_tables,
    validate_write_dispostiion,
)

##########


def table_adapter_callback(query, table):
    if os.environ.get("FILTER_CLAUSE"):
        from sqlalchemy.sql import text

        filter_text = os.environ["FILTER_CLAUSE"]

        query = query.where(text(filter_text))
    return query


def type_adapter_callback(sql_type):
    if isinstance(sql_type, (JSON, ARRAY, JSONB)):
        return VARCHAR
    return sql_type


def run_import(
    vendor_name: str,
    source_schema_name: str,
    source_table_names: Union[List[str], None],
    destination_schema_name: str,
    connection_string: str,
    write_disposition: str,
    row_chunk_size: Optional[int] = 10_000,
    include_views: bool = True,
):
    """
    Executes an import from a remote host to the destination warehouse

    Args:
        vendor_name:                Name of the vendor to sync (for alerting purposes)
        source_schema_name:         Schema to replicate on the source database
        source_table_names:         List of tables to replicate OR `None` (this will sync all tables)
        destination_schema_name:    Schema to write to in TMC's system
        connection_string:          JDBC string to authenticate source database
        write_disposition:          One of `append`, `replace`, or `drop`
        row_chunk_size:             Number of rows to return in a single request
        include_views:              If `True`, views on the source database will be replicated
    """

    logger.info(f"Beginning sync to {destination_schema_name}")
    if source_table_names:
        for table in source_table_names:
            logger.info(
                f"{source_schema_name}.{table} -> {destination_schema_name}.{table}"
            )
    else:
        logger.info("BE ADVISED - All tables in the source schema will be replicated")

    # Establish pipeline connection to BigQuery
    pipeline = dlt.pipeline(
        pipeline_name=f"tmc_{vendor_name}",
        destination="bigquery",
        dataset_name=destination_schema_name,
        progress=dlt.progress.log(log_period=60, logger=logger),
    )
    # Setup connection to source database
    source_postgres_connection = sql_database(
        credentials=connection_string,
        schema=source_schema_name,
        table_names=source_table_names,
        chunk_size=row_chunk_size,
        query_adapter_callback=table_adapter_callback,
        type_adapter_callback=type_adapter_callback,
        include_views=include_views,
    )
    source_postgres_connection.max_table_nesting = 0

    # Kick off the read -> write
    info = pipeline.run(source_postgres_connection, write_disposition=write_disposition)
    logger.info(info)


#####

if __name__ == "__main__":
    logger.debug("** DEBUGGER ACTIVER **")

    ENV_CONFIG = {**BIGQUERY_DESTINATION_CONFIG, **SQL_SOURCE_CONFIG}
    set_dlt_environment_variables(ENV_CONFIG)

    # Source parameters
    CONNECTION_STRING = get_jdbc_connection_string(config=SQL_SOURCE_CONFIG)
    SOURCE_SCHEMA_NAME = os.environ["SOURCE_SCHEMA_NAME"]
    SOURCE_TABLE_NAMES = validate_source_tables(os.environ["SOURCE_TABLE_NAME"])
    INCLUDE_VIEWS = os.environ.get("INCLUDE_VIEWS") != "false"

    # Destination parameters
    DESTINATION_SCHEMA_NAME = os.environ["DESTINATION_SCHEMA_NAME"]

    # Sync parameters
    VENDOR_NAME = os.environ["VENDOR_NAME"]
    ROW_CHUNK_SIZE = int(os.environ.get("ROW_CHUNK_SIZE", 10_000))
    WRITE_DISPOSITION = validate_write_dispostiion(
        os.environ["SOURCE_WRITE_DISPOSITION"]
    )

    run_import(
        vendor_name=VENDOR_NAME.lower().replace(" ", "_"),
        source_schema_name=SOURCE_SCHEMA_NAME,
        source_table_names=SOURCE_TABLE_NAMES,
        destination_schema_name=DESTINATION_SCHEMA_NAME,
        connection_string=CONNECTION_STRING,
        row_chunk_size=ROW_CHUNK_SIZE,
        write_disposition=WRITE_DISPOSITION,
        include_views=INCLUDE_VIEWS,
    )
