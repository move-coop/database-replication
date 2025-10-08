import os
from typing import List, Optional, Union

import click
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


def table_adapter_callback(query, table, filter_clause=None):
    if filter_clause:
        from sqlalchemy.sql import text

        query = query.where(text(filter_clause))
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
    filter_clause: Optional[str] = None,
):
    """
    Executes an import from a remote host to the destination warehouse

    """

    logger.info(f"Beginning sync to {destination_schema_name}")
    if source_table_names:
        for table in source_table_names:
            logger.info(
                f"{source_schema_name}.{table} -> {destination_schema_name}.{table}"
            )
    else:
        logger.info("BE ADVISED - All tables in the source schema will be replicated")

    def query_callback(query, table):
        return table_adapter_callback(query, table, filter_clause)

    # Establish pipeline connection to BigQuery
    pipeline = dlt.pipeline(
        pipeline_name=f"tmc_{vendor_name}",
        destination="bigquery",
        dataset_name=destination_schema_name,
        progress=dlt.progress.log(log_period=60, logger=logger)
    )
    # Setup connection to source database
    source_postgres_connection = sql_database(
        credentials=connection_string,
        schema=source_schema_name,
        table_names=source_table_names,
        chunk_size=row_chunk_size or 10_000,
        query_adapter_callback=query_callback,
        type_adapter_callback=type_adapter_callback,
        include_views=include_views,
    )
    source_postgres_connection.max_table_nesting = 0

    # Kick off the read -> write
    info = pipeline.run(source_postgres_connection, write_disposition=write_disposition)
    logger.info(info)


#####

@click.command()
@click.option(
    "--vendor-name",
    envvar="VENDOR_NAME",
    required=True,
    help="Name of the vendor to sync (for alerting purposes)"
)
@click.option(
    "--source-schema-name",
    envvar="SOURCE_SCHEMA_NAME",
    required=True,
    help="Schema to replicate on the source database"
)
@click.option(
    "--source-table-name",
    envvar="SOURCE_TABLE_NAME",
    required=True,
    help="Comma-separated list of tables to replicate or 'all' for all tables"
)
@click.option(
    "--destination-schema-name",
    envvar="DESTINATION_SCHEMA_NAME",
    required=True,
    help="Schema to write to in the destination system"
)
@click.option(
    "--source-write-disposition",
    envvar="SOURCE_WRITE_DISPOSITION",
    required=True,
    type=click.Choice(["append", "replace", "merge"]),
    help="Write disposition: append, replace, or merge"
)
@click.option(
    "--row-chunk-size",
    envvar="ROW_CHUNK_SIZE",
    default=10000,
    type=int,
    help="Number of rows to return in a single request"
)
@click.option(
    "--include-views",
    envvar="INCLUDE_VIEWS",
    is_flag=True,
    help="Include views in the replication"
)
@click.option(
    "--filter-clause",
    envvar="FILTER_CLAUSE",
    help="Optional SQL filter clause to apply to queries"
)
@click.option(
    "--local",
    envvar="LOCAL",
    is_flag=True,
    help="Enable local mode for development (not for production)"
)
def main(
    vendor_name: str,
    source_schema_name: str,
    source_table_name: str,
    destination_schema_name: str,
    source_write_disposition: str,
    row_chunk_size: int,
    include_views: bool,
    filter_clause: Optional[str],
    local: bool,
):
    """Import data from a PostgreSQL database to BigQuery using dlt."""
    
    if local:
        logger.warning("** LOCAL MODE ENABLED - THIS IS NOT FOR PRODUCTION USE **")
    
    logger.debug("** DEBUGGER ACTIVER **")

    # Set up environment variables for dlt
    ENV_CONFIG = {**BIGQUERY_DESTINATION_CONFIG, **SQL_SOURCE_CONFIG}
    set_dlt_environment_variables(ENV_CONFIG)

    # Source parameters
    CONNECTION_STRING = get_jdbc_connection_string(config=SQL_SOURCE_CONFIG)

    if local:
        logger.debug(f"Connection string: {CONNECTION_STRING}")

    # Validate and process source table names
    SOURCE_TABLE_NAMES = validate_source_tables(source_table_name)

    # Validate write disposition
    WRITE_DISPOSITION = validate_write_dispostiion(source_write_disposition)
    if WRITE_DISPOSITION is None:
        raise ValueError(f"Invalid write disposition: {source_write_disposition}")

    run_import(
        vendor_name=vendor_name.lower().replace(" ", "_"),
        source_schema_name=source_schema_name,
        source_table_names=SOURCE_TABLE_NAMES,
        destination_schema_name=destination_schema_name,
        connection_string=CONNECTION_STRING,
        row_chunk_size=row_chunk_size,
        write_disposition=WRITE_DISPOSITION,
        include_views=include_views,
        filter_clause=filter_clause,
    )


if __name__ == "__main__":
    main()
