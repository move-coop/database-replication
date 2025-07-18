import os
from typing import List, Optional

import dlt
from dlt.sources.sql_database import sql_database

from utilities.config import BIGQUERY_DESTINATION_CONFIG, SQL_SOURCE_CONFIG
from utilities.logger import logger
from utilities.setup import get_jdbc_connection_string, set_dlt_environment_variables

##########


def table_adapter_callback(query, table):
    if os.environ.get("FILTER_CLAUSE"):
        from sqlalchemy.sql import text

        filter_text = os.environ["FILTER_CLAUSE"]

        query = query.where(text(filter_text))
    return query


def run_import(
    vendor_name: str,
    source_schema_name: str,
    source_table_names: List[str],
    destination_schema_name: str,
    connection_string: str,
    row_chunk_size: Optional[int] = 10_000,
):
    """
    Executes an import from a remote host to the destination warehouse
    """

    # Establish pipeline connection to BigQuery
    pipeline = dlt.pipeline(
        pipeline_name=f"tmc_{vendor_name}",
        destination="bigquery",
        dataset_name=destination_schema_name,
        progress=dlt.progress.log(dump_system_stats=False, log_period=60),
    )

    # Setup connection to source database
    source_postgres_connection = sql_database(
        credentials=connection_string,
        schema=source_schema_name,
        table_names=source_table_names,
        chunk_size=row_chunk_size,
        query_adapter_callback=table_adapter_callback,
    )

    # Kick off the read -> write
    info = pipeline.run(
        source_postgres_connection,
        write_disposition="replace",  # TODO - Update this to allow for incremental builds
    )
    logger.info(info)


#####

if __name__ == "__main__":
    logger.debug("** DEBUGGER ACTIVER **")

    ENV_CONFIG = {**BIGQUERY_DESTINATION_CONFIG, **SQL_SOURCE_CONFIG}
    set_dlt_environment_variables(ENV_CONFIG)

    CONNECTION_STRING = get_jdbc_connection_string(config=SQL_SOURCE_CONFIG)

    ###

    VENDOR_NAME = os.environ["VENDOR_NAME"]

    SOURCE_SCHEMA_NAME = os.environ["SOURCE_SCHEMA_NAME"]
    SOURCE_TABLE_NAMES = [
        table.strip() for table in os.environ["SOURCE_TABLE_NAME"].split(",")
    ]
    DESTINATION_SCHEMA_NAME = os.environ["DESTINATION_SCHEMA_NAME"]

    FULL_REFRESH = os.environ.get("FULL_REFRESH") == "true"
    ROW_CHUNK_SIZE = int(os.environ.get("ROW_CHUNK_SIZE", 10_000))

    run_import(
        vendor_name=VENDOR_NAME.lower().replace(" ", "_"),
        source_schema_name=SOURCE_SCHEMA_NAME,
        source_table_names=SOURCE_TABLE_NAMES,
        destination_schema_name=DESTINATION_SCHEMA_NAME,
        connection_string=CONNECTION_STRING,
        row_chunk_size=ROW_CHUNK_SIZE,
    )
