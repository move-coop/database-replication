import os

import dlt
import psycopg2

from utilities.config import BIGQUERY_DESTINATION_CONFIG, SQL_SOURCE_CONFIG
from utilities.logger import logger
from utilities.setup import get_jdbc_connection_string, set_dlt_environment_variables
from dlt.sources.sql_database import sql_database

##########


def pg_source():
    """
    Defines connection to remote Postgres and yields result
    """

    with psycopg2.connect(
        host=os.environ["SOURCE_HOST"],
        port=os.environ["SOURCE_PORT"],
        dbname=os.environ["SOURCE_DATABASE"],
        user=os.environ["SOURCE_USERNAME"],
        password=os.environ["SOURCE_PASSWORD"],
    ) as _dlt_connection:
        with _dlt_connection.cursor() as _dlt_cursor:
            _dlt_cursor.execute(
                "SELECT * FROM {}.{}".format(
                    os.environ["SOURCE_SCHEMA_NAME"], os.environ["SOURCE_TABLE_NAME"]
                )
            )
            columns = [desc[0] for desc in _dlt_cursor.description]
            for row in _dlt_cursor:
                yield dict(zip(columns, row))

            logger.info(f"Read {_dlt_cursor.rowcount} rows...")


def run_import(
    vendor_name: str,
    source_schema_name: str,
    source_table_names: list,
    destination_schema_name: str,
    full_refresh: bool,
    connection_string: str,
):
    """
    Executes an import from a remote host to the destination warehouse
    """

    # Establish pipeline connection to BigQuery
    pipeline = dlt.pipeline(
        pipeline_name=f"tmc_{vendor_name}",
        destination="bigquery",
        dataset_name=destination_schema_name,
        full_refresh=full_refresh,
        progress="log",
    )

    # Setup connection to source database
    source_postgres_connection = sql_database(
        credentials=connection_string,
        schema=source_schema_name,
        table_names=source_table_names,
        chunk_size=10_000,
    )

    # Kick off the read -> write
    info = pipeline.run(
        source_postgres_connection,
        write_disposition="replace",
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
    DESTINATON_TABLE_NAME = os.environ["DESTINATION_TABLE_NAME"]

    FULL_REFRESH = os.environ.get("FULL_REFRESH") == "true"

    run_import(
        vendor_name=VENDOR_NAME.lower().replace(" ", "_"),
        source_schema_name=SOURCE_SCHEMA_NAME,
        source_table_name=SOURCE_TABLE_NAMES,
        destination_schema_name=DESTINATION_SCHEMA_NAME,
        full_refresh=FULL_REFRESH,
        connection_string=CONNECTION_STRING,
    )
