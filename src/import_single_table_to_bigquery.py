import os
from typing import Optional

import dlt
from sqlalchemy import create_engine, text

from utilities.config import BIGQUERY_DESTINATION_CONFIG, SQL_SOURCE_CONFIG
from utilities.logger import logger
from utilities.setup import get_jdbc_connection_string, set_dlt_environment_variables

##########


def query_table_in_segments(
    connection_string: str,
    source_schema_name: str,
    source_table_name: str,
    workers: int,
    index: int,
):
    """
    Executes one query subset against the larger source table
    """

    # Connect to remote host via SQLAlchemy engine
    engine = create_engine(connection_string)

    @dlt.resource(name=f"{source_table_name}_segment_{index}")
    def run_query():
        logger.info(f"Excecuting f{source_table_name}_segment_{index}")
        with engine.connect() as _conn:
            query = f"""
                WITH numbered AS (
                    SELECT *, ROW_NUMBER() OVER () AS rn
                    FROM {source_schema_name}.{source_table_name}
                )
                SELECT * FROM numbered WHERE rn % {workers} = {index}
            """

            result = (
                _conn.execution_options(stream_results=True)
                .execute(text(query))
                .mappings()
            )
            logger.debug("Parsing results...")
            for row in result:
                yield dict(row)

    return run_query


def run_import(
    vendor_name: str,
    source_schema_name: str,
    source_table_name: str,
    destination_schema_name: str,
    destination_table_name: str,
    full_refresh: bool,
    connection_string: Optional[str] = None,
    workers: int = 1,
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
    )

    # Breaks up source table in x number of jobs (1 per worker)
    resources = [
        query_table_in_segments(
            connection_string=connection_string,
            source_schema_name=source_schema_name,
            source_table_name=source_table_name,
            workers=workers,
            index=x,
        )
        for x in range(workers)
    ]

    logger.info(
        f"Beginning pipeline tmc_{vendor_name} [{source_schema_name}.{source_table_name} -> {destination_schema_name}.{destination_table_name}]"
    )

    # TODO - It seems like this ... isn't actually running?
    # It's just hanging out after the log statement above but never errors

    # TODO - I bet we could thread this thing
    info = pipeline.run(resources)
    print(info)


#####

if __name__ == "__main__":
    logger.debug("** DEBUGGER ACTIVER **")

    ENV_CONFIG = {**BIGQUERY_DESTINATION_CONFIG, **SQL_SOURCE_CONFIG}
    set_dlt_environment_variables(ENV_CONFIG)

    CONNECTION_STRING = get_jdbc_connection_string(config=SQL_SOURCE_CONFIG)

    ###

    VENDOR_NAME = os.environ["VENDOR_NAME"]

    SOURCE_SCHEMA_NAME = os.environ["SOURCE_SCHEMA_NAME"]
    SOURCE_TABLE_NAME = os.environ["SOURCE_TABLE_NAME"]
    DESTINATION_SCHEMA_NAME = os.environ["DESTINATION_SCHEMA_NAME"]
    DESTINATON_TABLE_NAME = os.environ["DESTINATION_TABLE_NAME"]

    FULL_REFRESH = os.environ.get("FULL_REFRESH") == "true"

    run_import(
        vendor_name=VENDOR_NAME.lower().replace(" ", "_"),
        source_schema_name=SOURCE_SCHEMA_NAME,
        source_table_name=SOURCE_TABLE_NAME,
        destination_schema_name=DESTINATION_SCHEMA_NAME,
        destination_table_name=DESTINATON_TABLE_NAME,
        full_refresh=FULL_REFRESH,
        connection_string=CONNECTION_STRING,
        workers=4,
    )
