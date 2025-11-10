import click
from utilities.connection_clients import (
    TrinoClient,
)
from utilities.setup import (
    TRINO_CONNECTION_CREDENTIALS,
)

from common.logger import logger
from typing import Optional

#####


@click.command()
@click.option(
    "--source-catalog", required=True, type=str, help="Source Trino catalog name."
)
@click.option(
    "--source-schema", required=True, type=str, help="Source Trino schema name."
)
@click.option(
    "--source-table", required=True, type=str, help="Source Trino table name."
)
@click.option(
    "--destination-table",
    required=False,
    type=str,
    help="Destination Trino table name.",
)
def main(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    destination_table: Optional[str] = None,
):
    logger.debug("Setting up connectors...")
    trino_client = TrinoClient(**TRINO_CONNECTION_CREDENTIALS)

    trino_client.build_table(
        source_catalog=source_catalog,
        source_schema=source_schema,
        source_table=source_table,
        destination_table=destination_table,
    )


#####

if __name__ == "__main__":
    main()
