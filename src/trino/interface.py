import click

from .utilities.connection_clients import (
    ConnectionClient,
    SourceClient,
    TrinoClient,
)
from .utilities.setup import (
    SOURCE_DATABASE_CREDENTIALS,
    TRINO_CONNECTION_CREDENTIALS,
)

#####


@click.command()
@click.option(
    "--source-schema-name", required=True, help="Schema name in the source database"
)
@click.option(
    "--source-table-name", required=True, help="Table name in the source database"
)
@click.option(
    "--row-chunk-size", default=10_000, help="Number of rows to transfer in each chunk"
)
@click.option(
    "--write-disposition",
    default="truncate",
    help="Write disposition for the data transfer",
)
def main(
    source_schema_name: str,
    source_table_name: str,
    row_chunk_size: int,
    write_disposition: str,
):
    trino_client = TrinoClient(**TRINO_CONNECTION_CREDENTIALS)
    source_client = SourceClient(**SOURCE_DATABASE_CREDENTIALS)

    connection_client = ConnectionClient(
        source_client=source_client,
        source_schema_name=source_schema_name,
        source_table_name=source_table_name,
        destination_client=trino_client,
        row_chunk_size=row_chunk_size,
        write_disposition=write_disposition,
    )

    connection_client.transfer()


#####

if __name__ == "__main__":
    main()
