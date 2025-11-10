from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine

import trino
from common.logger import logger

#####


@dataclass
class TrinoClient:
    """
    This class connects directly to the Trino cluster.

    Args:
        username (str): The username for the Trino cluster.
        password (str): The password for the Trino cluster.
        host (str): The host for the Trino cluster.
        port (int): The port for the Trino cluster.
        catalog (str): The catalog for the Trino cluster.
        schema (str): The schema for the Trino cluster.
    """

    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    @contextmanager
    def connection(self):
        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            catalog=self.catalog,
            schema=self.schema,
        )
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def cursor(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def build_table(
        self,
        source_catalog: str,
        source_schema: str,
        source_table: str,
        destination_table: Optional[str] = None,
    ):
        """
        Build a table in the destination catalog and schema by copying data from the source table.
        """

        # If destination_table is not provided, use source_table name
        destination_table = destination_table or source_table

        # TODO - Resolve SQL injection risk with parameters
        create_table_query = f"""
        CREATE TABLE {self.catalog}.{self.schema}.{destination_table} AS
        SELECT * FROM {source_catalog}.{source_schema}.{source_table}
        """
        logger.debug(f"Executing query: {create_table_query}")

        with self.cursor() as cursor:
            cursor.execute(create_table_query)

        logger.info(
            f"Table {self.catalog}.{self.schema}.{destination_table} created successfully."
        )
