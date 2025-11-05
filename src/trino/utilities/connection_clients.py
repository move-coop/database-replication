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
            password=self.password,
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


@dataclass
class SourceClient:
    """
    This is an abstract class for source databases.

    Args:
        username (str): The username for the source database.
        password (str): The password for the source database.
        host (str): The host for the source database.
        port (int): The port for the source database.
        database (str): The database name for the source database.
        jdbc_driver (str): The JDBC driver for the source database.
        jdbc_connection_string (Optional[str], optional): The full JDBC connection string. Defaults to None.
    """

    username: str
    password: str
    host: str
    port: int
    database: str
    jdbc_driver: str
    jdbc_connection_string: Optional[str] = None

    @property
    def connection_string(self) -> str:
        """
        Returns the JDBC connection string for the source database.
        """

        # NOTE - if a full connection string is provided, we'll default to that
        if self.jdbc_connection_string:
            connection_string = self.jdbc_connection_string
        else:
            connection_string = (
                "jdbc+{DRIVER}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(
                    DRIVER=self.jdbc_driver,
                    USERNAME=self.username,
                    PASSWORD=self.password,
                    HOST=self.host,
                    PORT=self.port,
                    DATABASE=self.database,
                )
            )
        return connection_string

    @contextmanager
    def connection(self):
        """
        Context manager to create and dispose of a SQLAlchemy engine connection.
        """

        engine = create_engine(self.connection_string)
        connection = engine.connect()

        try:
            yield connection
        finally:
            connection.close()
            engine.dispose()

    @contextmanager
    def cursor(self):
        """
        Context manager to create and dispose of a cursor from the source database connection.
        """

        with self.connection() as conn:
            cursor = conn.connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


@dataclass
class ConnectionClient:
    """
    This class maintains a connection between the source database
    and the Trino cluster, and manages the data transfer.

    Args:
        source_client (SourceClient): The source database client.
        destination_client (TrinoClient): The Trino cluster client.
        source_table_name (str): The name of the source table.
        destination_table_name (Optional[str], optional): The name of the destination table. Defaults to the source table name
        row_chunk_size (Optional[int], optional): The number of rows to transfer in each chunk. Defaults to 10,000.
        write_disposition (Optional[str], optional): The write disposition for the data transfer. Defaults to "truncate".
        filter_clause (Optional[str], optional): An optional SQL filter clause to apply during data transfer. Defaults to None.
        include_views (bool, optional): Whether to include views in the data transfer. Defaults to True.
    """

    source_client: SourceClient
    destination_client: TrinoClient

    source_schema_name: str
    source_table_name: str
    _destination_schema_name: Optional[str] = None
    _destination_table_name: Optional[str] = None

    row_chunk_size: Optional[int] = 10_000
    write_disposition: Optional[str] = "truncate"
    filter_clause: Optional[str] = None
    include_views: bool = True

    @property
    def destination_table_name(self) -> str:
        return self._destination_table_name or self.source_table_name

    @property
    def destination_schema_name(self) -> str:
        return self._destination_schema_name or self.destination_client.schema

    def transfer(self):
        """
        Transfers data from the source table to the destination table.
        """

        with self.source_client.cursor() as _source_cursor:
            with self.destination_client.cursor() as _destination_cursor:
                source_query = (
                    f"SELECT * FROM {self.source_schema_name}.{self.source_table_name}"
                )
                if self.filter_clause:
                    source_query += f" WHERE {self.filter_clause}"

                _source_cursor.execute(source_query)

                while True:
                    rows = _source_cursor.fetchmany(self.row_chunk_size)

                    if not rows:
                        break

                    # Insert rows into destination table
                    for row in rows:
                        placeholders = ", ".join(["%s"] * len(row))
                        destination_query = f"INSERT INTO {self.destination_schema_name}.{self.destination_table_name} VALUES ({placeholders})"
                        _destination_cursor.execute(destination_query, row)
