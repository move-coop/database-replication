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
                "{DRIVER}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(
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
        destination_table_name (Optional[str], optional): The name of the destination table. Defaults to the source table name.
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

    def _table_exists(self, cursor, schema_name: str, table_name: str) -> bool:
        """
        Check if a table exists in Trino.

        Args:
            cursor: The Trino cursor.
            schema_name (str): The schema name.
            table_name (str): The table name.

        Returns:
            bool: True if the table exists, False otherwise.
        """

        check_query = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}' 
        AND table_name = '{table_name}'
        """
        cursor.execute(check_query)
        result = cursor.fetchone()

        return result[0] > 0

    def _get_source_table_schema(self, cursor) -> list:
        """
        Get the column definitions from the source table.
        """

        schema_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = '{self.source_schema_name}' 
        AND table_name = '{self.source_table_name}'
        ORDER BY ordinal_position
        """
        cursor.execute(schema_query)

        return cursor.fetchall()

    def _map_data_type_to_trino(self, source_type: str) -> str:
        """
        Map source database data types to Trino data types.
        """
        # Basic type mapping - extend as needed
        type_mapping = {
            # PostgreSQL to Trino
            "integer": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "decimal": "decimal",
            "numeric": "decimal",
            "real": "real",
            "double precision": "double",
            "boolean": "boolean",
            "character varying": "varchar",
            "varchar": "varchar",
            "char": "char",
            "text": "varchar",
            "date": "date",
            "timestamp": "timestamp",
            "timestamptz": "timestamp with time zone",
            "time": "time",
            "uuid": "varchar",
            "json": "json",
            "jsonb": "json",
            # MySQL to Trino
            "int": "integer",
            "tinyint": "tinyint",
            "mediumint": "integer",
            "float": "real",
            "double": "double",
            "datetime": "timestamp",
            "longtext": "varchar",
            "mediumtext": "varchar",
            "tinytext": "varchar",
        }

        # Handle types with parameters (e.g., varchar(255))
        base_type = source_type.split("(")[0].lower()
        return type_mapping.get(base_type, "varchar")  # Default to varchar

    def _create_destination_table(self, dest_cursor, source_cursor):
        """
        Create the destination table based on source table schema.
        """
        logger.info(
            f"Creating destination table: {self.destination_schema_name}.{self.destination_table_name}"
        )

        # Get source table schema
        columns = self._get_source_table_schema(source_cursor)

        if not columns:
            raise ValueError(
                f"Could not retrieve schema for source table {self.source_schema_name}.{self.source_table_name}"
            )

        # Build CREATE TABLE statement
        column_definitions = []
        for column_name, data_type, is_nullable in columns:
            trino_type = self._map_data_type_to_trino(data_type)
            # Note: BigQuery catalog doesn't support NOT NULL constraints via Trino
            # All columns will be nullable in the destination
            column_definitions.append(f"  {column_name} {trino_type}")

        columns_sql = ",\n".join(column_definitions)

        create_table_query = f"""
        CREATE TABLE {self.destination_schema_name}.{self.destination_table_name} (
{columns_sql}
        )
        """

        logger.debug(f"Creating table with query: {create_table_query}")
        dest_cursor.execute(create_table_query)
        logger.info(
            f"Successfully created table {self.destination_schema_name}.{self.destination_table_name}"
        )

    def transfer(self):
        """
        Transfers data from the source table to the destination table.
        Creates destination table if it doesn't exist.
        """

        logger.info(
            f"Starting transfer: {self.source_schema_name}.{self.source_table_name} -> {self.destination_schema_name}.{self.destination_table_name}"
        )

        with self.source_client.cursor() as _source_cursor:
            with self.destination_client.cursor() as _destination_cursor:
                # Check if destination table exists, create if not
                if not self._table_exists(
                    _destination_cursor,
                    self.destination_schema_name,
                    self.destination_table_name,
                ):
                    logger.info("Destination table does not exist, creating it...")
                    self._create_destination_table(_destination_cursor, _source_cursor)

                # Handle write disposition
                if self.write_disposition == "truncate":
                    logger.info("Truncating destination table...")
                    truncate_query = f"DELETE FROM {self.destination_schema_name}.{self.destination_table_name}"
                    _destination_cursor.execute(truncate_query)

                # Build and execute source query
                source_query = (
                    f"SELECT * FROM {self.source_schema_name}.{self.source_table_name}"
                )
                if self.filter_clause:
                    source_query += f" WHERE {self.filter_clause}"

                logger.debug(f"Executing source query: {source_query}")
                _source_cursor.execute(source_query)

                while True:
                    logger.debug("Fetching next chunk of rows from source...")
                    rows = _source_cursor.fetchmany(self.row_chunk_size)

                    if not rows:
                        logger.info("No more rows to fetch from source.")
                        break

                    # Insert rows into destination table
                    logger.info(
                        f"Inserting {len(rows)} rows into {self.destination_schema_name}.{self.destination_table_name}..."
                    )
                    for row in rows:
                        # Format values for Trino (no parameterized queries)
                        formatted_values = []
                        for value in row:
                            if value is None:
                                formatted_values.append("NULL")
                            elif isinstance(value, str):
                                # Escape single quotes and wrap in quotes
                                escaped_value = value.replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                            elif isinstance(value, (int, float)):
                                formatted_values.append(str(value))
                            else:
                                # Convert other types to string and quote
                                formatted_values.append(f"'{str(value)}'")

                        values_str = ", ".join(formatted_values)
                        destination_query = f"INSERT INTO {self.destination_schema_name}.{self.destination_table_name} VALUES({values_str})"
                        logger.debug(destination_query)
                        _destination_cursor.execute(destination_query)
