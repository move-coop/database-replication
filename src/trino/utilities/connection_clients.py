from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import trino
from common.logger import logger

#####

TYPE_MAPPER = {
    "json": "JSON_FORMAT({column_name}) AS {column_name}",
    "jsonb": "JSON_FORMAT({column_name}) AS {column_name}",
    "uuid": "CAST({column_name} AS VARCHAR) AS {column_name}",
}


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
        Handles data type compatibility between source and destination catalogs.
        """

        # If destination_table is not provided, use source_table name
        destination_table = destination_table or source_table

        logger.info("Building table...")
        logger.info(f"Source: {source_catalog}.{source_schema}.{source_table}")
        logger.info(f"Destination: {self.catalog}.{self.schema}.{destination_table}")

        # Get column information to handle type conversions
        with self.cursor() as cursor:
            # Get column metadata from information schema
            columns_query = f"""
            SELECT column_name, data_type 
            FROM {source_catalog}.information_schema.columns 
            WHERE table_schema = '{source_schema}' 
            AND table_name = '{source_table}'
            ORDER BY ordinal_position
            """

            cursor.execute(columns_query)
            columns = cursor.fetchall()

            if not columns:
                raise ValueError(
                    f"No columns found for table {source_catalog}.{source_schema}.{source_table}"
                )

            # Build SELECT clause with type conversions
            select_columns = []
            for column_name, data_type in columns:
                if data_type.lower() in TYPE_MAPPER.keys():
                    select_columns.append(
                        TYPE_MAPPER[data_type.lower()].format(column_name=column_name)
                    )
                elif (
                    "timestamp" in data_type.lower() and "timezone" in data_type.lower()
                ):
                    # Handle timezone-aware timestamps
                    select_columns.append(
                        f"CAST({column_name} AS TIMESTAMP) AS {column_name}"
                    )
                else:
                    # Keep column as-is for compatible types
                    select_columns.append(column_name)

            select_clause = ",\n    ".join(select_columns)

            # Build the CREATE TABLE AS query with type conversions
            create_table_query = f"""
            CREATE TABLE {self.catalog}.{self.schema}.{destination_table} AS
            SELECT 
                {select_clause}
            FROM {source_catalog}.{source_schema}.{source_table}
            """

            logger.debug(f"Executing query: {create_table_query}")
            cursor.execute(create_table_query)

        logger.info(
            f"Table {self.catalog}.{self.schema}.{destination_table} created successfully."
        )
