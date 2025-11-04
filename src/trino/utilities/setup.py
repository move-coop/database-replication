import os

TRINO_CONNECTION_CREDENTIALS = {
    "host": os.getenv("TRINO_HOST"),
    "port": int(os.getenv("TRINO_PORT", "8086")),
    "username": os.getenv("TRINO_USERNAME"),
    "password": os.getenv("TRINO_PASSWORD"),
    "catalog": os.getenv("TRINO_CATALOG"),
    "schema": os.getenv("TRINO_SCHEMA"),
}

SOURCE_DATABASE_CREDENTIALS = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", "5432")),
    "username": os.getenv("SOURCE_DB_USERNAME", "source_user"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "source_password"),
    "database": os.getenv("SOURCE_DB_NAME", "source_database"),
    "jdbc_driver": os.getenv("SOURCE_DB_JDBC_DRIVER", "postgresql"),
    "jdbc_connection_string": os.getenv("SOURCE_DB_JDBC_CONNECTION_STRING", None),
}
