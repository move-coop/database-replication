import os
from typing import List, Union
from urllib.parse import quote  # Import the quote function for URL encoding

from common.logger import logger


def set_dlt_environment_variables(config: dict) -> None:
    """
    Loops through the key / value pairs defined in the
    environment and sets the appropriate env variables
    """

    for key, value in config.items():
        logger.debug(f"Setting {key}...")
        try:
            os.environ[key] = str(value)
        except Exception as e:
            logger.error(f"Error setting environment variable {key}: {e}")
            raise e


def get_jdbc_connection_string(config: dict) -> str:
    """
    Translates config values into a standard JDBC string
    """

    # Allow for fully-formed JDBC strings to take precedence here
    if os.environ.get("JDBC_CONNECTION_STRING_PASSWORD"):
        return os.environ.get("JDBC_CONNECTION_STRING_PASSWORD")

    resp = "{driver}://{user}:{password}@{host}:{port}/{database}".format(
        driver=config["SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME"],
        user=config["SOURCES__SQL_DATABASE__USERNAME"],
        password=quote(config["SOURCES__SQL_DATABASE__PASSWORD"]),
        host=config["SOURCES__SQL_DATABASE__HOST"],
        database=config["SOURCES__SQL_DATABASE__DATABASE"],
        port=config["SOURCES__SQL_DATABASE__PORT"],
    )
    if config["SOURCES__SQL_DATABASE__SSL"]:
        resp += "?sslmode=require"
        if (
            "SOURCES__SQL_DATABASE__SSL_CERT" in config
            and "SOURCES__SQL_DATABASE__SSL_KEY" in config
        ):
            resp += f"&sslcert={config['SOURCES__SQL_DATABASE__SSL_CERT']}&sslkey={config['SOURCES__SQL_DATABASE__SSL_KEY']}"
        else:
            logger.warning("SSL is enabled but no SSL certificate or key is provided.")
    return resp


def validate_write_dispostiion(write_disposition: str) -> None:
    """
    Validates the write disposition value.
    Raises ValueError if the value is not valid.
    """

    valid_dispositions = ["append", "replace", "merge", "drop"]

    if write_disposition not in valid_dispositions:
        raise ValueError(
            f"Invalid write disposition: {write_disposition}. "
            f"Valid options are: {', '.join(valid_dispositions)}."
        )

    if write_disposition == "drop":
        write_disposition = None
    # TODO - Someday maybe
    elif write_disposition == "merge":
        raise ValueError(
            "We're not supporting merge as a write disposition yet - all pseduo-incremental loads are handled in dbt"
        )
    return write_disposition


def validate_source_tables(source_table_string: str) -> Union[List[str], None]:
    """
    If the user supplies 'ALL' in the runtime environment
    we want to pass in `None` to the import function (this
    is how dlt expects to handle all tables in a source schema)

    Args:
        source_table_string:    This should be a comma-separated list of tables in the environment (or 'ALL')

    Returns:
        Array of table names, or `None` if all tables are to be targeted
    """

    target_tables = [table.strip() for table in source_table_string.split(",")]

    if len(target_tables) == 1 and target_tables[0].upper() == "ALL":
        return

    return target_tables
