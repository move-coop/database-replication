import os
from urllib.parse import quote  # Import the quote function for URL encoding

from utilities.logger import logger


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
        jdbc_string = os.environ.get("JDBC_CONNECTION_STRING_PASSWORD")

    else:
        jdbc_string = "{driver}://{user}:{password}@{host}:{port}/{database}".format(
            driver=config["SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME"],
            user=config["SOURCES__SQL_DATABASE__USERNAME"],
            password=quote(config["SOURCES__SQL_DATABASE__PASSWORD"]),
            host=config["SOURCES__SQL_DATABASE__HOST"],
            database=config["SOURCES__SQL_DATABASE__DATABASE"],
            port=config["SOURCES__SQL_DATABASE__PORT"],
        )

    if config["SOURCES__SQL_DATABASE__SSL"]:
        jdbc_string += "?sslmode=require"

        if (
            "SOURCES__SQL_DATABASE__SSL_CERT" in config
            and "SOURCES__SQL_DATABASE__SSL_KEY" in config
        ):
            jdbc_string += f"&sslcert={config['SOURCES__SQL_DATABASE__SSL_CERT']}&sslkey={config['SOURCES__SQL_DATABASE__SSL_KEY']}"
        else:
            logger.warning("SSL is enabled but no SSL certificate or key is provided.")

    return jdbc_string
