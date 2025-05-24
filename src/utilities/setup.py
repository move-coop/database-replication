import os

from utilities.logger import logger


def set_dlt_environment_variables(config: dict) -> None:
    """
    Loops through the key / value pairs defined in the
    environment and sets the appropriate env variables
    """

    for key, value in config.items():
        logger.debug(f"Setting {key}...")
        os.environ[key] = value


def get_jdbc_connection_string(config: dict) -> str:
    """
    Translates config values into a standard JDBC string
    """

    resp = "{driver}://{user}:{password}@{host}:{port}/{database}".format(
        driver=config["SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME"],
        user=config["SOURCES__SQL_DATABASE__USERNAME"],
        password=config["SOURCES__SQL_DATABASE__PASSWORD"],
        host=config["SOURCES__SQL_DATABASE__HOST"],
        database=config["SOURCES__SQL_DATABASE__DATABASE"],
        port=config["SOURCES__SQL_DATABASE__PORT"],
    )

    return resp
