import json
import os

from dotenv import load_dotenv

# TODO use more standard .env name
load_dotenv(dotenv_path="test.env")

##########

# Civis artifacts - we need to read this into memory and save the
# env variable as a filepath
if os.environ.get("SOURCE_SSL_CERT_PASSWORD"):
    with open("/app/src/certificate.crt", "w") as _fp:
        _fp.write(os.environ.get("SOURCE_SSL_CERT_PASSWORD"))
        os.environ["SOURCE_SSL_CERT"] = "/app/src/certificate.crt"
    os.chmod("/app/src/certificate.crt", 0o600)

if os.environ.get("SOURCE_SSL_KEY_PASSWORD"):
    with open("/app/src/keyfile.key", "w") as _fp:
        _fp.write(os.environ.get("SOURCE_SSL_KEY_PASSWORD"))
        os.environ["SOURCE_SSL_KEY"] = "/app/src/keyfile.key"
    os.chmod("/app/src/keyfile.key", 0o600)


# NOTE - This is written with Civis compatability in mind
# Their custom credential type has a trailing _PASSWORD suffix
if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_PASSWORD"):
    with open("/app/src/app-creds.json", "w") as _fp:
        _fp.write(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_PASSWORD"))
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/src/app-creds.json"
    os.chmod("/app/src/app-creds.json", 0o600)

if os.environ.get("DESTINATION_USE_JSON") == "true":
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        raise OSError("You must set the path to a JSON Service Account Key")

# Load the JSON file and set the environment variables
with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) as _fp:
    creds = json.load(_fp)

    os.environ["DESTINATION_PROJECT_ID"] = creds["project_id"]
    os.environ["DESTINATION_PRIVATE_KEY"] = creds["private_key"]
    os.environ["DESTINATION_CLIENT_EMAIL"] = creds["client_email"]


BIGQUERY_DESTINATION_CONFIG = {
    "DESTINATION__BIGQUERY__LOCATION": os.environ.get("DESTINATION_LOCATION", "us"),
    "DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID": os.environ.get("DESTINATION_PROJECT_ID"),
    "DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY": os.environ.get("DESTINATION_PRIVATE_KEY"),
    "DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL": os.environ.get("DESTINATION_CLIENT_EMAIL"),
}


SQL_SOURCE_CONFIG = {
    "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": os.environ.get("SOURCE_DRIVERNAME"),
    "SOURCES__SQL_DATABASE__DATABASE": os.environ.get("SOURCE_DATABASE"),
    "SOURCES__SQL_DATABASE__HOST": os.environ.get("SOURCE_HOST"),
    "SOURCES__SQL_DATABASE__PORT": os.environ.get("SOURCE_PORT"),
    "SOURCES__SQL_DATABASE__USERNAME": os.environ.get("SOURCE_USERNAME"),
    "SOURCES__SQL_DATABASE__PASSWORD": os.environ.get("SOURCE_PASSWORD"),
    "SOURCES__SQL_DATABASE__SSL": os.environ.get("SOURCE_SSL", "false").lower() == "true",
    "SOURCES__SQL_DATABASE__SSL_CERT": os.environ.get("SOURCE_SSL_CERT"),
    "SOURCES__SQL_DATABASE__SSL_KEY": os.environ.get("SOURCE_SSL_KEY"),
}
