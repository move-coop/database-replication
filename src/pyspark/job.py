import click
from pyspark.sql import SparkSession
from google.cloud import secretmanager
from dataclasses import dataclass
from enum import Enum

class SrcDbType(Enum):
    POSTGRESQL = "postgresql"

@dataclass
class SrcConfig:
    driver: str
    secret_id: str

SOURCE_DB_DRIVERS: dict[SrcDbType, SrcConfig] = {
    "postgres": SrcConfig(
        driver="org.postgresql.Driver",
        secret_id="wtr-read-replica",
    ),
}

@click.command()
@click.option('--src-db-type', required=True, type=click.Choice(SrcDbType), help='Type of source database (e.g. mysql, postgres)')
@click.option('--src-protocol-action', default=None, help='Action specified in JDBC protocol for source database, if any')
@click.option('--src-host', required=True, help='Host of source database')
@click.option('--src-port', required=True, help='Port for source database, if any')
@click.option('--src-db-name', required=True, help='Database name of source database')
@click.option('--src-params', default=None, help='Connection parameters for source database, if any')
@click.option('--src-user', required=True, help='User to connect to source database with')
def main(
    src_db_type: SrcDbType,
    src_protocol_action: str,
    src_host: str,
    src_port: int,
    src_db_name: str,
    src_params: str,
    src_user: str,
):
    """
    PySpark job that tests DB replication to GBQ via JDBC
    """
    client = secretmanager.SecretManagerServiceClient()
    def get_secret(secret_id: str) -> str:
        name = client.secret_path("485513486873", secret_id)
        response = client.get_secret(request={"name": name})
        return response.payload.data.decode("UTF-8")

    src_jdbc_action = f":{src_protocol_action}" if src_protocol_action else ""
    src_jdbc_params = f"?{src_params}" if src_params else ""
    src_jdbc_url = f"jdbc:{src_db_type}{src_jdbc_action}://{src_host}:{src_port}/{src_db_name}{src_jdbc_params}"

    src_config = SOURCE_DB_DRIVERS[src_db_type]
    src_properties = {
        "user": src_user,
        "password": get_secret(src_config.secret_id),
        "driver": src_config.driver,
    }

    spark = SparkSession.builder \
        .appName("DbReplicationTest") \
        .getOrCreate()

    #query = "(SELECT * FROM public.message LIMIT 10) AS subquery"
    #df = spark.read.jdbc(url=src_jdbc_url, table=query, properties=src_properties)
    #df.show()

    print(f"{src_user} @ {src_jdbc_url}")

    spark.stop()

if __name__ == '__main__':
    main()