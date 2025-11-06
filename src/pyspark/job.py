import click
from pyspark.sql import SparkSession

# Source DB Name => PySpark JDBC Driver
SOURCE_DB_DRIVERS = {
    "postgres": "org.postgresql.Driver",
}

@click.command()
@click.option('--src-db-type', required=True, help='Type of source database (e.g. mysql, postgres)')
@click.option('--src-protocol-action', default=None, help='Action specified in JDBC protocol for source database, if any')
@click.option('--src-host', required=True, help='Host of source database')
@click.option('--src-port', required=True, help='Port for source database, if any')
@click.option('--src-db-name', required=True, help='Database name of source database')
@click.option('--src-params', default=None, help='Connection parameters for source database, if any')
@click.option('--src-user', required=True, help='User to connect to source database with')
def main(
    src_db_type: str,
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
    src_jdbc_action = f":{src_protocol_action}" if src_protocol_action else ""
    src_jdbc_params = f"?{src_params}" if src_params else ""
    src_jdbc_url = f"jdbc:{src_db_type}{src_jdbc_action}://{src_host}:{src_port}/{src_db_name}{src_jdbc_params}"

    if src_db_type not in SOURCE_DB_DRIVERS.keys():
        raise Exception(f"Failed to find driver for unrecognized source database type: {src_db_type}")
    src_driver = SOURCE_DB_DRIVERS[src_db_type]
    src_properties = {
        "user": src_user,
        #"password": src_password,
        "driver": src_driver
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