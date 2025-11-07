from typing import Optional
import click
from pyspark.sql import SparkSession
from utilities.args import jdbc_to_gbq_options

@click.command()
@jdbc_to_gbq_options
def main(
    input_url_secret: str,
    input_driver: str,
    input_table: Optional[str] = None,
    input_partition_column: Optional[str] = None,
    input_lower_bound: Optional[str] = None,
    input_upper_bound: Optional[str] = None,
    input_fetch_size: Optional[str] = None,
    input_session_init_statement: Optional[str] = None,
    num_partitions: Optional[int] = None,
    output_mode: Optional[str] = None,
):
    """
    PySpark job that replicates a JDBC-connected database to GBQ.

    Simple wrapper around this template:
    https://github.com/GoogleCloudPlatform/dataproc-templates/tree/main/python/dataproc_templates/jdbc#arguments-2
    """
    spark = SparkSession.builder \
        .appName("JdbcToGbq") \
        .getOrCreate()

    data = [
        input_url_secret,
        input_driver,
        input_table,
        input_partition_column,
        input_lower_bound,
        input_upper_bound,
        input_fetch_size,
        input_session_init_statement,
        num_partitions,
        output_mode
    ]
    print(data)
    rdd = spark.sparkContext.parallelize(data)
    print(f"Number of elements in RDD: {rdd.count()}")

    spark.stop()

if __name__ == '__main__':
    main()