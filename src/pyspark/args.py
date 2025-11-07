import click
import functools

from driver import Driver

def jdbc_to_gbq_options(f: callable) -> callable:
    """
    Click args for the JDBC to GBQ PySpark template:
    https://github.com/GoogleCloudPlatform/dataproc-templates/tree/main/python/dataproc_templates/jdbc#arguments-2
    """
    @click.option(
        '--input-url-secret',
        required=True,
        help='Name of the Google secret whose value is a JDBC url that includes a password to connect to the input database',
    )
    @click.option(
        '--input-driver',
        required=True,
        type=click.Choice(Driver),
        help='Enum value for JDBC input driver name',
    )
    @click.option(
        '--input-table',
        required=True,
        help='JDBC input table name',
    )
    @click.option(
        '--input-partition-column',
        required=False,
        help='JDBC input table partition column name',
    )
    @click.option(
        '--input-lower-bound',
        required=False,
        help='JDBC input table partition column lower bound which is used to decide the partition stride',
    )
    @click.option(
        '--input-upper-bound',
        required=False,
        help='JDBC input table partition column upper bound which is used to decide the partition stride',
    )
    @click.option(
        '--input-fetch-size',
        required=False,
        help='Determines how many rows to fetch per round trip',
    )
    @click.option(
        '--input-session-init-statement',
        required=False,
        help='Custom SQL statement to execute in each reader database session',
    )
    @click.option(
        '--num-partitions',
        required=False,
        help='The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10',
    )
    @click.option(
        '--output-mode',
        required=False,
        help='Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)',
    )
    @functools.wraps(f)
    def wrapper(**kwargs):
        return f(**kwargs)
    return wrapper