from typing import Optional
import click
import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
from args import jdbc_to_gbq_options

# TODO: Make this configurable in CI
GCP_PROJECT = "tmc-data-transfer"
DATAPROC_CLUSTER_NAME = "data-transfer-cluster"
DATAPROC_CLUSTER_REGION = "us-central1"
GCS_PARENT_FOLDER = "gs://dataproc-staging-us-central1-386874222317-aaiovycl/pyspark"

@click.command()
@jdbc_to_gbq_options
def main(**kwargs):
    """
    Submits a PySpark job to Dataproc to replicate a JDBC-connected database to GBQ.

    Docs & Code Referenced:
        - https://docs.cloud.google.com/dataproc/docs/samples/dataproc-submit-job
        - https://github.com/googleapis/google-cloud-python/blob/2feb74032fd9c5cc7eaf6072ab03e9e8397bd434/packages/google-cloud-dataproc/google/cloud/dataproc_v1/types/jobs.py#L305
    """
    # TODO: Make this configurable in CI
    job_client = dataproc.JobControllerClient(
        client_options={
            "api_endpoint": f"{DATAPROC_CLUSTER_REGION}-dataproc.googleapis.com:443",
        }
    )

    args = []
    for key, value in kwargs.items():
        if value is not None:
            args.append(f"--{key.replace('_', '-')}")
            args.append(str(value))
    job_config = {
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME,
        },
        "pyspark_job": {
            "main_python_file_uri": f"{GCS_PARENT_FOLDER}/job.py",
            "python_file_uris": [
                f"{GCS_PARENT_FOLDER}/args.py",
                f"{GCS_PARENT_FOLDER}/driver.py"
            ],
            "args": args,
        },
    }

    operation = job_client.submit_job_as_operation(
        request={
            "project_id": GCP_PROJECT,
            "region": DATAPROC_CLUSTER_REGION,
            "job": job_config
        }
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )
    print(f"Job finished successfully: {output}\r\n")

if __name__ == '__main__':
    main()