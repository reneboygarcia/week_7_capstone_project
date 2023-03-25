# imports
import typing
from prefect import task, flow
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

print("Setup Complete")
# Deployment 2
@task(log_prints=True, name="get-gcp-creds")
# Define a function to get GCP Credentials
def get_bigquery_client():
    gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds") # Change this credentials to yours
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    return client


@flow(log_prints=True, name="deduplicate data")
# Define a function to remove duplicate
def deduplicate_data(num: int):
    client = get_bigquery_client()
    
    query_dedup = f"CREATE OR REPLACE TABLE \
                        `dtc-de-2023.bandcamp.albums-full-info-{num}`  AS ( \
                            SELECT DISTINCT * \
                            FROM `dtc-de-2023.ny_taxi.ny_taxi_tripdata_{year}` \
                            )"

    # limit query to 10GB
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
    # priority=bigquery.QueryPriority.BATCH
    # query
    query_job = client.query(query_dedup, job_config=safe_config)

    # Check progress
    query_job = typing.cast(
        "bigquery.QueryJob",
        client.get_job(
            query_job.job_id, location=query_job.location
        ),  # Make an API request.
    )
    print("Complete removing duplicates")
    print(f"Job {query_job.job_id} is currently in state {query_job.state}")


# Upload data from GCS to BigQuery
@flow(log_prints=True, name="etl-gcs-to-bq")
def etl_gcs_to_bq(year: int, month: int):

    client = get_bigquery_client()
    table_id = f"dtc-de-2023.ny_taxi.fhv_tripdata_{year}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
            bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("dropOff_datetime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("PUlocationID", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("DOlocationID", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(
                "SR_Flag",
                "FLOAT",
                mode="NULLABLE",
            ),
            bigquery.SchemaField("Affiliated_base_number", "STRING", mode="NULLABLE"),
        ],
    )
    uri = f"gs://ny_taxi_bucket_de_2023/2019/fhv_tripdata_{year}-{month:02}.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


# Parent flow ETL
@flow(log_prints=True, name="etl-parent-to-bq")
def etl_parent_bq_flow(
    year: int = 2019, months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for month in months:
        etl_gcs_to_bq(year, month)


# run main
if __name__ == "__main__":
    year = 2019
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    etl_parent_bq_flow(year, months)
