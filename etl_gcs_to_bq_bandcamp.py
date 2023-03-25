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
    gcp_creds_block = GcpCredentials.load(
        "prefect-gcs-2023-creds"
    )  # Change this credentials to yours
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
                            FROM `dtc-de-2023.bandcamp.albums-full-info-{num}` \
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
def etl_gcs_to_bq(file_num: int):
    client = get_bigquery_client()
    table_id = f"dtc-de-2023.bandcamp.albums-full-info-{file_num}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
            bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("numTracks", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("keywords", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("datePublished", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("dateModified", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("comment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("inAlbum", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("offers", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("duration_secs", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("duration", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recordingOf", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("isrcCode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_image", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_genre", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_@id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_sameAs", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist_description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("track_itemListElement", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("track_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("track_numberOfItems", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("track", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("inAlbum_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("inAlbum_@id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("inAlbum_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("offers_availability", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "offers_priceSpecification_minPrice", "FLOAT", mode="NULLABLE"
            ),
            bigquery.SchemaField("offers_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("offers_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("offers_priceCurrency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("offers_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recordingOf_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recordingOf_lyrics_text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recordingOf_lyrics_@type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("byArtist", "FLOAT", mode="NULLABLE"),
        ],
    )
    uri = f"gs://prefect-gcs-bucket-bandcamp/albums-full-info-{file_num}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


# Parent flow ETL
@flow(log_prints=True, name="etl-parent-to-bq")
def etl_parent_bq_flow(file_num_list: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]):
    for num in file_num_list:
        etl_gcs_to_bq(file_num)


# run main
if __name__ == "__main__":
    file_num_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    etl_parent_bq_flow(file_num_list)
