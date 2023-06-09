# from distributed import Client
# client = Client()
import pandas as pd

# import modin.pandas as pd
import os
import json
from pathlib import Path
from urllib.request import urlretrieve
from zipfile import ZipFile
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket


pd.set_option("display.max_columns", None)
print("Setup Complete")


@task(log_prints=True, name="read_df")
# Seq 1-Define a function to convert the downloaded file to data frame
def read_df(file: str) -> pd.DataFrame:
    with open(file) as data_file:
        data = json.load(data_file)
        df = pd.read_json(data)
        df = pd.json_normalize(df.to_dict("records"), sep="_")
        return df


@task(log_prints=True, name="tweak_df")
# Seq 2-Define a function to tweak the data frame
def tweak_df(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Number of rows: {df.shape[0]}")
    df["datePublished"] = pd.to_datetime(df["datePublished"], errors="coerce")
    df["dateModified"] = pd.to_datetime(df["dateModified"], errors="coerce")
    df_ = df.drop(
        columns=["albumRelease", "@type", "image", "@id", "@context", "@graph"],
        errors="ignore",
    )
    return df_


@task(log_prints=True, name="write_local")
# Seq 3-Define a function to set a path for GCS storage and for local file
def write_local(df: pd.DataFrame, filename: str) -> Path:
    directory = Path("bandcamp")
    _file_name = filename.split("/")[-1].split(".")[0]
    path_name = directory / f"{_file_name}.parquet"
    try:
        os.makedirs(directory, exist_ok=True)
        # directory.mkdir()
        print("Converting json file to parquet....")
        df.to_parquet(path_name, compression="snappy")
        print("Done converting to parquet....")
    except OSError as error:
        print(error)
    return path_name


@task(log_prints=True, name="write_to_gcs", retries=3, retry_delay_seconds=30)
# Seq 4-Define a function to upload local file to GCS Bucket
def write_to_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("prefect-gcs-block-bandcamp")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    print("Hooray, we uploaded a huge file in GCS")
    return


@task(log_prints=True, name="Remove duplicate")
# Seq 5-Delete local file and its directory
def duduplicate(path: Path) -> None:
    try:
        path.unlink()
        full_path = path.resolve()
        full_path.parent.rmdir()
        print("Successfully deleted directory and its files")
    except OSError as error:
        print(f"Unable to find directory: {error}")


@flow(log_prints=True, name="etl_web_to_gcs", retries=1)
# Define ETL from web to gcs:
def etl_web_to_gcs(file: str):
    # Seq 1 -Read file
    df = read_df(file)
    # Seq 2 -Tweak df
    df_ = tweak_df(df)
    # Seq 3 -Set a path this will be use to convert file to parquet
    path_file = write_local(df_, file)
    # Seq 4-Upload local file to GCS Bucket
    write_to_gcs(path_file)


@task(log_prints=True, name="fetch_data", retries=3)
# cache_key_fn=task_input_hash,
# cache_expiration=timedelta(days=1)
# The cache will allow us to run again the pipeline without downloading
# all the files
# Seq 0 -Download file folder from web
def fetch_data(url: str):
    folder_name = url.split("/")[-1].split("?")[0]
    file_folder = urlretrieve(url, folder_name)
    if folder_name.endswith(".zip"):
        zip_file = ZipFile(folder_name)
        folder_name_ = os.path.commonprefix(zip_file.namelist()).strip("/")
        zip_file.extractall()
        print(f"Download Complete..extracted zip file")
        print(f"Extracted folder path: {folder_name_}")
        return folder_name_
    print(f"Download Complete..")
    return file_folder


@flow(log_prints=True, name="el_parent_web_gcs")
# Define a parent ETL to download the files
def etl_parent_web_gcs():
    # Parameters
    dataset_url = (
        "https://www.dropbox.com/s/a1kl5e35j4o53mz/bandcamp-items-json.zip?dl=1"
    )

    # Execution
    # Seq 0 -Download file folder from web
    file_folder = fetch_data(dataset_url)
    # Loop through the files then run etl_web_to_gcs
    print("Running etl_web_to_gcs...this will take sometime..grab some coffee or tea")
    for file in os.listdir(file_folder):
        if file.endswith(".json"):
            file_path = os.path.join(file_folder, file)
            print(f"Running: {file}")
            etl_web_to_gcs(file_path)
            print(f"Done uploading {file} to GCS")

    # Seq 5- Remove duplicate
    duduplicate(file_path)
    print("All files are Uploaded")


# Run Main
if __name__ == "__main__":
    etl_parent_web_gcs()
