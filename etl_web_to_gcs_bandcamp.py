# from distributed import Client
# client = Client()
# import pandas as pd

import modin.pandas as pd
import os
import json
from pathlib import Path
from urllib.request import urlretrieve
from tqdm import tqdm
from zipfile import ZipFile
from google.cloud import bigquery
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

pd.set_option("display.max_columns", None)
print("Setup Complete")


@task(log_prints=True, name="read_df")
# Seq 1-Define a function to convert the downloaded file to data frame
def read_df(file: str) -> pd.DataFrame:
    with open(file) as data_file:
        data = json.load(data_file)
        df = pd.read_json(data)
        df = pd.json_normalize(df, sep="_")
        return df


@task(log_prints=True, name="tweak_df")
# Seq 2-Define a function to tweak the data frame
def tweak_df(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Number of rows: {df.shape[0]}")
    df_ = df
    return df_


@task(log_prints=True, name="write_local")
# Seq 3-Define a function to set a path for GCS storage and for local file
def write_local(df: pd.DataFrame, filename: str) -> Path:
    directory = Path("bandcamp")
    _file_name = filename.split(".")[0]
    path_name = directory / f"{_file_name}.parquet"
    try:
        os.makedirs(directory)
        pd.to_parquet(path_name, compression="gzip", index=False)
    except OSError as error:
        print(error)
    return path_name


@task(log_prints=True, name="write_to_gcs")
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


@flow(log_prints=True, name="etl_web_to_gcs")
# Define ETL from web to gcs:
def etl_web_to_gcs(file: str):
    # Seq 1 -Read file
    df = read_df(file)
    # Seq 2 -Tweak df
    df_ = tweak_df(df)
    # Seq 3 -Set a path this will be use to convert file to parquet
    path_file = write_local(df, file)
    # Seq 4-Upload local file to GCS Bucket
    write_to_gcs(path_file)
    # Seq 5- Remove duplicate
    duduplicate(path_file)


# Define download progress hook
def download_progress_hook(block_num, block_size, total_size):
    global progress_bar
    if not progress_bar:
        progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)
    downloaded = block_num * block_size
    progress_bar.update(downloaded - progress_bar.n)
    if downloaded >= total_size:
        progress_bar = None


@task(log_prints=True, name="fetch_data")
# Seq 0 -Download file folder from web
def fetch_data(url: str):
    folder_name = url.split("/")[-1].split("?")[0]
    file_folder = urlretrieve(url, folder_name, reporthook=download_progress_hook)
    if folder_name.endswith(".zip"):
        unzipped_folder = ZipFile(folder_name).extractall()
        print(f"Download Complete..extracted zip file")
        return unzipped_folder
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
    progress_bar = None
    file_folder = fetch_data(dataset_url)
    # Loop through the files then run etl_web_to_gcs
    print("Running etl_web_to_gcs...this will take sometime..grab some coffee or tea")
    for f in os.listdir(file_folder):
        if f.endswith(".json"):
            print("Running: {f}")
            etl_web_to_gcs(f)
            print("Done uploading {f} to GCS")


if __name__ == "__main__":
    etl_parent_web_gcs()
