# pylint: disable=import-error

from pathlib import Path
import pandas as pd
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_aws.s3 import S3Bucket

# from prefect.filesystems import S3


# fetch task (Extract)
@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(url: str) -> pd.DataFrame:
    """Read data from web to pandas dataframe"""
    return pd.read_csv(url)


# clean task (Transform)
@task(log_prints=True, retries=3)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


# write local task (Load local)
@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


# load to bucket task (load remote)
@task(log_prints=True)
def write_aws(path: Path) -> None:
    """Upload local parquet file to AWS S3"""
    aws_block = S3Bucket.load("zoom-s3")
    aws_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="etl-to-aws-flow")
def etl_web_to_s3() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    #     dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url = "output.csv"

    # task 1
    df = fetch(dataset_url)

    # task 2
    clean_df = clean(df)

    # task 3
    path_local = write_local(clean_df, color, dataset_file)

    # task 4
    write_aws(path_local)


if __name__ == "__main__":
    etl_web_to_s3()
