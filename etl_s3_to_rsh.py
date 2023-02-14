# load from Data Lake (s3 bucket) to Data Warehouse (redshift)

from pathlib import Path
import pandas as pd
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_aws.s3 import S3Bucket
from prefect_aws import AwsCredentials
import redshift_connector
import sqlalchemy

# aws_credentials_block = AwsCredentials.load("aws-creds")

# task 1 (Extract from Data Lake)
@task(log_prints=True, retries=3)
def extract_from_s3(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    s3_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    local_path = f"data_from _lake/{color}_tripdata_{year}-{month:02}.parquet"
    aws_s3_block = S3Bucket.load("zoom-s3")
    aws_s3_block.download_object_to_path(s3_path, local_path)
    return Path(local_path)
    # return Path(f"../data_from _lake/{s3_path}")


# task 2 (Tranform Data to load to Warehouse)
@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning Handle null values for numerical data"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    df.astype({"VendorID": "float"}).dtypes
    return df


# load cleaned data from local to Data Warehouse (Redshiift)
@task(log_prints=True, retries=3)
def write_rshift(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    aws_credentials_block = AwsCredentials.load("aws-creds")

    credd = aws_credentials_block.aws_secret_access_key

    df = df.head(1000)

    conn = redshift_connector.connect(
        host="zoom-cluster.cvq52eyskaej.us-east-1.redshift.amazonaws.com",
        port=5439,
        database="dev",
        user="awsuser",
        password="Branham357.",
    )
    conn.autocommit = True
    print("connection --> ", bool(conn))

    # engine = sqlalchemy.create_engine(
    #     "postgresql://awsuser:Branham357.@zoom-cluster.cvq52eyskaej.us-east-1.redshift.amazonaws.com:5439/dev"
    # )
    # print("connection created --> ", bool(engine))

    # with engine.connect() as conn:
    #     print("connection created --> ", bool(conn))
    #     df.to_sql(
    #         "yellow_trips", conn, if_exists="append", index=False, chunksize=200000
    #     )

    # sql_table_schema = pd.io.sql.get_schema(df.reset_index(), "yellow_trips")
    # print(sql_table_schema)

    qry_txt = """
            CREATE TABLE IF NOT EXISTS yellow_trips (
            index BIGINT, 
            "VendorID" FLOAT(53), 
            tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
            tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count FLOAT(53), 
            trip_distance FLOAT(53), 
            "RatecodeID" FLOAT(53), 
            store_and_fwd_flag TEXT, 
            "PULocationID" BIGINT, 
            "DOLocationID" BIGINT, 
            payment_type FLOAT(53), 
            fare_amount FLOAT(53), 
            extra FLOAT(53), 
            mta_tax FLOAT(53), 
            tip_amount FLOAT(53), 
            tolls_amount FLOAT(53), 
            improvement_surcharge FLOAT(53), 
            total_amount FLOAT(53), 
            congestion_surcharge FLOAT(53)
        )
    """

    with conn.cursor() as cursor:
        cursor.execute(qry_txt)

        cursor.write_dataframe(df, "yellow_trips")
    return


@flow(name="ETL S3 to Redshift")
def etl_s3_to_rshift():
    """Main ETL flow to load data to Redshift"""
    color = "yellow"
    year = 2021
    month = 1

    # first task
    path = extract_from_s3(color, year, month)

    # # second task
    clean_df = transform(path)

    # # third task
    write_rshift(clean_df)


if __name__ == "__main__":
    etl_s3_to_rshift()
