#!/usr/bin/env python
# coding: utf-8

# pylint: disable=import-error

# import os
# from time import time
from datetime import timedelta
import pandas as pd

# from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


# task 1
@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url):
    # csv_name = 'output.csv.gz'

    # os.system(f"wget {url} -O {csv_name}")
    # os.system(f"gunzip {csv_name}")
    csv_name = "output.csv"

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df


# task 2
@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

        df.to_sql(name=table_name, con=engine, if_exists="append")


#  flow
@flow(name="ingest flow")
def main():
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    # first task
    df = extract_data(csv_url)

    # second task
    ingest_data(table_name, df)


if __name__ == "__main__":
    main()
