import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import requests

def download_parquet(url, output_file):
    """Download a Parquet file from the given URL."""
    print(f"Downloading Parquet file from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        with open(output_file, 'wb') as file:
            file.write(response.content)
        print(f"Parquet file downloaded successfully as {output_file}.")
    except Exception as e:
        print(f"Failed to download the Parquet file: {e}")
        exit(1)

def parquet_to_csv(parquet_file, csv_file):
    """Convert a Parquet file to a CSV file."""
    print(f"Converting Parquet file to CSV...")
    try:
        df = pd.read_parquet(parquet_file)
        df.to_csv(csv_file, index=False)
        print(f"CSV file saved successfully as {csv_file}.")
    except Exception as e:
        print(f"Failed to convert Parquet to CSV: {e}")
        exit(1)

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url

    # File names
    parquet_name = 'input.parquet'  # Temporary Parquet file
    csv_name = 'output.csv'  # Output CSV file

    # Step 1: Download the Parquet file
    download_parquet(url, parquet_name)

    # Step 2: Convert Parquet to CSV
    parquet_to_csv(parquet_name, csv_name)

    # Step 3: Check if the CSV file exists
    if not os.path.exists(csv_name):
        print(f"CSV file '{csv_name}' not found.")
        exit(1)

    # Step 4: Create a connection to the PostgreSQL database
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        engine.connect()  # Test the connection
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        exit(1)

    # Step 5: Read the CSV file in chunks
    try:
        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    except Exception as e:
        print(f"Failed to read the CSV file: {e}")
        exit(1)

    # Step 6: Get the first chunk
    df = next(df_iter)

    # Step 7: Check for required columns
    required_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    if not all(column in df.columns for column in required_columns):
        print(f"CSV file is missing required columns: {required_columns}")
        exit(1)

    # Step 8: Convert datetime columns
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Step 9: Create the table (replace if it exists)
    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

    # Step 10: Insert the first chunk
    df.to_sql(name=table, con=engine, if_exists='append')

    # Step 11: Insert the remaining chunks
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table, con=engine, if_exists='append')
            t_end = time()
            print('inserted another chunk, took %.3f seconds' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the database.")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres via CSV conversion')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table', help='table name of the where the result is stored')
    parser.add_argument('--url', help='url for the parquet file')

    args = parser.parse_args()

    main(args)