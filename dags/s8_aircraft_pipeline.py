import json
import os
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
TRACKING_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01"
AIRCRAFT_CSV_URL = "https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

TIMESTAMPS = [f"{h:02d}{m:02d}00Z" for h in range(0, 2) for m in range(0, 60, 5)]

with DAG(
    dag_id="s8_aircraft_pipeline",
    start_date=datetime(2023, 11, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["s8", "aircraft"],
) as dag:

    @task()
    def fetch_tracking_to_bronze(ds=None):
        """Download ADS-B tracking files and upload to S3 bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        uploaded = 0
        for ts in TIMESTAMPS:
            url = f"{TRACKING_URL}/{ts}.json.gz"
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    s3_key = f"aircraft/tracking/_date=2023-11-01/{ts}.json"
                    s3.put_object(Bucket=BRONZE_BUCKET, Key=s3_key, Body=resp.content)
                    uploaded += 1
            except Exception as e:
                print(f"Error {ts}: {e}")
        return f"Uploaded {uploaded} files"

    @task()
    def fetch_aircraft_db_to_bronze(ds=None):
        """Download aircraft metadata CSV to S3 bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        resp = requests.get(AIRCRAFT_CSV_URL, timeout=120, allow_redirects=True)
        s3_key = "aircraft/metadata/aircraftDatabase.csv"
        s3.put_object(Bucket=BRONZE_BUCKET, Key=s3_key, Body=resp.content)
        return s3_key

    @task()
    def fetch_fuel_rates_to_bronze(ds=None):
        """Download fuel consumption rates to S3 bronze."""
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        resp = requests.get(FUEL_RATES_URL, timeout=30)
        s3_key = "aircraft/fuel_rates/rates.json"
        s3.put_object(Bucket=BRONZE_BUCKET, Key=s3_key, Body=resp.content)
        return s3_key

    @task()
    def bronze_to_silver(tracking_result, metadata_key, fuel_key, ds=None):
        """Parse, enrich, and write Parquet to silver layer."""
        import pandas as pd
        import s3fs

        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        fs = s3fs.S3FileSystem(endpoint_url=S3_ENDPOINT, key="minioadmin", secret="minioadmin")

        # Read all tracking files
        all_aircraft = []
        resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="aircraft/tracking/_date=2023-11-01/")
        for obj in resp.get("Contents", []):
            data = json.loads(s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read())
            for ac in data.get("aircraft", []):
                all_aircraft.append({
                    "icao": ac.get("hex", "").strip().lower(),
                    "registration": ac.get("r", "").strip() if ac.get("r") else None,
                    "type": ac.get("t", "").strip() if ac.get("t") else None,
                })

        tracking_df = pd.DataFrame(all_aircraft).drop_duplicates(subset=["icao"])

        # Read aircraft DB
        csv_obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=metadata_key)
        aircraft_db = pd.read_csv(csv_obj["Body"], low_memory=False)
        aircraft_db = aircraft_db.rename(columns={"icao24": "icao_db", "manufacturername": "manufacturer"})
        aircraft_db["icao_db"] = aircraft_db["icao_db"].str.strip().str.lower()

        # Merge
        merged = tracking_df.merge(
            aircraft_db[["icao_db", "manufacturer", "model", "owner"]].drop_duplicates(subset=["icao_db"]),
            left_on="icao", right_on="icao_db", how="left",
        ).drop(columns=["icao_db"], errors="ignore")

        # Write to silver
        silver_key = f"{SILVER_BUCKET}/aircraft/enriched/_date=2023-11-01/data.snappy.parquet"
        with fs.open(silver_key, "wb") as f:
            merged.to_parquet(f, compression="snappy", index=False)

        return f"s3://{silver_key} ({len(merged)} aircraft)"

    # Pipeline
    t1 = fetch_tracking_to_bronze()
    t2 = fetch_aircraft_db_to_bronze()
    t3 = fetch_fuel_rates_to_bronze()
    bronze_to_silver(t1, t2, t3)
