"""
S8 Data Preparation Script
Downloads ADS-B tracking data, aircraft metadata, fuel consumption rates.
Enriches tracking data and stores in SQLite for the API.

Run: python -m bdi_api.s8.prepare_data
"""

import json
import os
import sqlite3
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "s8"
DB_PATH = DATA_DIR / "aircraft.db"
TRACKING_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01"
AIRCRAFT_CSV_URL = "https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

# Download a limited set of files (every 5 min = 60 files per 5 hours)
TIMESTAMPS = [f"{h:02d}{m:02d}{s:02d}Z" for h in range(0, 5) for m in range(0, 60, 5) for s in [0]]


def download_tracking_data():
    """Download ADS-B tracking JSON files."""
    raw_dir = DATA_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    all_aircraft = []
    downloaded = 0

    for ts in TIMESTAMPS:
        filepath = raw_dir / f"{ts}.json"
        if filepath.exists():
            print(f"  cached: {ts}")
            with open(filepath) as f:
                data = json.load(f)
            for ac in data.get("aircraft", []):
                ac["_timestamp"] = ts
                ac["_day"] = "2023-11-01"
            all_aircraft.extend(data.get("aircraft", []))
            downloaded += 1
            continue

        url = f"{TRACKING_URL}/{ts}.json.gz"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                with open(filepath, "w") as f:
                    json.dump(data, f)
                for ac in data.get("aircraft", []):
                    ac["_timestamp"] = ts
                    ac["_day"] = "2023-11-01"
                all_aircraft.extend(data.get("aircraft", []))
                downloaded += 1
                print(f"  downloaded: {ts} ({len(data.get('aircraft', []))} aircraft)")
            else:
                print(f"  skip: {ts} (HTTP {resp.status_code})")
        except Exception as e:
            print(f"  error: {ts} ({e})")

    print(f"Total files: {downloaded}, total observations: {len(all_aircraft)}")
    return all_aircraft


def download_aircraft_db():
    """Download OpenSky aircraft database CSV."""
    csv_path = DATA_DIR / "aircraftDatabase.csv"
    if csv_path.exists():
        print("  Aircraft DB cached")
        return pd.read_csv(csv_path, low_memory=False)

    print("  Downloading aircraft database (~90MB)...")
    resp = requests.get(AIRCRAFT_CSV_URL, timeout=120, allow_redirects=True)
    with open(csv_path, "wb") as f:
        f.write(resp.content)
    print(f"  Saved: {csv_path}")
    return pd.read_csv(csv_path, low_memory=False)


def download_fuel_rates():
    """Download fuel consumption rates JSON."""
    path = DATA_DIR / "fuel_rates.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)

    resp = requests.get(FUEL_RATES_URL, timeout=30)
    data = resp.json()
    with open(path, "w") as f:
        json.dump(data, f)
    return data


def build_database(all_aircraft, aircraft_db, fuel_rates):
    """Build SQLite database with enriched aircraft data and observations."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        os.remove(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))

    # Build observations table (for CO2 calculation)
    obs_df = pd.DataFrame([
        {
            "icao": ac.get("hex", "").strip().lower(),
            "type": ac.get("t", "").strip() if ac.get("t") else None,
            "day": ac.get("_day"),
            "timestamp": ac.get("_timestamp"),
        }
        for ac in all_aircraft
        if ac.get("hex")
    ])
    obs_df.to_sql("observations", conn, if_exists="replace", index=False)
    print(f"  Observations: {len(obs_df)} rows")

    # Build unique aircraft from tracking data
    tracking_df = pd.DataFrame([
        {
            "icao": ac.get("hex", "").strip().lower(),
            "registration": ac.get("r", "").strip() if ac.get("r") else None,
            "type": ac.get("t", "").strip() if ac.get("t") else None,
        }
        for ac in all_aircraft
        if ac.get("hex")
    ]).drop_duplicates(subset=["icao"]).reset_index(drop=True)

    # Clean aircraft DB
    aircraft_db = aircraft_db.rename(columns={
        "icao24": "icao_db",
        "manufacturername": "manufacturer",
        "typecode": "typecode_db",
    })
    aircraft_db["icao_db"] = aircraft_db["icao_db"].str.strip().str.lower()

    # Merge tracking with aircraft DB
    merged = tracking_df.merge(
        aircraft_db[["icao_db", "manufacturer", "model", "owner"]].drop_duplicates(subset=["icao_db"]),
        left_on="icao",
        right_on="icao_db",
        how="left",
    )
    merged = merged.drop(columns=["icao_db"], errors="ignore")

    # Clean up None/NaN
    for col in ["registration", "type", "owner", "manufacturer", "model"]:
        if col in merged.columns:
            merged[col] = merged[col].where(merged[col].notna(), None)

    merged = merged.sort_values("icao").reset_index(drop=True)
    merged.to_sql("aircraft", conn, if_exists="replace", index=False)
    print(f"  Aircraft (enriched): {len(merged)} rows")

    # Save fuel rates
    fuel_df = pd.DataFrame([
        {"type_code": k, "galph": v.get("galph", 0), "name": v.get("name", "")}
        for k, v in fuel_rates.items()
    ])
    fuel_df.to_sql("fuel_rates", conn, if_exists="replace", index=False)
    print(f"  Fuel rates: {len(fuel_df)} rows")

    conn.close()
    print(f"  Database: {DB_PATH}")


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Step 1: Download tracking data...")
    all_aircraft = download_tracking_data()

    print("Step 2: Download aircraft database...")
    aircraft_db = download_aircraft_db()

    print("Step 3: Download fuel rates...")
    fuel_rates = download_fuel_rates()

    print("Step 4: Build database...")
    build_database(all_aircraft, aircraft_db, fuel_rates)

    print("Done!")


if __name__ == "__main__":
    main()
