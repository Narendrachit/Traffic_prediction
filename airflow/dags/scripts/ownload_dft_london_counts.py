# download_dft_london_counts.py
#
# Downloads DfT raw traffic counts and filters them to London.
# Output: data/dft_london_traffic_counts.csv

import os
import io
import zipfile
import requests
import pandas as pd

RAW_COUNTS_URL = (
    "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/"
    "dft_traffic_counts_raw_counts.zip"
)

os.makedirs("data", exist_ok=True)


def download_and_extract_raw_counts():
    print("Downloading DfT raw counts zip...")
    resp = requests.get(RAW_COUNTS_URL, stream=True)
    resp.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    # Usually there's a single CSV inside the zip
    csv_name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
    print(f"Found CSV in zip: {csv_name}")

    with z.open(csv_name) as f:
        print("Reading CSV into pandas (this may take a minute)...")
        df = pd.read_csv(f)

    print("Columns in raw counts file:")
    print(df.columns.tolist())

    return df


def filter_to_london(df: pd.DataFrame) -> pd.DataFrame:
    """
    Works with the headers you showed:
    count_point_id, direction_of_travel, year, count_date, hour,
    region_id, region_name, region_ons_code,
    local_authority_id, local_authority_name, local_authority_code,
    road_name, ..., all_HGVs, all_motor_vehicles
    """

    # ---- 1) Filter to London region ----
    # COLUMN NAMES ARE LOWERCASE in your CSV: "region_name", "year", etc.
    df_london = df[df["region_name"] == "London"].copy()

    # Optional: keep only recent years to reduce size
    df_london = df_london[df_london["year"] >= 2019]

    # ---- 2) Rename a couple of columns for the Spark pipeline ----
    df_london = df_london.rename(
        columns={
            "local_authority_name": "local_authority",
            "all_HGVs": "all_hgvs",  # just a nicer name
        }
    )

    # ---- 3) Keep only useful columns ----
    keep_cols = [
        "year",
        "count_date",
        "hour",
        "region_name",
        "local_authority",
        "road_name",
        "latitude",
        "longitude",
        "all_hgvs",
        "all_motor_vehicles",
    ]
    # Only keep those that actually exist (defensive)
    keep_cols = [c for c in keep_cols if c in df_london.columns]
    df_london = df_london[keep_cols]

    # ---- 4) Lowercase column names for Spark consistency ----
    df_london.columns = [c.lower() for c in df_london.columns]

    return df_london


def main():
    df_raw = download_and_extract_raw_counts()
    df_london = filter_to_london(df_raw)

    if df_london.empty:
        print("WARNING: filtered London dataframe is empty!")
    else:
        print(f"Filtered London rows: {len(df_london)}")

    out_path = "data/dft_london_traffic_counts.csv"
    print(f"Saving filtered London data to {out_path}")
    df_london.to_csv(out_path, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
