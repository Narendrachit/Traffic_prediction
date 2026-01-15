# fetch_tfl_disruptions.py
#
# Pulls TfL Road/All/Disruption data and writes:
#   data/tfl_disruptions_london.csv
#
# By default it fetches *current* disruptions (no date range),
# which avoids the 400 Bad Request you saw.

import os
import requests
import pandas as pd
from scripts._spark_win import get_spark, _as_file_uri


# ========= CONFIG =========

# Option A (better): set env var TFL_APP_KEY before running
#   PowerShell:  $env:TFL_APP_KEY="YOUR_PRIMARY_KEY"
# Option B (quick): paste your key directly in the default string.
APP_KEY =  "662d0e8aa3b546538f096a6dd5523cdd"

# If you want a small historical window, set these, e.g. "2024-01-01"
# and set USE_DATE_RANGE = True. For now we keep it False to avoid 400s.
USE_DATE_RANGE = False
START_DATE = "2024-01-01"
END_DATE   = "2024-01-07"

BASE_URL = "https://api.tfl.gov.uk/Road/All/Disruption"

os.makedirs("data", exist_ok=True)


def fetch_disruptions(start_date: str | None, end_date: str | None):
    if not APP_KEY or APP_KEY == "PASTE_YOUR_TFL_PRIMARY_KEY_HERE":
        raise RuntimeError(
            "APP_KEY is not set. Set TFL_APP_KEY env var or edit APP_KEY in the script."
        )

    params = {"app_key": APP_KEY}

    # Only send dates if we explicitly want a short range
    if start_date and end_date:
        params["startDate"] = start_date
        params["endDate"] = end_date

    print("Requesting TfL disruptions...")
    print("URL:", BASE_URL)
    print("Params:", params)

    resp = requests.get(BASE_URL, params=params, timeout=60)
    print("HTTP status:", resp.status_code)

    if resp.status_code != 200:
        print("Response text (first 500 chars):")
        print(resp.text[:500])
        resp.raise_for_status()

    return resp.json()


def flatten_disruptions(disruptions):
    """
    Flatten list of disruption objects into a DataFrame.
    """
    rows = []

    for d in disruptions:
        disruption_id = d.get("id")
        category = d.get("category")
        sub_category = d.get("subCategory")

        severity = d.get("severity") or d.get("statusSeverity")
        severity_desc = d.get("severityDescription") or d.get("statusSeverityDescription")

        location = d.get("location")
        comments = d.get("comments")

        corridor_ids = d.get("corridorIds") or d.get("roadDisruptionLines") or []
        if isinstance(corridor_ids, list):
            corridor_str = ",".join(str(x) for x in corridor_ids)
        else:
            corridor_str = str(corridor_ids) if corridor_ids is not None else ""

        start_time = d.get("startDateTime")
        end_time = d.get("endDateTime")

        rows.append(
            {
                "disruption_id": disruption_id,
                "category": category,
                "sub_category": sub_category,
                "severity": severity,
                "severity_description": severity_desc,
                "location": location,
                "corridor_ids": corridor_str,
                "start_datetime": start_time,
                "end_datetime": end_time,
                "comments": comments,
            }
        )

    return pd.DataFrame(rows)


def main():
    if USE_DATE_RANGE:
        disruptions = fetch_disruptions(START_DATE, END_DATE)
    else:
        # No dates → current disruptions only
        disruptions = fetch_disruptions(None, None)

    print(f"Fetched {len(disruptions)} disruption records")

    if not disruptions:
        print("WARNING: API returned an empty list.")
        return

    df = flatten_disruptions(disruptions)

    # Convert times to proper datetime and also create an hourly bucket
    for col in ["start_datetime", "end_datetime"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["timestamp_hour"] = df["start_datetime"].dt.floor("H")

    out_path = "data/tfl_disruptions_london.csv"
    print(f"Saving flattened disruptions to {out_path}")
    df.to_csv(out_path, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
