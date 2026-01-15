# scripts/02_build_gold_features_spark.py
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts._spark_win import get_spark, spark_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_parquet", required=True, help="Input parquet folder (can be region partition)")
    ap.add_argument("--out_gold", required=True, help="Output gold base folder")
    ap.add_argument("--horizon_hours", type=int, default=1, help="Forecast horizon (hours)")

    ap.add_argument("--tfl_disruptions", default=None, help="Optional TfL disruptions CSV")
    ap.add_argument("--region", default="London")
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])

    ap.add_argument("--driver_memory", default="2g")
    ap.add_argument("--shuffle_partitions", type=int, default=16)
    ap.add_argument("--repartition", type=int, default=16)
    ap.add_argument("--coalesce", type=int, default=None)  # optional

    args = ap.parse_args()

    spark = get_spark(
        "step2_build_gold_features",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )

    in_path = spark_path(args.in_parquet)
    df = spark.read.parquet(in_path)

    # Ensure ts_hour exists (your data already has it, but keep this safe)
    if "ts_hour" not in df.columns and {"count_date", "hour"}.issubset(set(df.columns)):
        df = df.withColumn(
            "ts_hour",
            F.to_timestamp(
                F.concat_ws(" ", F.col("count_date").cast("string"), F.lpad(F.col("hour").cast("string"), 2, "0")),
                "yyyy-MM-dd HH",
            ),
        )

    # Use a stable series id
    series_id = "segment_id" if "segment_id" in df.columns else "count_point_id"
    if series_id not in df.columns:
        raise RuntimeError(f"Expected a series id column like segment_id or count_point_id, but got: {df.columns}")

    # Choose volume column
    vol_col = "volume" if "volume" in df.columns else "all_motor_vehicles"
    if vol_col not in df.columns:
        raise RuntimeError(f"Expected volume column 'volume' or 'all_motor_vehicles', but got: {df.columns}")

    # Basic time features
    df = (
        df.withColumn("hour_of_day", F.hour("ts_hour"))
          .withColumn("day_of_week", F.dayofweek("ts_hour"))  # 1=Sun..7=Sat
          .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]).cast("int"))
          .withColumn("month", F.month("ts_hour"))
          .withColumn("year", F.year("ts_hour"))
    )

    # Lag/rolling features
    w = Window.partitionBy(series_id).orderBy("ts_hour")

    df = (
        df.withColumn("lag_1h", F.lag(F.col(vol_col), 1).over(w))
          .withColumn("lag_2h", F.lag(F.col(vol_col), 2).over(w))
          .withColumn("lag_24h", F.lag(F.col(vol_col), 24).over(w))
          .withColumn("roll_mean_24h", F.avg(F.col(vol_col)).over(w.rowsBetween(-24, -1)))
          .withColumn("roll_std_24h", F.stddev(F.col(vol_col)).over(w.rowsBetween(-24, -1)))
    )

    # Target at horizon
    df = df.withColumn(f"y_t_plus_{args.horizon_hours}h", F.lead(F.col(vol_col), args.horizon_hours).over(w))

    # -----------------------------
    # TfL disruptions (JOIN BY HOUR)
    # -----------------------------
    if args.tfl_disruptions:
        tfl_path = Path(args.tfl_disruptions)
        if tfl_path.exists():
            tfl = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(spark_path(str(tfl_path)))
            )

            # try common datetime columns
            candidates = [
                "timestamp", "datetime", "date_time", "start_time", "start_datetime",
                "created", "publish_date", "last_modified", "time"
            ]
            tcol = next((c for c in candidates if c in tfl.columns), None)
            if tcol is None:
                # fall back: pick first column containing "date" or "time"
                tcol = next((c for c in tfl.columns if "date" in c.lower() or "time" in c.lower()), None)

            if tcol is None:
                raise RuntimeError(f"TfL CSV has no obvious datetime column. Columns: {tfl.columns}")

            tfl = tfl.withColumn("tfl_ts", F.to_timestamp(F.col(tcol)))
            # if parsing fails, try a common UK-ish format
            tfl = tfl.withColumn("tfl_ts", F.coalesce(
                F.col("tfl_ts"),
                F.to_timestamp(F.col(tcol), "dd/MM/yyyy HH:mm"),
                F.to_timestamp(F.col(tcol), "dd/MM/yyyy HH:mm:ss"),
                F.to_timestamp(F.col(tcol), "yyyy-MM-dd HH:mm:ss"),
            ))

            tfl = tfl.filter(F.col("tfl_ts").isNotNull())
            tfl = tfl.withColumn("timestamp_hour", F.date_trunc("hour", F.col("tfl_ts")))

            # hourly disruption count (region-wide)
            tfl_hour = (
                tfl.groupBy("timestamp_hour")
                .agg(F.count(F.lit(1)).alias("tfl_disruptions_hour"))
            )

            df = (
                df.join(tfl_hour, df["ts_hour"] == tfl_hour["timestamp_hour"], "left")
                  .drop("timestamp_hour")
                  .fillna({"tfl_disruptions_hour": 0})
            )
        else:
            print(f"WARNING: TfL file not found: {args.tfl_disruptions}")

    # Add/force region column
    if "region" not in df.columns:
        df = df.withColumn("region", F.lit(args.region))

    # Drop rows where we can’t build features/target
    df = df.filter(F.col("lag_1h").isNotNull() & F.col(f"y_t_plus_{args.horizon_hours}h").isNotNull())

    # Partition control
    if args.coalesce is not None:
        df = df.coalesce(int(args.coalesce))
    else:
        df = df.repartition(int(args.repartition))

    out_base = Path(args.out_gold)
    out_path = out_base / f"region={args.region}"
    out_write = spark_path(str(out_path))

    (
        df.write.mode(args.mode)
        .option("compression", "snappy")
        .parquet(out_write)
    )

    print("DONE Step2")
    print("Input :", args.in_parquet)
    print("Output:", str(out_path))

    spark.stop()


if __name__ == "__main__":
    main()
