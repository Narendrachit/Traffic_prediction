from __future__ import annotations

import argparse
from pathlib import Path
from datetime import timedelta

import pandas as pd

from scripts._spark_win import get_spark, spark_path


def _read_parquet_strict(spark, folder: str):
    """
    Read only *.parquet files from a folder.
    This avoids failures if CSV/_SUCCESS/.crc files exist in the same directory.
    """
    p = Path(folder)
    files = sorted(p.glob("part-*.parquet"))
    if not files:
        # fallback (some setups may not use part- prefix)
        files = sorted(p.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in: {p.resolve()}")

    # Ensure Spark gets plain strings, not Path objects (PySpark cannot serialize Path).
    uris = [str(spark_path(f)) for f in files]
    return spark.read.parquet(*uris)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold_parquet", required=True, help="Gold parquet folder (region folder)")
    ap.add_argument("--out_dir", required=True, help="Output folder for dashboard data")

    ap.add_argument("--label_col", default="y_t_plus_1h")
    ap.add_argument("--time_col", default="ts_hour")

    ap.add_argument("--hotspot_quantile", type=float, default=0.60, help="Top-q predicted volume per hour => hotspot")
    ap.add_argument("--score_days", type=int, default=90, help="Predict only last N days (0 = all)")

    # Spark tuning
    ap.add_argument("--driver_memory", default="4g")
    ap.add_argument("--shuffle_partitions", type=int, default=4)
    ap.add_argument("--repartition", type=int, default=4)

    # GBT params (safe on laptop)
    ap.add_argument("--gbt_maxIter", type=int, default=60)
    ap.add_argument("--gbt_maxDepth", type=int, default=6)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max_train_rows", type=int, default=200000, help="Cap training rows (0 = no cap)")

    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark(
        "step5_make_dashboard_data",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor
    from pyspark.ml.evaluation import RegressionEvaluator

    # ---- Load gold parquet strictly (ignore csv files etc) ----
    df = _read_parquet_strict(spark, args.gold_parquet)

    # ensure timestamp
    df = df.withColumn(args.time_col, F.to_timestamp(F.col(args.time_col)))

    # ---- Required columns ----
    required_cols = [
        args.time_col,
        args.label_col,
        "count_point_id",
        "latitude",
        "longitude",
        "start_junction_road_name",
        "end_junction_road_name",
    ]
    missing_req = [c for c in required_cols if c not in df.columns]
    if missing_req:
        raise ValueError(f"Missing required columns: {missing_req}\nAvailable: {df.columns}")

    # ---- Feature columns (numeric only, use what exists) ----
    candidate_features = [
        # temporal
        "hour_of_day", "day_of_week", "is_weekend", "month", "year",
        # volume signals (allowed for 1-hour ahead forecasting)
        "all_motor_vehicles", "all_HGVs",
        # lags / rolling
        "lag_1h", "lag_2h", "lag_24h",
        "roll_mean_24h", "roll_std_24h",
        # disruptions + geometry
        "tfl_disruptions_hour",
        "link_length_km",
    ]
    feature_cols = [c for c in candidate_features if c in df.columns]
    if not feature_cols:
        raise ValueError(f"No features found from: {candidate_features}\nAvailable: {df.columns}")

    # ---- Clean ----
    keep = required_cols + feature_cols
    df = df.select(*keep)

    # cast label + numeric features
    df = df.withColumn(args.label_col, F.col(args.label_col).cast("double"))
    for c in feature_cols:
        df = df.withColumn(c, F.col(c).cast("double"))

    df = df.dropna(subset=[args.time_col, args.label_col] + feature_cols)

    # ---- Train/score split (last N days) ----
    max_t = df.agg(F.max(args.time_col).alias("max_t")).collect()[0]["max_t"]
    if max_t is None:
        raise ValueError("No timestamps after cleaning.")

    if args.score_days and args.score_days > 0:
        cutoff = max_t - timedelta(days=int(args.score_days))
        train_df = df.filter(F.col(args.time_col) <= F.lit(cutoff))
        score_df = df.filter(F.col(args.time_col) > F.lit(cutoff))
    else:
        train_df = df
        score_df = df

    if args.repartition and args.repartition > 0:
        train_df = train_df.repartition(int(args.repartition))
        score_df = score_df.repartition(int(args.repartition))

    train_n = train_df.count()
    if args.max_train_rows and args.max_train_rows > 0 and train_n > args.max_train_rows:
        frac = args.max_train_rows / train_n
        train_df = train_df.sample(withReplacement=False, fraction=frac, seed=args.seed)
        train_n = train_df.count()
    score_n = score_df.count()
    if train_n == 0 or score_n == 0:
        raise ValueError(f"Empty train/score split. train={train_n}, score={score_n}. Try --score_days 0 or smaller.")

    # ---- Model ----
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=args.label_col,
        predictionCol="y_pred",
        maxIter=args.gbt_maxIter,
        maxDepth=args.gbt_maxDepth,
        seed=args.seed,
    )

    pipe = Pipeline(stages=[assembler, gbt])
    model = pipe.fit(train_df)

    preds = model.transform(score_df).select(
        F.col(args.time_col).alias("ts_hour"),
        F.col("count_point_id"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("start_junction_road_name").alias("from_node"),
        F.col("end_junction_road_name").alias("to_node"),
        F.col(args.label_col).alias("y_true"),
        F.col("y_pred").alias("y_pred"),
        *[F.col(c) for c in ["hour_of_day", "day_of_week", "is_weekend", "month", "year"] if c in df.columns],
    )

    # ---- Hotspot definition (volume-based) ----
    # For each hour, compute P90 of predicted volume across all links.
    w = Window.partitionBy("ts_hour")
    thr = F.expr(f"percentile_approx(y_pred, {float(args.hotspot_quantile)})").over(w)

    preds = preds.withColumn("hotspot_threshold", thr)
    preds = preds.withColumn("is_hotspot", F.when(F.col("y_pred") >= F.col("hotspot_threshold"), F.lit(1)).otherwise(F.lit(0)))

    # ---- Save predictions parquet (for Mapbox/PowerBI via parquet connector, or future steps) ----
    pred_parquet_dir = out_dir / "predictions_hourly_parquet"
    preds.write.mode("overwrite").parquet(str(spark_path(pred_parquet_dir)))

    # ---- Evaluate quickly (RMSE/MAE/R2 on scoring window) ----
    evaluator_rmse = RegressionEvaluator(labelCol="y_true", predictionCol="y_pred", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="y_true", predictionCol="y_pred", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="y_true", predictionCol="y_pred", metricName="r2")
    rmse = float(evaluator_rmse.evaluate(preds))
    mae = float(evaluator_mae.evaluate(preds))
    r2 = float(evaluator_r2.evaluate(preds))

    # ---- Aggregations for dashboard (small => write as single CSV using pandas) ----
    # (A) Map layer per count point (hotspot frequency)
    map_df = (preds.groupBy("count_point_id", "latitude", "longitude")
              .agg(
                  F.avg("is_hotspot").alias("hotspot_rate"),
                  F.avg("y_pred").alias("avg_pred"),
                  F.avg("y_true").alias("avg_true"),
                  F.count("*").alias("n_rows"),
              ))

    # (B) Spatio-temporal patterns (hour x day_of_week)
    time_cols = [c for c in ["hour_of_day", "day_of_week", "is_weekend", "month"] if c in preds.columns]
    time_df = (preds.groupBy(*time_cols)
               .agg(
                   F.avg("y_true").alias("avg_true"),
                   F.avg("y_pred").alias("avg_pred"),
                   F.avg("is_hotspot").alias("hotspot_rate"),
                   F.count("*").alias("n_rows"),
               ))

    # (C) Edge hotspot rate for routing penalties (from_node -> to_node)
    edge_df = (preds.groupBy("from_node", "to_node")
               .agg(
                   F.avg("is_hotspot").alias("hotspot_rate"),
                   F.avg("y_pred").alias("avg_pred"),
                   F.count("*").alias("n_rows"),
               ))

    # collect small tables to pandas and save as single CSV files
    map_pd = map_df.toPandas()
    time_pd = time_df.toPandas()
    edge_pd = edge_df.toPandas()

    map_csv = out_dir / "hotspots_map.csv"
    time_csv = out_dir / "hotspots_time_summary.csv"
    edge_csv = out_dir / "edge_hotspot_rate.csv"

    map_pd.to_csv(map_csv, index=False)
    time_pd.to_csv(time_csv, index=False)
    edge_pd.to_csv(edge_csv, index=False)

    # also save a simple evaluation txt (for dissertation)
    eval_txt = out_dir / "model_eval_on_scoring_window.txt"
    eval_txt.write_text(
        "\n".join([
            f"Scoring window rows: {score_n}",
            f"Train rows: {train_n}",
            f"RMSE: {rmse:.3f}",
            f"MAE : {mae:.3f}",
            f"R2  : {r2:.6f}",
            f"Hotspot quantile (per hour): {args.hotspot_quantile}",
        ]),
        encoding="utf-8"
    )

    print("DONE Step5")
    print("Predictions parquet:", pred_parquet_dir)
    print("Dashboard CSVs:")
    print(" -", map_csv)
    print(" -", time_csv)
    print(" -", edge_csv)
    print("Eval file:", eval_txt)

    spark.stop()


if __name__ == "__main__":
    main()
