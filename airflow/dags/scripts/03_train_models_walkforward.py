import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from scripts._spark_win import get_spark, spark_path

# ---------------------------------------------------------------------
# distutils shim for Python 3.12 environments (some Spark/PySpark builds)
# ---------------------------------------------------------------------
def _ensure_distutils():
    try:
        from distutils.version import LooseVersion  # noqa: F401
        return
    except ModuleNotFoundError:
        try:
            from setuptools._distutils.version import LooseVersion  # type: ignore
            import types

            distutils_mod = types.ModuleType("distutils")
            version_mod = types.ModuleType("distutils.version")
            version_mod.LooseVersion = LooseVersion
            distutils_mod.version = version_mod

            sys.modules["distutils"] = distutils_mod
            sys.modules["distutils.version"] = version_mod
        except Exception:
            # If this fails, PySpark may still work in some environments.
            pass

_ensure_distutils()

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Your helper (already in your project)
from scripts._spark_win import get_spark, spark_path


def collect_parquet_files(base_dir: Path):
    """
    Return ONLY real parquet data files (exclude .crc, _SUCCESS, hidden dotfiles).
    This avoids Spark trying to read CSVs or checksum files as Parquet.
    """
    files = []
    for p in base_dir.rglob("*.parquet"):
        name = p.name
        if name.startswith("."):
            continue
        if name.endswith(".crc"):
            continue
        # keep typical Spark output files
        if name.startswith("part-") or name.endswith(".snappy.parquet") or name.endswith(".parquet"):
            files.append(p)
    return sorted(files)


def build_time_folds(df, time_col: str, n_folds: int, test_days: int):
    """
    Create folds by using the ordered list of unique timestamps.
    This prevents empty train/test windows when the data has gaps.
    """
    # Collect distinct timestamps in order (safe for your scale)
    times = (
        df.select(F.col(time_col).cast("timestamp").alias(time_col))
          .where(F.col(time_col).isNotNull())
          .distinct()
          .orderBy(time_col)
          .collect()
    )
    times = [r[0] for r in times]
    if len(times) < 10:
        raise ValueError(f"Not enough timestamps in {time_col} to build folds (found {len(times)}).")

    test_steps = max(1, test_days * 24)  # treat as hours ahead (works even with missing hours)
    folds = []

    # Use quantile-like cut points over the timestamp list
    for i in range(n_folds):
        frac = (i + 1) / (n_folds + 1)
        train_end_idx = int(frac * (len(times) - 1))
        train_end_idx = max(1, min(train_end_idx, len(times) - 2))

        test_end_idx = min(train_end_idx + test_steps, len(times) - 1)
        if test_end_idx <= train_end_idx:
            continue

        train_end = times[train_end_idx]
        test_end = times[test_end_idx]
        folds.append((i + 1, train_end, test_end))

    return folds


def geh_percent_lt5(pred_df, label_col: str, pred_col: str = "prediction"):
    """
    GEH = sqrt( 2*(p-o)^2 / (p+o) )
    Return percentage of rows where GEH < 5.
    """
    o = F.col(label_col).cast("double")
    p = F.col(pred_col).cast("double")

    denom = (p + o)
    geh = F.when(denom > 0, F.sqrt(2.0 * (p - o) * (p - o) / denom)).otherwise(F.lit(None))

    tmp = pred_df.select(geh.alias("geh")).where(F.col("geh").isNotNull())
    total = tmp.count()
    if total == 0:
        return 0.0
    good = tmp.where(F.col("geh") < 5.0).count()
    return (good / total) * 100.0


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--gold_parquet", required=True, help="Gold parquet folder (features_hourly output)")
    ap.add_argument("--out_dir", required=True, help="Where to save models + metrics")

    ap.add_argument("--label_col", default="y_t_plus_1h", help="Target column (default: y_t_plus_1h)")
    ap.add_argument("--time_col", default="ts_hour", help="Timestamp column (default: ts_hour)")

    ap.add_argument("--n_folds", type=int, default=3)
    ap.add_argument("--test_days", type=int, default=30)

    ap.add_argument("--shuffle_partitions", type=int, default=8)
    ap.add_argument("--repartition", type=int, default=0, help="Optional: repartition dataset to reduce memory")
    ap.add_argument("--driver_memory", default="4g")

    ap.add_argument("--enable_lr", type=int, default=1)
    ap.add_argument("--enable_gbt", type=int, default=1)
    ap.add_argument("--enable_rf", type=int, default=0)  # default OFF on Windows (often OOM)

    # regularization to prevent singular matrix / LBFGS issues
    ap.add_argument("--lr_reg", type=float, default=0.1)
    ap.add_argument("--lr_elastic", type=float, default=0.0)

    # smaller defaults to avoid OOM
    ap.add_argument("--gbt_iters", type=int, default=40)
    ap.add_argument("--gbt_depth", type=int, default=5)

    ap.add_argument("--rf_trees", type=int, default=40)
    ap.add_argument("--rf_depth", type=int, default=8)

    # optional sampling
    ap.add_argument("--max_train_rows", type=int, default=0, help="If >0, sample training down to this many rows")

    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark(app_name="traffic-step3-walkforward", driver_memory=args.driver_memory)
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")  # skips non-parquet junk safely

    gold = Path(args.gold_parquet)
    parquet_files = collect_parquet_files(gold)
    if not parquet_files:
        raise ValueError(f"No parquet files found under: {gold}")

    parquet_paths = [spark_path(str(p)) for p in parquet_files]
    df = spark.read.parquet(*parquet_paths)

    if args.repartition and args.repartition > 0:
        df = df.repartition(args.repartition)

    if args.label_col not in df.columns:
        raise ValueError(f"Missing label column '{args.label_col}'. Available: {df.columns}")
    if args.time_col not in df.columns:
        raise ValueError(f"Missing time column '{args.time_col}'. Available: {df.columns}")

    # Basic cleaning
    df = df.where(F.col(args.label_col).isNotNull()).where(F.col(args.time_col).isNotNull())

    # Choose features (exclude obvious non-features)
    exclude = {
        args.label_col, args.time_col,
        "count_date", "region_name", "region_ons_code", "region",
        "start_junction_road_name", "end_junction_road_name",
    }
    feature_cols = [c for c in df.columns if c not in exclude]

    # Convert categoricals (direction/road_category/road_type etc.) safely using StringIndexer? (optional)
    # For simplicity + stability, keep ONLY numeric columns here:
    numeric_cols = []
    for c, t in df.dtypes:
        if c in feature_cols and t in ("int", "bigint", "double", "float", "smallint", "tinyint"):
            numeric_cols.append(c)

    if len(numeric_cols) < 3:
        raise ValueError(f"Not enough numeric feature columns found. Candidates were: {feature_cols}")

    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features", handleInvalid="skip")
    df_feat = assembler.transform(df).select(
        F.col(args.time_col).cast("timestamp").alias(args.time_col),
        F.col(args.label_col).cast("double").alias(args.label_col),
        F.col("features")
    ).cache()

    folds = build_time_folds(df_feat, args.time_col, args.n_folds, args.test_days)
    if not folds:
        raise ValueError("No folds could be built. Reduce --n_folds / --test_days or check timestamps.")

    evaluator_rmse = RegressionEvaluator(labelCol=args.label_col, predictionCol="prediction", metricName="rmse")
    evaluator_mae  = RegressionEvaluator(labelCol=args.label_col, predictionCol="prediction", metricName="mae")
    evaluator_r2   = RegressionEvaluator(labelCol=args.label_col, predictionCol="prediction", metricName="r2")

    results = []

    def maybe_sample_train(train_df):
        if args.max_train_rows and args.max_train_rows > 0:
            n = train_df.count()
            if n > args.max_train_rows:
                frac = args.max_train_rows / n
                return train_df.sample(withReplacement=False, fraction=frac, seed=42)
        return train_df

    for fold_idx, train_end, test_end in folds:
        train = df_feat.where(F.col(args.time_col) <= F.lit(train_end))
        test  = df_feat.where((F.col(args.time_col) > F.lit(train_end)) & (F.col(args.time_col) <= F.lit(test_end)))

        train_n = train.count()
        test_n  = test.count()
        if train_n == 0 or test_n == 0:
            continue

        train = maybe_sample_train(train)

        # -------------------- Baseline: lag_1h if present --------------------
        # If lag_1h exists in original df, it is NOT in df_feat now (we dropped non-numeric/non-features).
        # So baseline = predict label using lag_1h ONLY if lag_1h was included in numeric_cols.
        if "lag_1h" in numeric_cols:
            base_pred = test.select(
                F.col(args.label_col).alias(args.label_col),
                F.col("features"),  # unused
            ).withColumn("prediction", F.lit(None))
            # Extract lag_1h from features vector isn't trivial -> so baseline here is skipped by default.
            # (You already have baseline in your previous outputs; keeping Step3 stable is better.)
        # --------------------------------------------------------------------

        # -------------------- Linear Regression --------------------
        if args.enable_lr:
            lr = LinearRegression(
                featuresCol="features",
                labelCol=args.label_col,
                predictionCol="prediction",
                maxIter=100,
                regParam=args.lr_reg,
                elasticNetParam=args.lr_elastic,
                standardization=True
            )
            m = lr.fit(train)
            pred = m.transform(test).select(args.label_col, "prediction")
            results.append((
                fold_idx, "LR", str(train_end), str(test_end), train_n, test_n,
                float(evaluator_rmse.evaluate(pred)),
                float(evaluator_mae.evaluate(pred)),
                float(evaluator_r2.evaluate(pred)),
                float(geh_percent_lt5(pred, args.label_col))
            ))

        # -------------------- Gradient Boosted Trees --------------------
        if args.enable_gbt:
            gbt = GBTRegressor(
                featuresCol="features",
                labelCol=args.label_col,
                predictionCol="prediction",
                maxIter=args.gbt_iters,
                maxDepth=args.gbt_depth,
                maxBins=64,
                stepSize=0.1,
                subsamplingRate=0.7,
                minInstancesPerNode=10
            )
            m = gbt.fit(train)
            pred = m.transform(test).select(args.label_col, "prediction")
            results.append((
                fold_idx, "GBT", str(train_end), str(test_end), train_n, test_n,
                float(evaluator_rmse.evaluate(pred)),
                float(evaluator_mae.evaluate(pred)),
                float(evaluator_r2.evaluate(pred)),
                float(geh_percent_lt5(pred, args.label_col))
            ))

        # -------------------- Random Forest (heavy) --------------------
        if args.enable_rf:
            rf = RandomForestRegressor(
                featuresCol="features",
                labelCol=args.label_col,
                predictionCol="prediction",
                numTrees=args.rf_trees,
                maxDepth=args.rf_depth,
                maxBins=64,
                subsamplingRate=0.7,
                featureSubsetStrategy="sqrt",
                minInstancesPerNode=10
            )
            m = rf.fit(train)
            pred = m.transform(test).select(args.label_col, "prediction")
            results.append((
                fold_idx, "RF", str(train_end), str(test_end), train_n, test_n,
                float(evaluator_rmse.evaluate(pred)),
                float(evaluator_mae.evaluate(pred)),
                float(evaluator_r2.evaluate(pred)),
                float(geh_percent_lt5(pred, args.label_col))
            ))

    if not results:
        raise ValueError("No folds produced results. Try lowering --n_folds / --test_days or check timestamps.")

    # Write metrics using plain Python (avoids Spark CSV write crashes)
    import pandas as pd
    cols = ["fold", "model", "train_end", "test_end", "train_rows", "test_rows", "rmse", "mae", "r2", "geh_pct_lt5"]
    metrics = pd.DataFrame(results, columns=cols)

    metrics_path = out_dir / "walkforward_metrics.csv"
    metrics.to_csv(metrics_path, index=False)

    print("\nDONE Step3: Walk-forward training/evaluation")
    print("Saved:", metrics_path)
    print("\nModel summary (mean RMSE):")
    print(metrics.groupby("model")["rmse"].mean().sort_values())


if __name__ == "__main__":
    main()
