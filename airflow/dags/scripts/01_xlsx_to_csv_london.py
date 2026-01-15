import os
import argparse
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True, help="CSV base folder (region=London root)")
    ap.add_argument("--out_parquet", required=True, help="Output parquet base folder")
    args = ap.parse_args()

    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        bin_dir = str(Path(hadoop_home) / "bin")
        os.environ["PATH"] = bin_dir + ";" + os.environ.get("PATH", "")

    tmp_root = Path(os.environ.get("SPARK_LOCAL_DIR", Path(tempfile.gettempdir()) / "spark_tmp"))
    warehouse_dir = Path(os.environ.get("SPARK_WAREHOUSE_DIR", tmp_root / "warehouse"))
    tmp_root.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName("clean_csv_to_parquet")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.local.dir", str(tmp_root))
    )
    if hadoop_home:
        bin_dir = str(Path(hadoop_home) / "bin")
        builder = builder.config("spark.driver.extraJavaOptions", f"-Djava.library.path={bin_dir}")
        builder = builder.config("spark.executor.extraJavaOptions", f"-Djava.library.path={bin_dir}")

    spark = builder.getOrCreate()

    in_path = str(Path(args.in_csv))
    out_path = str(Path(args.out_parquet) / "region=London")

    # Read recursively (year=/month=/part*.csv)
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("recursiveFileLookup", True)
        .csv(in_path)
    )

    # Normalize null tokens
    null_tokens = ["NULL", "null", "NaN", "nan", ""]
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)).isin(null_tokens), F.lit(None)).otherwise(F.col(c))
        )

    # Required columns
    required_cols = [
        "count_date", "hour", "count_point_id",
        "start_junction_road_name", "end_junction_road_name",
        "all_motor_vehicles", "link_length_km"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns in CSV data: {missing}")

    # Cast types (bad casts -> null)
    df = (
        df
        .withColumn("count_date", F.to_date(F.col("count_date")))
        .withColumn("hour", F.col("hour").cast("int"))
        .withColumn("count_point_id", F.col("count_point_id").cast("long"))
        .withColumn("link_length_km", F.col("link_length_km").cast("double"))
        .withColumn("latitude", F.col("latitude").cast("double") if "latitude" in df.columns else F.lit(None).cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double") if "longitude" in df.columns else F.lit(None).cast("double"))
        .withColumn("all_motor_vehicles", F.col("all_motor_vehicles").cast("double"))
        .withColumn("all_HGVs", F.col("all_HGVs").cast("double") if "all_HGVs" in df.columns else F.lit(None).cast("double"))
    )

    # Drop only critical-null rows
    df = df.dropna(subset=[
        "count_date", "hour", "count_point_id",
        "start_junction_road_name", "end_junction_road_name",
        "all_motor_vehicles", "link_length_km"
    ])

    # Partitions
    df = (
        df
        .withColumn("p_year", F.year("count_date"))
        .withColumn("p_month", F.date_format("count_date", "MM"))
    )

    # Write parquet
    (
        df.write
        .mode("overwrite")
        .partitionBy("p_year", "p_month")
        .parquet(out_path)
    )

    print(f"DONE Step1b: Cleaned CSV -> Parquet written at: {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
