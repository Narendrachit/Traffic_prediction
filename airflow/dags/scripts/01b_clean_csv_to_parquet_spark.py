# scripts/01b_clean_csv_to_parquet_spark.py
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from scripts._spark_win import get_spark, spark_path


def to_snake(s: str) -> str:
    s = (s or "").strip().replace("\ufeff", "")
    s = s.replace("(", "").replace(")", "")
    s = s.replace("/", "_").replace("-", "_").replace(" ", "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.lower()


def _find_input_files(base: Path) -> list[str]:
    """
    Your input is nested like:
      region=London/year=YYYY/month=MM/part-....
    Spark on Windows sometimes fails when listing directories.
    So we enumerate files in Python and pass explicit file paths to Spark.
    """
    files: list[Path] = []
    for p in base.rglob("*"):
        if not p.is_file():
            continue
        name = p.name.lower()
        if name.startswith("_"):   # skip _SUCCESS etc
            continue
        if p.name.startswith("part-") or p.suffix.lower() in (".csv", ".gz"):
            files.append(p)

    return [spark_path(f) for f in sorted(files)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True, help="Input folder containing nested CSV part files")
    ap.add_argument("--out_parquet", required=True, help="Output parquet base folder")
    ap.add_argument("--region", default="London")
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    ap.add_argument("--driver_memory", default="2g")
    ap.add_argument("--shuffle_partitions", type=int, default=16)

    ap.add_argument("--repartition", type=int, default=None, help="Repartition before write (expensive)")
    ap.add_argument("--coalesce", type=int, default=None, help="Coalesce before write (cheaper)")

    args = ap.parse_args()

    spark = get_spark(
        "step1b_clean_csv_to_parquet",
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
    )

    in_base = Path(args.in_csv)
    if not in_base.is_absolute():
        in_base = (Path.cwd() / in_base).resolve()
    if not in_base.exists():
        raise SystemExit(f"[ERROR] Input path does not exist: {in_base}")

    read_files = _find_input_files(in_base)
    if not read_files:
        raise SystemExit(f"[ERROR] No CSV/part files found under: {in_base}")

    print("READ FILE COUNT:", len(read_files))
    print("READ ROOT      :", spark_path(in_base))

    reader = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # keep strings first (safer)
        .option("escape", '"')
        .option("quote", '"')
    )

    df = reader.csv(read_files)

    # snake_case columns
    old_cols = df.columns
    new_cols = [to_snake(c) for c in old_cols]
    for o, n in zip(old_cols, new_cols):
        if o != n:
            df = df.withColumnRenamed(o, n)

    # normalize null markers
    for c in df.columns:
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.col(c)))
        df = df.withColumn(
            c,
            F.when(F.upper(F.trim(F.col(c))).isin("NULL", "N/A", "NA", "NONE"), F.lit(None)).otherwise(F.col(c)),
        )

    # cast numeric-like columns (sample-based to avoid scanning whole dataset per column)
    sample = df.limit(50000)
    numeric_regex = r"^-?\d+(\.\d+)?$"
    for c in df.columns:
        if any(k in c for k in ["road_name", "junction", "location", "name", "code", "region"]):
            continue
        cleaned = F.regexp_replace(F.col(c), ",", "")
        ratio = sample.select(F.avg(F.when(cleaned.rlike(numeric_regex), 1).otherwise(0)).alias("r")).collect()[0]["r"]
        if ratio is not None and ratio >= 0.80:
            df = df.withColumn(c, cleaned.cast(DoubleType()))

    # ensure region column exists
    df = df.withColumn("region", F.lit(args.region))

    # repartition/coalesce before write (optional)
    if args.repartition is not None:
        df = df.repartition(int(args.repartition))
    elif args.coalesce is not None:
        df = df.coalesce(int(args.coalesce))

    out_base = Path(args.out_parquet)
    if not out_base.is_absolute():
        out_base = (Path.cwd() / out_base).resolve()
    out_path = out_base / f"region={args.region}"
    out_path_str = spark_path(out_path)

    (
        df.write.mode(args.mode)
        .option("compression", "snappy")
        .parquet(out_path_str)
    )

    print("DONE Step1b")
    print("Output:", str(out_path))
    spark.stop()


if __name__ == "__main__":
    main()
