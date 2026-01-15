# traffic_pipeline.py
#
# End-to-end pipeline:
#   - Reads data/dft_london_traffic_counts.csv  (DfT)
#   - Reads data/tfl_disruptions_london.csv     (TfL)
#   - Joins them on hourly timestamp
#   - Engineers features and trains a GBT regressor
#   - Saves predictions to CSV (avoids Hadoop/winutils issues on Windows)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    concat_ws,
    hour,
    dayofweek,
    month,
    lpad,
    when,
    instr,
    avg as _avg,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def build_spark():
    spark = (
        SparkSession.builder
        .appName("LondonTrafficCongestionPrediction")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    return spark


def load_dft(spark: SparkSession):
    """
    Load DfT London counts produced by download_dft_london_counts.py.

    Expected columns (lowercase):
      ['year','count_date','hour','region_name','local_authority',
       'road_name','latitude','longitude','all_hgvs','all_motor_vehicles']
    """

    dft_df = spark.read.csv(
        "data/dft_london_traffic_counts.csv",
        header=True,
        inferSchema=True,
    )

    # Some rows have count_date like "21/05/2004" (dd/MM/yyyy),
    # others like "2020-06-26" (yyyy-MM-dd).
    # Build "date hour" string, then apply a format conditionally.
    dft_df = dft_df.withColumn(
        "hour_str", lpad(col("hour").cast("string"), 2, "0")
    ).withColumn(
        "datetime_str",
        concat_ws(" ", col("count_date"), col("hour_str"))
    ).withColumn(
        "timestamp",
        when(
            instr(col("count_date").cast("string"), "-") > 0,
            # yyyy-MM-dd HH
            to_timestamp(col("datetime_str"), "yyyy-MM-dd HH"),
        ).otherwise(
            # dd/MM/yyyy HH
            to_timestamp(col("datetime_str"), "dd/MM/yyyy HH"),
        ),
    ).drop("hour_str", "datetime_str")

    # Drop rows where timestamp parsing failed
    dft_df = dft_df.filter(col("timestamp").isNotNull())

    dft_df = (
        dft_df.withColumn("hour_of_day", hour(col("timestamp")))
        .withColumn("day_of_week", dayofweek(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
    )

    return dft_df


def load_tfl(spark: SparkSession):
    """
    Load TfL disruptions CSV from fetch_tfl_disruptions.py.

    Expected columns:
      ['disruption_id','category','sub_category','severity',
       'severity_description','location','corridor_ids',
       'start_datetime','end_datetime','comments','timestamp_hour']
    """

    tfl_df = spark.read.csv(
        "data/tfl_disruptions_london.csv",
        header=True,
        inferSchema=True,
    )

    # Parse timestamp_hour into proper timestamp
    tfl_df = tfl_df.withColumn(
        "timestamp_hour",
        to_timestamp(col("timestamp_hour")),
    )

    # Map severity text to numeric index (adjust as needed based on your data)
    tfl_df = tfl_df.withColumn(
        "severity_num",
        when(col("severity") == "Severe", 3.0)
        .when(col("severity") == "Serious", 2.0)
        .when(col("severity") == "Moderate", 1.0)
        .when(col("severity") == "Minor", 1.0)
        .otherwise(0.0),
    )

    # Aggregate to one value per hour (city-wide average severity)
    tfl_agg = (
        tfl_df.groupBy("timestamp_hour")
        .agg(_avg("severity_num").alias("avg_severity"))
    )

    return tfl_agg


def main():
    spark = build_spark()

    # 1. Load DfT and TfL data
    dft_df = load_dft(spark)
    tfl_agg = load_tfl(spark)

    # 2. Join on timestamp (hourly)
    joined = (
        dft_df.alias("d")
        .join(
            tfl_agg.alias("t"),
            col("d.timestamp") == col("t.timestamp_hour"),
            how="left",
        )
        .drop("timestamp_hour")
        .fillna({"avg_severity": 0.0})
    )

    # 3. Target variable
    data = joined.withColumn(
        "target_volume", col("all_motor_vehicles").cast("double")
    )

    # 4. Clean rows (drop any with missing feature/label)
    feature_cols = [
        "hour_of_day",
        "day_of_week",
        "month",
        "avg_severity",
        "latitude",
        "longitude",
    ]

    cols_for_na_drop = ["target_volume"] + feature_cols
    data_clean = data.dropna(subset=cols_for_na_drop)

    # 5. Random train/test split (avoids empty train set)
    train_df, test_df = data_clean.randomSplit([0.8, 0.2], seed=42)

    print(f"Train rows: {train_df.count()}")
    print(f"Test rows : {test_df.count()}")

    # 6. Feature pipeline
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True,
    )

    gbt = GBTRegressor(
        labelCol="target_volume",
        featuresCol="features",
        maxDepth=6,
        maxIter=80,
        stepSize=0.1,
    )

    pipeline = Pipeline(stages=[assembler, scaler, gbt])

    # 7. Fit model
    model = pipeline.fit(train_df)

    # 8. Predictions + evaluation
    predictions = model.transform(test_df)

    for metric in ["rmse", "mae", "r2"]:
        evaluator = RegressionEvaluator(
            labelCol="target_volume",
            predictionCol="prediction",
            metricName=metric,
        )
        val = evaluator.evaluate(predictions)
        print(f"Test {metric.upper()}: {val:.3f}")

    # 9. Save predictions to CSV using pandas (avoid Hadoop/winutils issues on Windows)
    import os

    os.makedirs("outputs", exist_ok=True)

    preds_sel = predictions.select(
        "road_name",
        "timestamp",
        "target_volume",
        "prediction",
        "hour_of_day",
        "day_of_week",
        "month",
        "avg_severity",
    )

    preds_pdf = preds_sel.toPandas()
    full_csv_path = "outputs/predictions_london_congestion.csv"
    preds_pdf.to_csv(full_csv_path, index=False)
    print(f"Predictions saved to {full_csv_path}")

    spark.stop()


if __name__ == "__main__":
    main()
