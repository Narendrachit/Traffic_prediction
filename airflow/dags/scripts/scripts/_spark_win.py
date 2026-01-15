from __future__ import annotations
from pyspark.sql import SparkSession

def spark_path(p: str) -> str:
    return p

def get_spark(app_name: str, driver_memory: str = "2g", shuffle_partitions: int = 16):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
