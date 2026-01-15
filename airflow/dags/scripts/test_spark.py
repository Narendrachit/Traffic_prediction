from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestSpark").getOrCreate()

df = spark.range(5)
df.show()

spark.stop()

