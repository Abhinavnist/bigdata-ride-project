from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check_data").getOrCreate()
df = spark.read.parquet("/data/raw/*.parquet")

df.printSchema()
print("Total rows:", df.count())
df.show(5, truncate=False)
spark.stop()
