import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

START = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
END = sys.argv[2] if len(sys.argv) > 2 else "2025-02-01"

spark = SparkSession.builder.appName("clean_features").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.parquet("/data/raw/*.parquet").select(
    "pickup_datetime", "dropoff_datetime", "PULocationID",
    "DOLocationID", "trip_miles", "trip_time", "base_passenger_fare")
raw_count = raw.count()

start_ts = F.lit(START).cast("timestamp_ntz")
end_ts = F.lit(END).cast("timestamp_ntz")

clean = (raw
    .filter((F.col("pickup_datetime") >= start_ts) & (F.col("pickup_datetime") < end_ts))
    .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
    .filter((F.col("trip_miles") > 0) & (F.col("trip_miles") < 100))
    .filter((F.col("trip_time") > 0) & (F.col("trip_time") < 6 * 3600))
    .filter(F.col("base_passenger_fare") > 0)
    .filter(F.col("PULocationID").between(1, 263)))

clean_count = clean.count()
print(f"Raw rows: {raw_count}, clean rows: {clean_count}, removed: {raw_count - clean_count}")

# Zone x hour demand
hourly = (clean
    .withColumn("hour_ts", F.date_trunc("hour", "pickup_datetime"))
    .groupBy("PULocationID", "hour_ts")
    .agg(F.count("*").alias("demand"),
         F.round(F.avg("base_passenger_fare"), 2).alias("avg_fare"),
         F.round(F.avg("trip_miles"), 2).alias("avg_miles")))

# Complete grid: every zone x every hour (missing hour = 0 demand)
hours = hourly.select("hour_ts").distinct()
zones = spark.range(1, 264).select(F.col("id").cast("int").alias("PULocationID"))
full = (zones.crossJoin(hours)
    .join(hourly, ["PULocationID", "hour_ts"], "left")
    .withColumn("demand", F.coalesce(F.col("demand"), F.lit(0)))
    .fillna({"avg_fare": 0.0, "avg_miles": 0.0}))

# Time and lag features
w = Window.partitionBy("PULocationID").orderBy("hour_ts")
feat = (full
    .withColumn("hour", F.hour("hour_ts"))
    .withColumn("dow", F.dayofweek("hour_ts"))
    .withColumn("is_weekend", F.when(F.col("dow").isin(1, 7), 1).otherwise(0))
    .withColumn("is_holiday",
        F.when(F.to_date("hour_ts").cast("string").isin("2025-01-01", "2025-01-20"), 1).otherwise(0))
    .withColumn("lag_1h", F.lag("demand", 1).over(w))
    .withColumn("lag_24h", F.lag("demand", 24).over(w))
    .withColumn("lag_168h", F.lag("demand", 168).over(w))
    .withColumn("roll_3h", F.round(F.avg("demand").over(w.rowsBetween(-3, -1)), 2)))

feat.cache()
print("Zone-hour rows:", feat.count())
feat.orderBy(F.desc("demand")).show(5, truncate=False)

feat.coalesce(1).write.mode("overwrite").parquet("/data/processed/zone_hour")
print("Saved to /data/processed/zone_hour")
spark.stop()
