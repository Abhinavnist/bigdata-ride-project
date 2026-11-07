from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("load_to_cassandra")
         .config("spark.cassandra.connection.host", "cassandra")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pred = spark.read.parquet("/data/processed/predictions_v2")
hist = spark.read.parquet("/data/processed/zone_hour")

split = F.lit("2025-01-25").cast("timestamp_ntz")
base = (hist.filter(F.col("hour_ts") < split)
            .groupBy("PULocationID", "hour")
            .agg(F.avg("demand").alias("baseline")))

out = (pred.withColumn("hour", F.hour("hour_ts"))
           .join(base, ["PULocationID", "hour"], "left")
           .withColumn("surge_ratio",
                       F.round(F.col("predicted_demand") / (F.col("baseline") + 1.0), 2))
           .fillna({"surge_ratio": 1.0})
           .withColumn("surge_level",
                       F.when(F.col("surge_ratio") >= 1.5, "HIGH")
                        .when(F.col("surge_ratio") >= 1.2, "MEDIUM")
                        .otherwise("NORMAL"))
           .select(F.col("PULocationID").alias("zone_id"),
                   F.col("hour_ts").cast("timestamp").alias("hour_ts"),
                   F.col("demand").cast("double").alias("actual_demand"),
                   F.col("predicted_demand").cast("double").alias("predicted_demand"),
                   F.col("surge_ratio").cast("double").alias("surge_ratio"),
                   "surge_level").cache())

print("Rows to load:", out.count())
out.groupBy("surge_level").count().show()

fmt = "org.apache.spark.sql.cassandra"
out.write.format(fmt).mode("append").options(table="zone_demand", keyspace="ride").save()
out.write.format(fmt).mode("append").options(table="zone_demand_by_hour", keyspace="ride").save()
print("Loaded into Cassandra")
spark.stop()
