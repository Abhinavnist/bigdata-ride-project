import time
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("streaming_job")
         .config("spark.cassandra.connection.host", "cassandra")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("event_time", T.StringType()),
    T.StructField("zone_id", T.IntegerType()),
    T.StructField("miles", T.DoubleType()),
    T.StructField("fare", T.DoubleType())])

# Historical baseline: avg trips per zone, hour, weekday/weekend (training period only)
hist = spark.read.parquet("/data/processed/zone_hour")
baseline = (hist.filter((F.col("hour_ts") < F.lit("2025-01-25").cast("timestamp_ntz")) & (F.col("is_holiday") == 0))
                .groupBy(F.col("PULocationID").alias("zone_id"), "hour", "dow")
                .agg(F.avg("demand").alias("hourly_baseline")))

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "trips")
       .option("startingOffsets", "latest")
       .load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
             .select("e.*")
             .withColumn("event_ts", F.to_timestamp("event_time")))

agg = (events.withWatermark("event_ts", "10 minutes")
       .groupBy(F.window("event_ts", "5 minutes"), "zone_id")
       .agg(F.count("*").alias("trips"), F.round(F.avg("fare"), 2).alias("avg_fare")))

def write_batch(df, batch_id):
    if df.isEmpty():
        return
    out = (df.select("zone_id", F.col("window.start").alias("window_start"), "trips", "avg_fare")
             .withColumn("hour", F.hour("window_start"))
             .withColumn("dow", F.dayofweek("window_start"))
             .join(F.broadcast(baseline), ["zone_id", "hour", "dow"], "left")
             .withColumn("expected", F.round(F.coalesce(F.col("hourly_baseline"), F.lit(0.0)) / 12.0, 2))
             .withColumn("live_ratio", F.round(F.col("trips") / (F.col("expected") + 1.0), 2))
             .withColumn("surge_level",
                         F.when(F.col("live_ratio") >= 1.5, "HIGH")
                          .when(F.col("live_ratio") >= 1.2, "MEDIUM")
                          .otherwise("NORMAL"))
             .select("zone_id", "window_start", "trips", "avg_fare",
                     "expected", "live_ratio", "surge_level")
             .persist())
    n = out.count()
    fmt = "org.apache.spark.sql.cassandra"
    out.write.format(fmt).mode("append").options(table="live_zone_demand", keyspace="ride").save()
    out.write.format(fmt).mode("append").options(table="live_by_window", keyspace="ride").save()
    print(f"batch {batch_id}: wrote {n} zone-window rows", flush=True)
    out.unpersist()

query = (agg.writeStream.outputMode("update")
         .foreachBatch(write_batch)
         .option("checkpointLocation", f"/tmp/chk_{int(time.time())}")
         .trigger(processingTime="10 seconds")
         .start())
print("Streaming started. Waiting for data... (Ctrl+C to stop)", flush=True)
query.awaitTermination()
