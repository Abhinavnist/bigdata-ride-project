import os, time, csv, statistics
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("bench_spark")
         .config("spark.sql.shuffle.partitions", "8").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

COLS = ["pickup_datetime", "PULocationID", "trip_miles", "base_passenger_fare"]
SIZES = {"1M": 1_000_000, "5M": 5_000_000, "20M": None}
BASE = "/data/bench"
RUNS = 3

full = spark.read.parquet("/data/raw/*.parquet").select(*COLS)
total = None
for name, n in SIZES.items():
    path = f"{BASE}/trips_{name}"
    if os.path.exists(path):
        continue
    if total is None:
        total = full.count()
    df = full if n is None else full.sample(False, n / total, seed=42)
    df.repartition(4).write.mode("overwrite").parquet(path)
    print("created", path, flush=True)

spark.read.option("header", True).csv("/data/lookup/taxi_zone_lookup.csv") \
     .createOrReplaceTempView("zones")

QUERIES = {
 "Q1_hourly_count": "SELECT hour(pickup_datetime) AS hr, COUNT(*) AS c FROM trips "
                    "GROUP BY hour(pickup_datetime) ORDER BY hr",
 "Q2_join_top_zones": "SELECT z.Zone, COUNT(*) AS c FROM trips t JOIN zones z "
                      "ON CAST(t.PULocationID AS STRING) = z.LocationID "
                      "GROUP BY z.Zone ORDER BY c DESC LIMIT 10",
 "Q3_zone_hour_dow_agg": "SELECT COUNT(*) FROM (SELECT PULocationID, hour(pickup_datetime) AS hr, "
                         "dayofweek(pickup_datetime) AS dow, COUNT(*) AS c, AVG(base_passenger_fare) AS f "
                         "FROM trips GROUP BY PULocationID, hour(pickup_datetime), "
                         "dayofweek(pickup_datetime)) x",
 "Q4_point_filter": "SELECT COUNT(*) FROM trips WHERE PULocationID = 132 "
                    "AND pickup_datetime >= '2025-01-31 18:00:00' "
                    "AND pickup_datetime < '2025-01-31 19:00:00'",
}

rows = []
for name in SIZES:
    spark.read.parquet(f"{BASE}/trips_{name}").createOrReplaceTempView("trips")
    n_rows = spark.table("trips").count()
    for qn, sql in QUERIES.items():
        ts = []
        for _ in range(RUNS):
            t0 = time.time()
            spark.sql(sql).collect()
            ts.append(time.time() - t0)
        rows.append((name, n_rows, qn, round(statistics.median(ts), 2), round(min(ts), 2)))
        print(f"{name:>4} {n_rows:>11,} {qn:22s} median={rows[-1][3]:>7}s best={rows[-1][4]:>7}s", flush=True)

with open(f"{BASE}/results_spark.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["size", "rows", "query", "median_s", "best_s"])
    w.writerows(rows)
print("saved", f"{BASE}/results_spark.csv")
spark.stop()
