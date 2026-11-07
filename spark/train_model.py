import time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

SPLIT = "2025-01-25"   # train: before this date, test: on/after

spark = SparkSession.builder.appName("train_model").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/data/processed/zone_hour")
wx = (spark.read.option("header", True).csv("/data/weather/weather.csv")
      .withColumn("hour_ts", F.regexp_replace("time", "T", " ").cast("timestamp_ntz"))
      .select("hour_ts",
              F.col("temp_c").cast("double").alias("temp_c"),
              F.col("precip_mm").cast("double").alias("precip_mm"),
              F.col("snowfall_cm").cast("double").alias("snowfall_cm"),
              F.col("wind_kmh").cast("double").alias("wind_kmh")))

data = (df.join(wx, "hour_ts", "left")
          .withColumn("demand", F.col("demand").cast("double"))
          .dropna())   # first 7 days have no lag_168h, so they get dropped

features = ["lag_1h", "lag_24h", "lag_168h", "roll_3h",
            "hour", "dow", "is_weekend", "is_holiday",
            "temp_c", "precip_mm", "snowfall_cm", "wind_kmh"]

data = VectorAssembler(inputCols=features, outputCol="features").transform(data)

split_ts = F.lit(SPLIT).cast("timestamp_ntz")
train = data.filter(F.col("hour_ts") < split_ts).cache()
test = (data.filter(F.col("hour_ts") >= split_ts)
            .withColumn("baseline", F.col("lag_168h").cast("double")).cache())
print(f"Train rows: {train.count()}, Test rows: {test.count()}")

def score(pred_df, pred_col, metric):
    return RegressionEvaluator(labelCol="demand", predictionCol=pred_col,
                               metricName=metric).evaluate(pred_df)

results = [("Baseline (same hour last week)", 0.0,
            round(score(test, "baseline", "rmse"), 2),
            round(score(test, "baseline", "mae"), 2),
            round(score(test, "baseline", "r2"), 4))]

algos = {
    "LinearRegression": LinearRegression(featuresCol="features", labelCol="demand"),
    "RandomForest": RandomForestRegressor(featuresCol="features", labelCol="demand",
                                          numTrees=40, maxDepth=8, seed=42),
    "GBT": GBTRegressor(featuresCol="features", labelCol="demand",
                        maxIter=40, maxDepth=5, seed=42),
}

fitted = {}
for name, algo in algos.items():
    t0 = time.time()
    model = algo.fit(train)
    secs = round(time.time() - t0, 1)
    pred = model.transform(test)
    fitted[name] = (model, pred)
    results.append((name, secs,
                    round(score(pred, "prediction", "rmse"), 2),
                    round(score(pred, "prediction", "mae"), 2),
                    round(score(pred, "prediction", "r2"), 4)))

print("\n=== MODEL COMPARISON (test = last 7 days of January) ===")
print(f"{'Model':35s}{'Train(s)':>10s}{'RMSE':>10s}{'MAE':>10s}{'R2':>10s}")
for r in results:
    print(f"{r[0]:35s}{r[1]:>10}{r[2]:>10}{r[3]:>10}{r[4]:>10}")

best_name = min([r for r in results[1:]], key=lambda r: r[2])[0]
print("\nBest model:", best_name)

best_model, best_pred = fitted[best_name]
if best_name != "LinearRegression":
    print("\nFeature importance:")
    imps = sorted(zip(features, best_model.featureImportances.toArray()),
                  key=lambda x: -x[1])
    for f, v in imps:
        print(f"  {f:15s}{v:.3f}")

(best_pred.select("PULocationID", "hour_ts", "demand",
                  F.round("prediction", 1).alias("predicted_demand"))
          .coalesce(1).write.mode("overwrite").parquet("/data/processed/predictions"))
best_model.write().overwrite().save("/data/models/best_model")

(spark.createDataFrame(results, ["model", "train_seconds", "rmse", "mae", "r2"])
      .coalesce(1).write.mode("overwrite").option("header", True)
      .csv("/data/processed/metrics"))
print("\nSaved: predictions, best_model, metrics")
spark.stop()
