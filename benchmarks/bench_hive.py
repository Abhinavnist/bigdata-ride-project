import csv, re, statistics, subprocess

SIZES = ["1M", "5M", "20M"]
RUNS = 3
QUERIES = {
 "Q1_hourly_count": "SELECT hour(pickup_datetime) AS hr, COUNT(*) AS c FROM {T} "
                    "GROUP BY hour(pickup_datetime) ORDER BY hr",
 "Q2_join_top_zones": "SELECT z.Zone, COUNT(*) AS c FROM {T} t JOIN zones z "
                      "ON CAST(t.PULocationID AS STRING) = z.LocationID "
                      "GROUP BY z.Zone ORDER BY c DESC LIMIT 10",
 "Q3_zone_hour_dow_agg": "SELECT COUNT(*) FROM (SELECT PULocationID, hour(pickup_datetime) AS hr, "
                         "dayofweek(pickup_datetime) AS dow, COUNT(*) AS c, AVG(base_passenger_fare) AS f "
                         "FROM {T} GROUP BY PULocationID, hour(pickup_datetime), "
                         "dayofweek(pickup_datetime)) x",
 "Q4_point_filter": "SELECT COUNT(*) FROM {T} WHERE PULocationID = 132 "
                    "AND pickup_datetime >= '2025-01-31 18:00:00' "
                    "AND pickup_datetime < '2025-01-31 19:00:00'",
}


def beeline(sql, db=""):
    cmd = ["docker", "exec", "hive", "beeline", "-u", f"jdbc:hive2://localhost:10000/{db}", "-e", sql]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return p.stdout + p.stderr


ddl = "USE ride; "
for s in SIZES:
    t = f"trips_{s.lower()}"
    ddl += (f"DROP TABLE IF EXISTS {t}; CREATE EXTERNAL TABLE {t} (pickup_datetime TIMESTAMP, "
            f"PULocationID INT, trip_miles DOUBLE, base_passenger_fare DOUBLE) "
            f"STORED AS PARQUET LOCATION 'file:///data/bench/trips_{s}/'; ")
out = beeline(ddl)
if "Error:" in out:
    raise SystemExit("Table creation failed:\n" + out[-1500:])
print("Hive tables ready", flush=True)


def timed(sql):
    out = beeline("SET hive.execution.engine=tez; SET tez.local.mode=true; " + sql, db="ride")
    ex = re.findall(r"Completed executing command\([^)]*\); Time taken: ([0-9.]+) seconds", out)
    co = re.findall(r"Completed compiling command\([^)]*\); Time taken: ([0-9.]+) seconds", out)
    if not ex or not co or "Error:" in out:
        raise RuntimeError(out[-800:])
    return float(ex[-1]) + float(co[-1])


rows = []
for s in SIZES:
    tbl = f"trips_{s.lower()}"
    for qn, sql in QUERIES.items():
        try:
            ts = [timed(sql.replace("{T}", tbl)) for _ in range(RUNS)]
            med, best = round(statistics.median(ts), 2), round(min(ts), 2)
        except Exception as e:
            print(f"{s} {qn} FAILED:\n{e}", flush=True)
            med = best = ""
        rows.append((s, qn, med, best))
        print(f"{s:>4} {qn:22s} median={med}s best={best}s", flush=True)

with open("data/bench/results_hive.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["size", "query", "median_s", "best_s"])
    w.writerows(rows)
print("saved data/bench/results_hive.csv")
