import random, statistics, time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

s = Cluster(["cassandra"]).connect("ride")
start = datetime(2025, 1, 25)
point = s.prepare("SELECT actual_demand, predicted_demand, surge_level "
                  "FROM zone_demand WHERE zone_id=? AND hour_ts=?")
series = s.prepare("SELECT hour_ts, actual_demand, predicted_demand FROM zone_demand WHERE zone_id=?")
hour_slice = s.prepare("SELECT zone_id, predicted_demand, surge_level FROM zone_demand_by_hour WHERE hour_ts=?")

rz = lambda: random.randint(1, 263)
rh = lambda: start + timedelta(hours=random.randrange(168))


def bench(name, make_args, stmt, n=500):
    for _ in range(50):
        s.execute(stmt, make_args())
    t = []
    for _ in range(n):
        a = make_args()
        t0 = time.perf_counter()
        rows = list(s.execute(stmt, a))
        t.append((time.perf_counter() - t0) * 1000)
    t.sort()
    print(f"{name:36s} mean={statistics.mean(t):6.2f} ms  p50={t[n // 2]:6.2f}  "
          f"p95={t[int(n * 0.95)]:6.2f}  p99={t[int(n * 0.99)]:6.2f}  rows={len(rows)}")


bench("point lookup (zone + hour)", lambda: (rz(), rh()), point)
bench("zone time series (168 rows)", lambda: (rz(),), series)
bench("all zones for one hour (263 rows)", lambda: (rh(),), hour_slice)
