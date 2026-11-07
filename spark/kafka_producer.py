import sys, json, time
from datetime import datetime
import pyarrow.dataset as ds
from kafka import KafkaProducer

START = datetime.fromisoformat(sys.argv[1])
END = datetime.fromisoformat(sys.argv[2])
SPEED = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0

dataset = ds.dataset("/data/raw", format="parquet")
flt = (ds.field("pickup_datetime") >= START) & (ds.field("pickup_datetime") < END)
tbl = dataset.to_table(
    columns=["pickup_datetime", "PULocationID", "trip_miles", "base_passenger_fare"],
    filter=flt)
df = tbl.to_pandas()
df = df[df.PULocationID.between(1, 263) & (df.trip_miles > 0) & (df.base_passenger_fare > 0)]
df = df.sort_values("pickup_datetime")
print(f"Trips to replay: {len(df)}  ({START} to {END}, speed x{SPEED})", flush=True)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20)

t0 = time.time()
for i, r in enumerate(df.itertuples(index=False), start=1):
    due = t0 + (r.pickup_datetime - START).total_seconds() / SPEED
    delay = due - time.time()
    if delay > 0.002:
        time.sleep(delay)
    producer.send("trips", {
        "event_time": r.pickup_datetime.strftime("%Y-%m-%d %H:%M:%S"),
        "zone_id": int(r.PULocationID),
        "miles": float(r.trip_miles),
        "fare": float(r.base_passenger_fare)})
    if i % 10000 == 0:
        print(f"sent {i}, simulated time {r.pickup_datetime}", flush=True)

producer.flush()
print("Done. Total sent:", len(df))
