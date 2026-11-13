import os, json, time
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from cassandra.cluster import Cluster

CASS_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
SHAPE = "/data/zones/shapefile/taxi_zones/taxi_zones.shp"
LOOKUP = "/data/zones/taxi_zone_lookup.csv"
COLORS = {"HIGH": "#d7301f", "MEDIUM": "#fdae35", "NORMAL": "#41ab5d", "LOW VOLUME": "#d0d0d0"}
ORDER = ["HIGH", "MEDIUM", "NORMAL", "LOW VOLUME"]
COLS = ["zone_id", "now", "usual", "ratio", "level"]

st.set_page_config(page_title="NYC Ride Demand & Surge", layout="wide")


@st.cache_resource
def get_session():
    return Cluster([CASS_HOST], port=9042).connect("ride")


@st.cache_resource
def load_geo():
    gdf = gpd.read_file(SHAPE).to_crs(4326)
    gdf["LocationID"] = gdf["LocationID"].astype(int)
    gdf = gdf.dissolve(by="LocationID", as_index=False)
    gdf["geometry"] = gdf.geometry.simplify(0.0003)
    return json.loads(gdf[["LocationID", "geometry"]].to_json())


@st.cache_data
def load_zones():
    z = pd.read_csv(LOOKUP).rename(columns={"LocationID": "zone_id"})
    return z[["zone_id", "Borough", "Zone"]]


def q(cql, params=None):
    rows = get_session().execute(cql, params or [])
    return pd.DataFrame(list(rows))


@st.cache_data(ttl=5)
def list_times(table, col):
    rows = get_session().execute(f"SELECT DISTINCT {col} FROM {table}")
    return sorted(r[0] for r in rows)


try:
    get_session()
except Exception as e:
    st.error(f"Cannot connect to Cassandra at {CASS_HOST}:9042. Is the container running? ({e})")
    st.stop()

zones = load_zones()
geo = load_geo()

st.title("NYC Ride Demand & Surge Dashboard")
st.caption("Big Data pipeline: Hive + Spark MLlib + Kafka + Spark Streaming + Cassandra. "
           "Times are New York local time.")

mode = st.sidebar.radio("Mode", ["Predicted (ML model)", "Live (Kafka stream)"])
live = mode.startswith("Live")
bg = st.sidebar.selectbox("Map background", ["white-bg", "open-street-map"])

if live:
    table, tcol = "live_by_window", "window_start"
    st.sidebar.caption("Each window = 5 minutes of trips. Surge = trips / usual trips.")
else:
    table, tcol = "zone_demand_by_hour", "hour_ts"
    st.sidebar.caption("Each point = 1 hour. Surge = predicted demand / usual demand.")

times = list_times(table, tcol)
if not times:
    st.warning("No data in Cassandra for this mode yet. Run the loader / streaming job first.")
    st.stop()

fmt = lambda x: x.strftime("%a %d %b, %H:%M")
follow = False
auto = False
if live:
    follow = st.sidebar.checkbox("Follow latest window", value=True)
    auto = st.sidebar.checkbox("Auto-refresh every 10 s", value=True)

if live and follow:
    # latest window may still be filling, so show the one before it
    t = times[-2] if len(times) > 1 else times[-1]
    st.sidebar.write("Showing:", fmt(t))
else:
    t = st.sidebar.select_slider("Time", options=times, value=times[-1], format_func=fmt)

min_vol = st.sidebar.number_input(
    "Grey out zones with usual volume below", min_value=0.0,
    value=3.0 if live else 10.0, step=1.0,
    help="Tiny zones give noisy ratios, so they are not coloured.")

if live:
    df = q("SELECT zone_id, trips, expected, live_ratio, surge_level "
           "FROM live_by_window WHERE window_start = %s", [t])
    df = df.rename(columns={"trips": "now", "expected": "usual",
                            "live_ratio": "ratio", "surge_level": "level"})
    now_label, usual_label = "Trips now", "Usual trips"
else:
    df = q("SELECT zone_id, actual_demand, predicted_demand, surge_ratio, surge_level "
           "FROM zone_demand_by_hour WHERE hour_ts = %s", [t])
    df = df.rename(columns={"predicted_demand": "now", "actual_demand": "usual",
                            "surge_ratio": "ratio", "surge_level": "level"})
    now_label, usual_label = "Predicted demand", "Actual demand"

if df.empty:
    df = pd.DataFrame(columns=COLS)
df["vol"] = df["usual"] if live else df["now"]

base = zones.merge(df, on="zone_id", how="left")
for c in ["now", "usual", "ratio", "vol"]:
    base[c] = pd.to_numeric(base[c], errors="coerce")
base["level"] = base["level"].fillna("LOW VOLUME")
base.loc[base["vol"].fillna(0) < min_vol, "level"] = "LOW VOLUME"

busiest = base.sort_values("now", ascending=False).head(1)
c1, c2, c3, c4 = st.columns(4)
c1.metric(now_label + " (all zones)", f"{int(base['now'].fillna(0).sum()):,}")
c2.metric("HIGH surge zones", int((base["level"] == "HIGH").sum()))
c3.metric("MEDIUM surge zones", int((base["level"] == "MEDIUM").sum()))
c4.metric("Busiest zone",
          busiest["Zone"].iloc[0] if len(busiest) and pd.notna(busiest["now"].iloc[0]) else "-")

left, right = st.columns([3, 2])

with left:
    fig = px.choropleth_mapbox(
        base, geojson=geo, locations="zone_id", featureidkey="properties.LocationID",
        color="level", color_discrete_map=COLORS, category_orders={"level": ORDER},
        hover_name="Zone",
        hover_data={"zone_id": False, "level": True, "Borough": True,
                    "now": ":.0f", "usual": ":.1f", "ratio": ":.2f"},
        labels={"now": now_label, "usual": usual_label, "ratio": "Surge ratio", "level": "Level"},
        mapbox_style=bg, center={"lat": 40.72, "lon": -73.95},
        zoom=9.4, opacity=0.75)
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=600, legend_title_text="Surge level")
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Top 10 surge zones")
    top = (base[base["level"] != "LOW VOLUME"]
           .sort_values("ratio", ascending=False).head(10)
           [["Zone", "Borough", "now", "usual", "ratio", "level"]]
           .rename(columns={"now": now_label, "usual": usual_label,
                            "ratio": "Surge ratio", "level": "Level"}))
    st.dataframe(top, hide_index=True, use_container_width=True)

    names = zones.sort_values("Zone").reset_index(drop=True)
    hit = names.index[names["zone_id"] == 132]
    pos = int(hit[0]) if len(hit) else 0
    zname = st.selectbox("Zone trend", names["Zone"].tolist(), index=pos)
    zid = int(names.loc[names["Zone"] == zname, "zone_id"].iloc[0])

    if live:
        ts = q("SELECT window_start, trips, expected FROM live_zone_demand WHERE zone_id = %s", [zid])
        xcol, a, b, na, nb = "window_start", "trips", "expected", "Trips", "Usual"
    else:
        ts = q("SELECT hour_ts, actual_demand, predicted_demand FROM zone_demand WHERE zone_id = %s", [zid])
        xcol, a, b, na, nb = "hour_ts", "actual_demand", "predicted_demand", "Actual", "Predicted"

    if len(ts):
        ts = ts.sort_values(xcol)
        f2 = go.Figure()
        f2.add_trace(go.Scatter(x=ts[xcol], y=ts[a], name=na, mode="lines"))
        f2.add_trace(go.Scatter(x=ts[xcol], y=ts[b], name=nb, mode="lines", line=dict(dash="dash")))
        f2.add_shape(type="line", x0=t, x1=t, y0=0, y1=1, yref="paper",
                     line=dict(dash="dot", color="gray"))
        f2.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                         legend=dict(orientation="h", y=1.15))
        st.plotly_chart(f2, use_container_width=True)
    else:
        st.info("No data for this zone yet.")

if live and auto:
    time.sleep(10)
    st.rerun()