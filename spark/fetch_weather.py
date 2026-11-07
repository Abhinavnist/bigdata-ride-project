import sys, json, csv, os, urllib.request

START = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
END = sys.argv[2] if len(sys.argv) > 2 else "2025-01-31"

url = ("https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0060"
       f"&start_date={START}&end_date={END}"
       "&hourly=temperature_2m,precipitation,snowfall,wind_speed_10m"
       "&timezone=America%2FNew_York")

with urllib.request.urlopen(url, timeout=60) as r:
    data = json.load(r)

h = data["hourly"]
os.makedirs("/data/weather", exist_ok=True)
with open("/data/weather/weather.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["time", "temp_c", "precip_mm", "snowfall_cm", "wind_kmh"])
    for row in zip(h["time"], h["temperature_2m"], h["precipitation"],
                   h["snowfall"], h["wind_speed_10m"]):
        w.writerow(row)
print("weather rows:", len(h["time"]))
