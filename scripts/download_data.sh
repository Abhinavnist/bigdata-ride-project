#!/usr/bin/env bash
set -e
source .env
BASE="https://d37ci6vzurychx.cloudfront.net"

mkdir -p data/raw data/zones

echo "Downloading HVFHV trips for ${DATA_MONTH}..."
curl -L -o "data/raw/fhvhv_tripdata_${DATA_MONTH}.parquet" \
  "${BASE}/trip-data/fhvhv_tripdata_${DATA_MONTH}.parquet"

echo "Downloading zone lookup and shapefile..."
curl -L -o data/zones/taxi_zone_lookup.csv "${BASE}/misc/taxi_zone_lookup.csv"
curl -L -o data/zones/taxi_zones.zip "${BASE}/misc/taxi_zones.zip"
unzip -o data/zones/taxi_zones.zip -d data/zones/shapefile

echo "Done."
ls -lh data/raw data/zones
