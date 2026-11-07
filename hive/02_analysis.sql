USE ride;
SET hive.execution.engine=tez;
SET tez.local.mode=true;

SELECT COUNT(*) AS total_trips FROM trips;

SELECT hour(pickup_datetime) AS hr, COUNT(*) AS trips
FROM trips GROUP BY hour(pickup_datetime) ORDER BY hr;

SELECT z.Zone, z.Borough, COUNT(*) AS trips
FROM trips t JOIN zones z ON CAST(t.PULocationID AS STRING) = z.LocationID
GROUP BY z.Zone, z.Borough ORDER BY trips DESC LIMIT 10;

SELECT CASE WHEN dayofweek(pickup_datetime) IN (1,7) THEN 'weekend' ELSE 'weekday' END AS day_type,
       COUNT(*) AS trips, ROUND(AVG(trip_miles),2) AS avg_miles
FROM trips GROUP BY CASE WHEN dayofweek(pickup_datetime) IN (1,7) THEN 'weekend' ELSE 'weekday' END;
