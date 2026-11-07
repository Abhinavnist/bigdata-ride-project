CREATE TABLE IF NOT EXISTS ride.live_zone_demand (
  zone_id int,
  window_start timestamp,
  trips bigint,
  avg_fare double,
  expected double,
  live_ratio double,
  surge_level text,
  PRIMARY KEY ((zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS ride.live_by_window (
  window_start timestamp,
  zone_id int,
  trips bigint,
  avg_fare double,
  expected double,
  live_ratio double,
  surge_level text,
  PRIMARY KEY ((window_start), zone_id)
);
