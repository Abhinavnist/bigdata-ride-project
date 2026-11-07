CREATE KEYSPACE IF NOT EXISTS ride
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS ride.zone_demand (
  zone_id int,
  hour_ts timestamp,
  actual_demand double,
  predicted_demand double,
  surge_ratio double,
  surge_level text,
  PRIMARY KEY ((zone_id), hour_ts)
) WITH CLUSTERING ORDER BY (hour_ts DESC);

CREATE TABLE IF NOT EXISTS ride.zone_demand_by_hour (
  hour_ts timestamp,
  zone_id int,
  actual_demand double,
  predicted_demand double,
  surge_ratio double,
  surge_level text,
  PRIMARY KEY ((hour_ts), zone_id)
);
