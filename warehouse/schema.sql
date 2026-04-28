-- NYC Taxi Trips Data Warehouse
-- Database: nyc_taxi

CREATE TABLE IF NOT EXISTS taxi_trips (
    id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    ratecode_id FLOAT,
    store_and_fwd_flag TEXT,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    trip_duration_min FLOAT,
    price_per_mile FLOAT,
    pickup_hour INTEGER
);

-- Indexes للأداء
CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON taxi_trips(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_pickup_hour ON taxi_trips(pickup_hour);
CREATE INDEX IF NOT EXISTS idx_total_amount ON taxi_trips(total_amount);