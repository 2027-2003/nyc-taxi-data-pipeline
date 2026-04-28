with source as (
    select * from {{ source('nyc_taxi', 'taxi_trips') }}
),

staged as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        trip_duration_min,
        price_per_mile,
        pickup_hour
    from source
    where total_amount > 0
      and trip_distance > 0
      and passenger_count > 0
)

select * from staged