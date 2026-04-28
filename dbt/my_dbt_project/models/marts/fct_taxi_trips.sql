with trips as (
    select * from {{ ref('stg_taxi_trips') }}
)

select
    pickup_hour,
    COUNT(*) as total_trips,
    ROUND(AVG(total_amount)::numeric, 2) as avg_amount,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(trip_duration_min)::numeric, 2) as avg_duration,
    ROUND(SUM(total_amount)::numeric, 2) as total_revenue
from trips
group by pickup_hour
order by pickup_hour