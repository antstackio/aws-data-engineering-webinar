create or replace view under_five_trips as (
    select * from "yellow_taxi_datat" 
    where tpep_datetime/60 <= 5
    and tpep_datetime/60 > 0
    and trip_distance > 0
    and "passenger_count" > 0
)


select * from "under_five_trips" limit 100

select * from "under_five_trips" where "tpep_datetime" < 0

select * from "yellow_taxi_datat"  where "tpep_datetime" < 0

select "passenger_count", count(1) cnt from "under_five_trips" 
group by "passenger_count"
order by "passenger_count"

select avg("tip_amount") average, min("tip_amount") min, max("tip_amount") max from "under_five_trips"  

select 
    avg("trip_distance") average, 
    min("trip_distance") min, 
    max("trip_distance") max 
from "under_five_trips"  