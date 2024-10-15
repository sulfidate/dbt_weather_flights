-- Description: This model aggregates flight data to provide statistics on routes between airports.
WITH route_stats AS (
    SELECT origin AS origin_airport_code
            , dest AS dest_airport_code
            , COUNT(origin) AS total_flights
            , COUNT(DISTINCT tail_number) AS unique_airplanes
            , COUNT(DISTINCT airline) AS unique_airlines
            , AVG(actual_elapsed_time) AS avg_actual_elapsed_time
            , AVG(arr_delay) AS avg_arrival_delay
            , MAX(arr_delay) AS max_arrival_delay
            , MIN(arr_delay) AS min_arrival_delay
            , SUM(cancelled) AS total_cancelled
            , SUM(diverted) AS total_diverted
    FROM {{ref('prep_flights')}}
    GROUP BY origin, dest
),
oringin_airports AS (
    SELECT faa AS origin_airport_code
            ,city AS origin_city
            ,country AS origin_country
            ,name AS origin_name
    FROM {{ref('prep_airports')}}
),
dest_airports AS (
    SELECT faa AS dest_airport_code
            ,city AS dest_city
            ,country AS dest_country
            ,name AS dest_name
    FROM {{ref('prep_airports')}}
)
SELECT *
FROM {{ref('route_stats')}}
JOIN oringin_airports
USING (origin_airport_code)
JOIN dest_airports
USING (dest_airport_code)
ORDER BY origin_city, dest_city




-- origin airport code
-- destination airport code
--total flights on this route
-- unique airplanes
-- unique airlines
-- on average what is the actual elapsed time
-- on average what is the delay on arrival
-- what was the max delay?
-- what was the min delay?
-- total number of cancelled
-- total number of diverted
-- add city, country and name for both, origin and destination, airports