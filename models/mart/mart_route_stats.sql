{# -- Description: This model aggregates flight data to provide statistics on routes between airports.
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
origin_airports AS (
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
FROM {{ref('mart_route_stats')}}
JOIN origin_airports
USING (origin_airport_code)
JOIN dest_airports
USING (dest_airport_code)
ORDER BY origin_city, dest_city
 #}


WITH route_stats AS (
    SELECT
        origin AS origin_faa,
        dest AS dest_faa,
        COUNT(*) AS total_flights, -- total flights on this route
        COUNT(DISTINCT tail_number) AS unique_airplanes, -- unique airplanes on this route
        COUNT(DISTINCT airline) AS unique_airlines, -- unique airlines on this route
        AVG(actual_elapsed_time) AS avg_actual_elapsed_time, -- average actual elapsed time
        AVG(arr_delay) AS avg_arrival_delay, -- average arrival delay
        MAX(arr_delay) AS max_arrival_delay, -- maximum delay on arrival
        MIN(arr_delay) AS min_arrival_delay, -- minimum delay on arrival
        SUM(cancelled) AS total_cancelled, -- total number of cancelled flights
        SUM(diverted) AS total_diverted -- total number of diverted flights
    FROM {{ref('prep_flights')}}
    GROUP BY origin, dest
),
-- Add city, country, and name for both origin and destination airports
origin_airport AS (
    SELECT faa AS origin_faa, city AS origin_city, country AS origin_country, name AS origin_name
    FROM {{ref('prep_airports')}}
),
destination_airport AS (
    SELECT faa AS dest_faa, city AS dest_city, country AS dest_country, name AS dest_name
    FROM {{ref('prep_airports')}}
)
-- Join the route statistics with the airport information
SELECT
    rs.origin_faa,
    oa.origin_city,
    oa.origin_country,
    oa.origin_name,
    rs.dest_faa,
    da.dest_city,
    da.dest_country,
    da.dest_name,
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_actual_elapsed_time,
    rs.avg_arrival_delay,
    rs.max_arrival_delay,
    rs.min_arrival_delay,
    rs.total_cancelled,
    rs.total_diverted
FROM route_stats rs
JOIN origin_airport oa ON rs.origin_faa = oa.origin_faa
JOIN destination_airport da ON rs.dest_faa = da.dest_faa
ORDER BY rs.origin_faa, rs.dest_faa


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