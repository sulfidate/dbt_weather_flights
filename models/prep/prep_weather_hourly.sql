{# WITH hourly_data AS (
    SELECT * 
    FROM staging_weather_hourly -- {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *   
    , timestamp::DATE AS date --only time as TIME data type
    , timestamp::TIME AS time --only time as TIME data type
    , TO_CHAR(timestamp, 'HH24:MI') AS hour --only time as TEXT data type
    , TO_CHAR(timestamp, 'FMmonth') AS month_name -- month name as a text
    , EXTRACT(DOW FROM timestamp) AS weekday -- day of the week as a number
    , DATE_PART('day', timestamp) AS date_day -- hour of the day as a number
    , DATE_PART('month', timestamp) AS date_month -- month of the year as a number
    , DATE_PART('year', timestamp) AS date_year -- year as a number
    -- , DATE_PART('cw', timestamp) AS cw -- calendar week as a number
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
            WHEN time BETWEEN '00:00:00' AND '05:59:59' THEN 'night'
            WHEN time BETWEEN '06:00:00' AND '17:59:59' THEN 'day'
            WHEN time BETWEEN '18:00:00' AND '23:59:59' THEN 'evening'
            ELSE 'unknown'
        END) AS day_time
    FROM add_features
)
SELECT * FROM add_more_features #}
WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date -- Extract only the date
        , timestamp::TIME AS time -- Extract only the time (hours:minutes:seconds)
        , TO_CHAR(timestamp, 'HH24:MI') AS hour -- Extract time (hours:minutes) as text
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- Extract month name as text
        , TO_CHAR(timestamp, 'Day') AS weekday -- Extract weekday name as text
        , DATE_PART('day', timestamp) AS date_day -- Extract the day of the month
        , DATE_PART('month', timestamp) AS date_month -- Extract the numeric month
        , DATE_PART('year', timestamp) AS date_year -- Extract the year
        , TO_CHAR(timestamp, 'IW') AS cw -- Extract the ISO calendar week
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        ,(CASE 
            WHEN time BETWEEN '00:00:00' AND '06:00:00' THEN 'night'
            WHEN time BETWEEN '06:01:00' AND '18:00:00' THEN 'day'
            WHEN time BETWEEN '18:01:00' AND '23:59:59' THEN 'evening'
        END) AS day_part -- Define day part based on the time of day
    FROM add_features
)

SELECT *
FROM add_more_features