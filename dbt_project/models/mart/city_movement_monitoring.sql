{{ config(materialized='table') }}

WITH train_data_with_delay AS (
    SELECT
        tm.TRAIN_ID,
        DATE(tm.ACTUAL_TIMESTAMP) AS Date,
        TIME(tm.ACTUAL_TIMESTAMP) AS ARRIVAL_TIME,
        sc.FULL_NAME AS CURRENT_STATION,
        TIME(tm.PLANNED_TIMESTAMP) AS PLANNED_TIME,
        tm.PLANNED_EVENT_TYPE,
        tm.DIRECTION_IND,
        tm.PLATFORM,
        COALESCE(sc_next.FULL_NAME, 'Unknown') AS NEXT_STATION,
        TIMESTAMPDIFF(minute, tm.PLANNED_TIMESTAMP, tm.ACTUAL_TIMESTAMP) AS DELAY_MINUTES,
        m.CITY AS CITY,
        tm.ACTUAL_TIMESTAMP,
        ROW_NUMBER() OVER (PARTITION BY tm.TRAIN_ID ORDER BY tm.ACTUAL_TIMESTAMP DESC) AS row_num
    FROM 
        {{ source('rail_data', 'train_movt_staging') }} tm
    LEFT JOIN 
        {{ source('rail_data', 'station_codes') }} sc
    ON 
        tm.LOC_STANOX = sc.STANOX_NO
    LEFT JOIN
        {{ source('rail_data', 'station_codes') }} sc_next
    ON
        tm.NEXT_REPORT_STANOX = sc_next.STANOX_NO
    LEFT JOIN
        {{ source('rail_data', 'mapping') }} m
    ON
        sc.ROUTE_DESCRIPTION = m.ROUTE_DESCRIPTION
),
latest_train_data AS (
    SELECT 
        td.*
    FROM 
        train_data_with_delay td
    WHERE 
        row_num = 1
    ORDER BY 
        td.ARRIVAL_TIME DESC
    LIMIT 100
),
city_weather_data AS (
    SELECT DISTINCT
        ltd.*,
        wd.TEMPERATURE
    FROM 
        latest_train_data ltd
    LEFT JOIN
        {{ source('rail_data', 'weather_data_staging') }} wd
    ON
        ltd.CITY = wd.CITY
)
SELECT 
    TRAIN_ID,
    Date,
    ARRIVAL_TIME,
    CURRENT_STATION,
    PLANNED_TIME,
    PLANNED_EVENT_TYPE,
    DIRECTION_IND,
    PLATFORM,
    NEXT_STATION,
    DELAY_MINUTES,
    CITY,
    TEMPERATURE
FROM 
    city_weather_data