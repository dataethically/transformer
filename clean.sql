START TRANSACTION;

-- First insert location data if it doesn't exist
INSERT INTO locations (
    name, 
    country, 
    longitude, 
    latitude, 
    timezone, 
    openweather_id
)
SELECT
    json_extract_path_text(raw_json, 'name'),
    json_extract_path_text(raw_json, 'sys', 'country'),
    (json_extract_path_text(raw_json, 'coord', 'lon'))::DECIMAL,
    (json_extract_path_text(raw_json, 'coord', 'lat'))::DECIMAL,
    (json_extract_path_text(raw_json, 'timezone'))::INTEGER,
    (json_extract_path_text(raw_json, 'id'))::INTEGER
FROM 
    raw_data_table
WHERE 
    NOT EXISTS (
        SELECT 1 FROM locations 
        WHERE openweather_id = (json_extract_path_text(raw_json, 'id'))::INTEGER
    )
ON CONFLICT (openweather_id) DO NOTHING;

-- Insert weather conditions if they don't exist
-- Extract first weather condition from the weather array (index 0)
INSERT INTO weather_conditions (
    condition_id,
    main_category,
    description,
    icon
)
SELECT
    (json_extract_path_text(weather_element, 'id'))::INTEGER,
    json_extract_path_text(weather_element, 'main'),
    json_extract_path_text(weather_element, 'description'),
    json_extract_path_text(weather_element, 'icon')
FROM (
    SELECT json_array_elements(raw_json->'weather') AS weather_element
    FROM raw_data_table
) AS weather_data
ON CONFLICT (condition_id) DO UPDATE SET
    main_category = EXCLUDED.main_category,
    description = EXCLUDED.description,
    icon = EXCLUDED.icon;

-- Insert main metrics
INSERT INTO main_metrics (
    temperature,
    feels_like,
    temp_min,
    temp_max,
    pressure,
    humidity,
    sea_level,
    ground_level
)
SELECT
    (json_extract_path_text(raw_json, 'main', 'temp'))::DECIMAL,
    (json_extract_path_text(raw_json, 'main', 'feels_like'))::DECIMAL,
    (json_extract_path_text(raw_json, 'main', 'temp_min'))::DECIMAL,
    (json_extract_path_text(raw_json, 'main', 'temp_max'))::DECIMAL,
    (json_extract_path_text(raw_json, 'main', 'pressure'))::INTEGER,
    (json_extract_path_text(raw_json, 'main', 'humidity'))::INTEGER,
    (json_extract_path_text(raw_json, 'main', 'sea_level'))::INTEGER,
    (json_extract_path_text(raw_json, 'main', 'grnd_level'))::INTEGER
FROM
    raw_data_table
RETURNING main_id;

-- Insert wind metrics
INSERT INTO wind_metrics (
    speed,
    direction,
    gust
)
SELECT
    (json_extract_path_text(raw_json, 'wind', 'speed'))::DECIMAL,
    (json_extract_path_text(raw_json, 'wind', 'deg'))::INTEGER,
    (json_extract_path_text(raw_json, 'wind', 'gust'))::DECIMAL
FROM
    raw_data_table
RETURNING wind_id;

-- Finally insert the weather data linking everything
WITH location_data AS (
    SELECT 
        location_id, 
        json_extract_path_text(raw_json, 'id') AS openweather_id
    FROM 
        locations, raw_data_table
    WHERE 
        openweather_id = (json_extract_path_text(raw_json, 'id'))::INTEGER
),
condition_data AS (
    SELECT 
        condition_id,
        json_extract_path_text(weather_element, 'id') AS condition_openweather_id
    FROM 
        weather_conditions,
        (SELECT json_array_elements(raw_json->'weather') AS weather_element FROM raw_data_table) AS weather_data
    WHERE 
        condition_id = (json_extract_path_text(weather_element, 'id'))::INTEGER
),
main_data AS (
    SELECT 
        main_id
    FROM 
        main_metrics
    ORDER BY 
        main_id DESC
    LIMIT 1
),
wind_data AS (
    SELECT 
        wind_id
    FROM 
        wind_metrics
    ORDER BY 
        wind_id DESC
    LIMIT 1
)
INSERT INTO weather_data (
    location_id,
    condition_id,
    main_id,
    wind_id,
    clouds_percentage,
    visibility,
    observation_time,
    sunrise_time,
    sunset_time
)
SELECT
    l.location_id,
    c.condition_id,
    m.main_id,
    w.wind_id,
    (json_extract_path_text(raw_json, 'clouds', 'all'))::INTEGER,
    (json_extract_path_text(raw_json, 'visibility'))::INTEGER,
    to_timestamp((json_extract_path_text(raw_json, 'dt'))::BIGINT),
    to_timestamp((json_extract_path_text(raw_json, 'sys', 'sunrise'))::BIGINT),
    to_timestamp((json_extract_path_text(raw_json, 'sys', 'sunset'))::BIGINT)
FROM
    raw_data_table,
    location_data l,
    condition_data c,
    main_data m,
    wind_data w
ON CONFLICT (location_id, observation_time) DO NOTHING;

-- Clean up the raw data (uncomment when you're sure the above works)
-- DELETE FROM raw_data_table;


COMMIT;
