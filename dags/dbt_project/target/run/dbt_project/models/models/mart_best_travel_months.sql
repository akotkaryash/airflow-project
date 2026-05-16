

  create or replace view `de-weather-project-492917`.`weather_processed`.`mart_best_travel_months`
  OPTIONS()
  as WITH daily_weather AS (
    SELECT * FROM `de-weather-project-492917`.`weather_processed`.`daily_weather`
)

SELECT
    city,
    avg_temp,
    travel_score,
    is_rainy,
    humidity,
    wind_speed
FROM daily_weather
ORDER BY travel_score DESC;

