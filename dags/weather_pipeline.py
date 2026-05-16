from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'yash',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def fetch_weather_data():
    api_key = '293a9085f3fac634f5aeb5e60cfd39c2'
    
    cities = [
        'Amsterdam', 'Dubai', 'Berlin', 'Toronto', 'Mumbai',
        'Bangkok', 'London', 'Paris', 'Tokyo', 'Barcelona',
        'Singapore', 'Sydney', 'New York', 'Los Angeles', 'Rome',
        'Madrid', 'Vienna', 'Prague', 'Budapest', 'Istanbul'
    ]
    
    all_data = []
    for city in cities:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
        response = requests.get(url)
        data = response.json()
        all_data.append({
            'city': city,
            'temp': float(data['main']['temp']),
            'temp_min': float(data['main']['temp_min']),
            'temp_max': float(data['main']['temp_max']),
            'humidity': float(data['main']['humidity']),
            'weather': data['weather'][0]['description'],
            'wind_speed': float(data['wind']['speed']),
            'timestamp': data['dt']
        })
        print(f"Fetched {city}: {data['main']['temp']}°C")
    
    # Save to local file for next task to pick up
    with open('/opt/airflow/dags/weather_raw.json', 'w') as f:
        json.dump(all_data, f)
    
    print(f"Saved {len(all_data)} cities to weather_raw.json")

def run_spark_transformations():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    import json
    from datetime import datetime, date
    
    spark = SparkSession.builder \
        .appName("weather_pipeline") \
        .master("local[*]") \
        .getOrCreate()
    
    # Load raw data from previous task
    with open('/opt/airflow/dags/weather_raw.json', 'r') as f:
        raw_data = json.load(f)
    
    # Convert to Spark DataFrame
    rows = []
    for d in raw_data:
        rows.append({
            'city': d['city'],
            'temp': float(d['temp']),
            'temp_min': float(d['temp_min']),
            'temp_max': float(d['temp_max']),
            'humidity': float(d['humidity']),
            'weather': d['weather'],
            'wind_speed': float(d['wind_speed']),
            'date': datetime.fromtimestamp(d['timestamp']).strftime('%Y-%m-%d')
        })
    
    df = spark.createDataFrame(rows)
    
    # Transformation 1: Add avg_temp
    df = df.withColumn('avg_temp', F.round((F.col('temp_max') + F.col('temp_min')) / 2, 2))
    
    # Transformation 2: City rankings
    window_rank = Window.orderBy(F.col('temp').desc())
    df_ranked = df.withColumn('temp_rank', F.rank().over(window_rank)) \
                  .withColumn('travel_score', F.round(
                      (F.col('temp') * 0.4) + 
                      ((100 - F.col('humidity')) * 0.4) + 
                      ((30 - F.col('wind_speed')) * 0.2), 2))
    
    # Transformation 3: Rain probability
    df_rain = df_ranked.withColumn('is_rainy', 
        F.when(F.col('weather').contains('rain'), 1).otherwise(0))
    
    # Save results as CSV for BigQuery upload
    df_rain.toPandas().to_csv('/opt/airflow/dags/weather_processed.csv', index=False)
    
    print(f"Spark transformations complete. Processed {df_rain.count()} rows.")
    spark.stop()

def load_to_bigquery():
    from google.cloud import bigquery
    import pandas as pd
    
    client = bigquery.Client()
    
    df = pd.read_csv('/opt/airflow/dags/weather_processed.csv')
    
    table_id = 'de-weather-project-492917.weather_processed.daily_weather'
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    print(f"Loaded {len(df)} rows to {table_id}")

def trigger_dbt():
    import subprocess
    import os
    
    result = subprocess.run(
        ['dbt', 'build', '--project-dir', '/opt/airflow/dags/dbt_project', 
         '--profiles-dir', '/opt/airflow/dags/dbt_project'],
        capture_output=True,
        text=True
    )
    
    print("dbt stdout:", result.stdout)
    print("dbt stderr:", result.stderr)
    
    if result.returncode != 0:
        raise Exception(f"dbt build failed: {result.stderr}")
    
    print("dbt build completed successfully")

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Daily weather data pipeline for travel destinations',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    task_spark = PythonOperator(
        task_id='run_spark_transformations',
        python_callable=run_spark_transformations,
    )

    task_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
    )

    task_dbt = PythonOperator(
        task_id='trigger_dbt',
        python_callable=trigger_dbt,
    )

    task_fetch >> task_spark >> task_bigquery >> task_dbt