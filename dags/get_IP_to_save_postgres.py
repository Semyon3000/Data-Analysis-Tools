from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

default_args = {
    'start_date': datetime(2024, 11, 22),
}

dag = DAG(
    'get_IP_to_save_postgres',
    default_args=default_args,
    schedule_interval='@hourly',
)


# Функция для выполнения запроса к API
def fetch_weather_data():
    url = 'http://api.weatherstack.com/current'
    params = {
        'access_key': 'ab95de45fc66b434ec704f9dd0149bbd',
        'query': 'Moscow',
        'units': 'm'
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        if "current" in data:  # Проверяем, что ключ "current" присутствует
            return data
        else:
            raise ValueError("Unexpected API response structure: 'current' data missing.")
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


# Функция для парсинга JSON и создания SQL-запроса
def parse_and_prepare_sql(**context):
    response = context['ti'].xcom_pull(task_ids='fetch_weather_data')

    data = {
        "request_type": response["request"]["type"],
        "query": response["request"]["query"],
        "language": response["request"]["language"],
        "unit": response["request"]["unit"],
        "location_name": response["location"]["name"],
        "location_country": response["location"]["country"],
        "location_region": response["location"]["region"],
        "location_lat": response["location"]["lat"],
        "location_lon": response["location"]["lon"],
        "location_timezone_id": response["location"]["timezone_id"],
        "location_localtime": response["location"]["localtime"],
        "location_localtime_epoch": response["location"]["localtime_epoch"],
        "location_utc_offset": response["location"]["utc_offset"],
        "observation_time": response["current"]["observation_time"],
        "temperature": response["current"]["temperature"],
        "weather_code": response["current"]["weather_code"],
        "weather_icon": response["current"]["weather_icons"][0],
        "weather_description": response["current"]["weather_descriptions"][0],
        "wind_speed": response["current"]["wind_speed"],
        "wind_degree": response["current"]["wind_degree"],
        "wind_dir": response["current"]["wind_dir"],
        "pressure": response["current"]["pressure"],
        "precip": response["current"]["precip"],
        "humidity": response["current"]["humidity"],
        "cloudcover": response["current"]["cloudcover"],
        "feelslike": response["current"]["feelslike"],
        "uv_index": response["current"]["uv_index"],
        "visibility": response["current"]["visibility"],
        "is_day": True if response["current"]["is_day"] == 'yes' else False
    }

    # SQL-запрос для вставки данных
    insert_query = f"""
    INSERT INTO "IAD".weather_data (
        request_type, query, language, unit, location_name, location_country, location_region,
        location_lat, location_lon, location_timezone_id, location_localtime, location_localtime_epoch,
        location_utc_offset, observation_time, temperature, weather_code, weather_icon,
        weather_description, wind_speed, wind_degree, wind_dir, pressure, precip, humidity,
        cloudcover, feelslike, uv_index, visibility, is_day
    ) VALUES (
        '{data["request_type"]}', '{data["query"]}', '{data["language"]}', '{data["unit"]}', 
        '{data["location_name"]}', '{data["location_country"]}', '{data["location_region"]}',
        {data["location_lat"]}, {data["location_lon"]}, '{data["location_timezone_id"]}', 
        '{data["location_localtime"]}', {data["location_localtime_epoch"]}, {data["location_utc_offset"]},
        '{data["observation_time"]}', {data["temperature"]}, {data["weather_code"]}, '{data["weather_icon"]}',
        '{data["weather_description"]}', {data["wind_speed"]}, {data["wind_degree"]}, '{data["wind_dir"]}', 
        {data["pressure"]}, {data["precip"]}, {data["humidity"]}, {data["cloudcover"]}, 
        {data["feelslike"]}, {data["uv_index"]}, {data["visibility"]}, {data["is_day"]}
    );
    """
    return insert_query


# Задача для получения данных
fetch_weather_data_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

# Задача для подготовки SQL
generate_sql_task = PythonOperator(
    task_id='generate_sql',
    python_callable=parse_and_prepare_sql,
    provide_context=True,
    dag=dag,
)

# Задача для вставки данных
insert_data_task = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',
    sql="{{ task_instance.xcom_pull(task_ids='generate_sql') }}",
    dag=dag,
)

# Последовательность выполнения задач
fetch_weather_data_task >> generate_sql_task >> insert_data_task
