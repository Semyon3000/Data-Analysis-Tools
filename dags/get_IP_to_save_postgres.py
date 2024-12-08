from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.decorators import task_group  # Импорт декоратора task_group
import requests
import json

# Функция для выполнения запроса к API
def fetch_weather_data():
    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f"Fetched data: {json.dumps(data[:5], indent=2)}")  # Выводим первые 5 записей для проверки
        return data  # Данные будут сохранены в XCom
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


# Функция для загрузки данных в таблицу PostgreSQL
def save_data_to_postgres(ti):
    # Получаем данные из XCom
    data = ti.xcom_pull(task_ids='fetch_data_task')
    if not data:
        raise ValueError("No data fetched from API to save in PostgreSQL")

    # Подключение к PostgreSQL через PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres')  # ID подключения к БД в Airflow
    insert_query = """
        INSERT INTO \"IAD\".posts (user_id, post_id, title, body)
        VALUES (%s, %s, %s, %s);
    """

    # Вставка записей в базу
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for record in data:
        cursor.execute(insert_query, (
            record['userId'], record['id'], record['title'], record['body']
        ))
    conn.commit()
    print("Data successfully saved to PostgreSQL")
    

# Создание DAG
with DAG(
    dag_id='get_IP_to_save_postgres',
    start_date=datetime(2024, 11, 1),
    schedule_interval=None,  # Запуск вручную
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_weather_data
    )
    save_data = PythonOperator(
        task_id='save_data_to_postgres_task',
        python_callable=save_data_to_postgres
    )
    # Упорядочивание задач
    fetch_data >> save_data
