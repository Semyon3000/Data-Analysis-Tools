from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Функция для выполнения SELECT запроса
def fetch_data_from_db():
    hook = PostgresHook(postgres_conn_id='postgres')  # Conn Id из настроек Airflow
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM \"IAD\".professors;")  # Ваш SQL запрос
    rows = cursor.fetchall()
    for row in rows:
        print(row)  # Либо можете обработать данные

# Создание DAG
with DAG(
    dag_id='select_query_dag',
    start_date=datetime(2024, 11, 1),
    schedule_interval=None,  # Запуск вручную
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_db
    )