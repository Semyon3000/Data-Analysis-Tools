from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def start_dag (**context):
    print(f"Начало работы дага - start ")



def transfer_data(**context):
    
    # Подключение к базе данных через Airflow
    db_hook = PostgresHook(postgres_conn_id='postgres')  # ID подключения к БД в Airflow
    conn = db_hook.get_conn()
    cursor = conn.cursor()

    # Извлечение данных из таблицыvprofessors
    cursor.execute("select name, department from \"IAD\".professors;")
    rows = cursor.fetchall()

    # Вставка данных в таблицу professors0
    for row in rows:
        cursor.execute(
            "INSERT INTO \"IAD\".professors0 (name, department) VALUES (%s, %s);", row
        )

    conn.commit()
    print(f"Данные из professors добавлены в professors0.")
    

# Определение DAG
with DAG(
    dag_id='transfer_data_dag',
    start_date=datetime(2024, 11, 22),  # Укажите дату старта
    schedule_interval='*/15 * * * *',  # Запуск каждые 15 минуту
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=start_dag
    )
    transfer_task = PythonOperator(
        task_id='transfer_data_task',
        python_callable=transfer_data
    )
    start>>transfer_task