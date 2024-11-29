from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


def start_dag (**context):
    print(f"Начало работы дага - start ")



def fetch_data(**context):
    
    # Подключение к базе данных через Airflow
    db_hook = PostgresHook(postgres_conn_id='postgres')  # ID подключения к БД в Airflow
    conn = db_hook.get_conn()
    cursor = conn.cursor()

    # Извлечение данных из таблицыvprofessors
    cursor.execute("select name, department from \"IAD\".professors;")
    rows = cursor.fetchall()

    # Закрытие соединения
    # cursor.close()
    # conn.close()

    # Запись данных в XCom
    context['ti'].xcom_push(key='professors_data', value=rows)
    print(f"Данные из professors извлечены: {rows}")
    return rows


def insert_data(**context):
     # Извлечение данных из XCom
    ti = context['ti']
    rows = ti.xcom_pull(key='professors_data', task_ids='select')
    print(f"Полученные данные из XCom: {rows}")

    if not rows:
        raise ValueError("Нет данных для вставки в таблицу professors0!")


    # Подключение к базе данных через Airflow
    db_hook = PostgresHook(postgres_conn_id='postgres')
    conn = db_hook.get_conn()
    cursor = conn.cursor()

    # Вставка данных в таблицу professors0
    for row in rows:
        cursor.execute(
            "INSERT INTO \"IAD\".professors0 (name, department) VALUES (%s, %s);", row
        )

    conn.commit()

    # Закрытие соединения
    # cursor.close()
    # conn.close()

    print(f"Данные добавлены в professors0.")

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
    select = PythonOperator(
        task_id='select',
        python_callable=fetch_data,
        provide_context=True  # Включить передачу context в функцию
    )
    insert = PythonOperator(
        task_id='insert',
        python_callable=insert_data,
        provide_context=True  # Включить передачу context в функцию
    )
    start>>select>>insert