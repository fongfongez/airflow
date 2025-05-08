from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 定義任務要執行的 Python 函式
def print_hello():
    print("Hello from Airflow!")

def print_goodbye():
    print("Goodbye from Airflow!")

# 預設參數
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 建立 DAG
with DAG(
    dag_id='sample_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task_hello = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )

    task_goodbye = PythonOperator(
        task_id='say_goodbye',
        python_callable=print_goodbye,
    )

    # 設定執行順序
    task_hello >> task_goodbye