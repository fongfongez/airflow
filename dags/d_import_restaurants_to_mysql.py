from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pymysql
import csv
import os

MYSQL_CONFIG = {
    'host': '35.194.188.197',
    'port': 3306,
    'user': 'user',
    'password': 'password',
    'database': 'testDB',
    'charset': 'utf8mb4'
}

CSV_PATH = './data/Clean/restaurants_Cleaned.csv'
TABLE_NAME = 'testTABLE'

def import_restaurant_csv():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    conn = pymysql.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    with open(CSV_PATH, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
        print("CSV欄位名稱:", reader.fieldnames)
        for row in reader:
            print("資料行欄位:", row.keys())
        # 確保可以正確存取 'nm_name'
            if 'nm_name' in row:
                print(row['nm_name'])
            else:
                print("找不到 'nm_name' 欄位")
            # 去除資料行的欄位名稱空格
            row = {key.strip(): value for key, value in row.items()}
            
            # 嘗試存取 'nm_name'
            print(row['nm_name'])  # 應該不會再出現 KeyError

            cursor.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                (nm_name, st_name, nm_city, st_address, st_url, st_score, st_price, st_total_com, st_tag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    nm_name=VALUES(nm_name),
                    st_name=VALUES(st_name),
                    nm_city=VALUES(nm_city),
                    st_address=VALUES(st_address),
                    st_score=VALUES(st_score),
                    st_price=VALUES(st_price),
                    st_total_com=VALUES(st_total_com),
                    st_tag=VALUES(st_tag),
                    st_url=VALUES(st_url)
                """,
                (
                    row['nm_name'],
                    row['st_name'],
                    row['nm_city'],
                    row['st_address'],
                    row['st_url'],
                    row['st_score'],
                    row['st_price'],
                    row['st_total_com'],
                    row['st_tag']
                )
            )

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='d_import_restaurants_to_mysql',
    start_date=datetime(2025, 5, 7),
    schedule_interval=None,
    catchup=False,
    tags=['mysql', 'csv', 'import']
) as dag:

    import_task = PythonOperator(
        task_id='import_restaurant_csv_task',
        python_callable=import_restaurant_csv
    )