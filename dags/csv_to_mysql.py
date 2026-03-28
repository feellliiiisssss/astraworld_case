from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

DATA_FOLDER = "/opt/airflow/data/"
TABLE_NAME = "customer_addresses"
MYSQL_CONN_STR = "mysql+pymysql://user:pass@mysql:3306/astraworld_db"

def load_csv_to_mysql(**context):
    execution_date = context["ds_nodash"]
    file_name = f"customer_addresses_{execution_date}.csv"
    file_path = os.path.join(DATA_FOLDER, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError("File not found")
    
    df = pd.read_csv(file_path)    
    if df.empty:
        raise ValueError("File is empty")
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["city"] = df["city"].str.strip().str.title()
    df["province"] = df["province"].str.strip().str.title()


    engine = create_engine(MYSQL_CONN_STR)
    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)

with DAG(
    dag_id="csv_to_mysql",
    start_date=datetime(2026, 3, 27),
    schedule="@daily",
    catchup=False,
    tags=["demo"],
) as dag:
    load_task = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_csv_to_mysql
    )