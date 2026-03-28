from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from datetime import datetime

MYSQL_CONN_STR = "mysql+pymysql://user:pass@mysql:3306/astraworld_db"

def clean_customers():
    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("""
            update customers_raw 
            set dob = replace(dob, '/', '-')
            where dob like '%/%'
        """))
        conn.execute(text("""
            update customers_raw 
            set dob = str_to_date(dob, '%d-%m-%Y')
            where dob like '__-__-____'
        """))
        conn.execute(text("""
            update customers_raw
            set dob = null
            where dob = '1900-01-01'
        """))
        

def clean_sales():
    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("""
            update sales_raw 
            set price = replace(price, '.', '')
            where price like '%.%' 
            and price not like '%.00';
        """))
        conn.execute(text("""
            alter table sales_raw 
            modify price decimal(15,2)
        """))
        

def clean_after_sales():
    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        conn.execute(text("""
            update after_sales_raw
            set service_ticket = upper(service_ticket)
        """))
        

with DAG(
    dag_id="clean_exist_tbl",
    start_date=datetime(2026, 3, 27),
    schedule="@daily",
    catchup=False,
    tags=["demo"],
) as dag:

    task_clean_customers = PythonOperator(
        task_id="clean_customers",
        python_callable=clean_customers
    )

    task_clean_sales = PythonOperator(
        task_id="clean_sales",
        python_callable=clean_sales
    )

    task_clean_after_sales = PythonOperator(
        task_id="clean_after_sales",
        python_callable=clean_after_sales
    )

    task_clean_customers >> task_clean_sales >> task_clean_after_sales