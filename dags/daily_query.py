from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="daily_query",
    start_date=datetime(2026, 3, 27),
    schedule="@daily",
    catchup=False,
    tags=["demo"],
) as dag:

    task_a = SQLExecuteQueryOperator(
        task_id="task_a",
        conn_id="mysql_default",
        sql="""
        drop table if exists sales_summary;
        create table sales_summary as
            select
            date_format(invoice_date, '%Y-%m') as periode,
            case when price between 100000000 and 250000000 then 'LOW'
            when price > 400000000 then 'HIGH'
            else 'MEDIUM' end as class,
            model,
            sum(price) as total
            from sales_raw
            group by date_format(invoice_date, '%Y-%m'), class, model
        """
    )

    task_b = SQLExecuteQueryOperator(
        task_id="task_b",
        conn_id="mysql_default",
        sql="""
        drop table if exists after_sales_summary;
        create table after_sales_summary as
            select 
            periode, vin, customer_name, address, count_service,
            case when count_service < 5 then 'LOW'
            when count_service > 10 then 'HIGH'
            else 'MED' end as priority
            from (
            select
            date_format(a.service_date, '%Y') as periode,
            a.vin,
            b.name as customer_name,
            c.address,
            count(a.service_ticket) as count_service
            from after_sales_raw a
            left join customers_raw b on a.customer_id = b.id
            left join customer_addresses c on b.id = c.customer_id
            group by date_format(a.service_date, '%Y'), a.vin, b.name, c.address
            ) subquery
        """
    )

    task_a >> task_b