from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import pyarrow.parquet as pq
import os

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-Orders-Parquet',
    default_args=default_args,
    description='A DAG to ingest Parquet file into PostgreSQL',
    schedule_interval='@yearly',
)

def ingest_parquet(**kwargs):
    parquet_file_path = '/opt/airflow/data/order.parquet' 
    table_name = 'orders_parquet'
    table_df = pq.read_table(parquet_file_path).to_pandas()
    return table_df

def insert_parquet_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_parquet')  
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')
    table_name = 'orders'
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

ingest_task = PythonOperator(
    task_id='ingest_parquet',
    python_callable=ingest_parquet,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql=f"""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            status VARCHAR(255),
            created_at TIMESTAMP
        );
    """,
    postgres_conn_id='PostgresWarehouse',  
    autocommit=True,
    dag=dag,
)

insert_parquet_table_task = PythonOperator(
    task_id='insert_parquet_table',
    python_callable=insert_parquet_postgres,
    provide_context=True,
    dag=dag,
)

ingest_task >> create_table_task >> insert_parquet_table_task
