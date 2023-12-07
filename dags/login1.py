from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
import json

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-login_attempts',
    default_args=default_args,
    description='A DAG to ingest JSON file into PostgreSQL',
    schedule_interval='@yearly',
)

def ingest_json(**kwargs):
    data_folder = '/opt/airflow/data'
    json_file_path = os.path.join(data_folder, 'login_attempts_0.json')  
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
    df = pd.json_normalize(data)
    return df

def insert_json_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_json')  
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    table_name = 'login_attempts'

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

ingest_task = PythonOperator(
    task_id='ingest_json',
    python_callable=ingest_json,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS login_attempts (
        id INT,
        customer_id INT,
        login_successful BOOLEAN,
        attempted_at TIMESTAMP
    );
    """,
    postgres_conn_id='PostgresWarehouse',  
    autocommit=True,
    dag=dag,
)

insert_json_table_task = PythonOperator(
    task_id='insert_json_table',
    python_callable=insert_json_postgres,
    provide_context=True,
    dag=dag,
)

ingest_task >> create_table_task >> insert_json_table_task
