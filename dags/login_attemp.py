from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine
import json

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-Json-Login-Attempts',
    default_args=default_args,
    description='A simple DAG to ingest JSON files into PostgreSQL',
    schedule_interval='@yearly',
)

data_folder = '/opt/airflow/data'
json_files = ['login_attempts_0.json', 'login_attempts_1.json', 'login_attempts_2.json','login_attempts_3.json','login_attempts_4.json','login_attempts_5.json','login_attempts_6.json','login_attempts_7.json','login_attempts_8.json','login_attempts_9.json']  # Add more JSON files as needed

def ingest_json(file_name, **kwargs):
    json_file_path = os.path.join(data_folder, file_name)
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    df = pd.json_normalize(data)
    return df

def insert_into_postgres(**kwargs):
    ti = kwargs['ti']
    df_list = ti.xcom_pull(task_ids=[f'ingest_json_{json_file}' for json_file in json_files])

    # Concatenate or merge all dataframes into a single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)  # or df = pd.concat(df, axis=1) for merging along columns

    # Modify the connection string based on your PostgreSQL setup
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Use a single table name for all merged data
    table_name = 'login_attempts'

    combined_df.to_sql(table_name, con=engine, index=False, if_exists='replace')  # Change 'replace' to 'append' if needed # Change 'replace' to 'append' if needed

ingest_tasks = []
insert_task = PythonOperator(
    task_id='insert_into_table',
    python_callable=insert_into_postgres,
    provide_context=True,
    dag=dag,
)

for json_file in json_files:
    ingest_task = PythonOperator(
        task_id=f'ingest_json_{json_file}',
        python_callable=ingest_json,
        op_args=[json_file],
        provide_context=True,
        dag=dag,
    )
    ingest_task >> insert_task
    ingest_tasks.append(ingest_task)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
    CREATE TABLE IF NOT EXISTS login_attempts (
        id INT,
        customer_id INT,
        login_successful BOOLEAN,
        attempted_at TIMESTAMP
    );
    ''',
    postgres_conn_id='PostgresWarehouse',
    autocommit=True,
    dag=dag,
)

create_table_task >> ingest_tasks
