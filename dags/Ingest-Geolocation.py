from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-Geolocation',
    default_args=default_args,
    description='A simple DAG to ingest CSV file into PostgreSQL',
    schedule_interval=None,
)

# Function to ingest CSV file into a Pandas DataFrame
def ingest_csv(**kwargs):
    data_folder = '/opt/airflow/data'
    csv_file_path = os.path.join(data_folder, 'olist_geolocation_dataset.csv')  # Update with your CSV file name
    df = pd.read_csv(csv_file_path)
    return df

# Function to insert data into PostgreSQL table
def insert_into_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_csv')  # Retrieve the DataFrame from the output of 'ingest_csv' task

    # Assuming the PostgreSQL connection ID is 'your_postgres_conn_id'
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Replace 'your_table_name' with the actual table name in PostgreSQL
    table_name = 'geolocation'

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

# Task to ingest CSV file
ingest_task = PythonOperator(
    task_id='ingest_csv',
    python_callable=ingest_csv,
    provide_context=True,
    dag=dag,
)

# Task to create PostgreSQL table
create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
    CREATE TABLE IF NOT EXISTS geolocation (
        geolocation_zip_code_prefix VARCHAR(10),
        geolocation_lat DOUBLE PRECISION,
        geolocation_lng DOUBLE PRECISION,
        geolocation_city VARCHAR(255),
        geolocation_state VARCHAR(2)
    );
    ''',
    postgres_conn_id='PostgresWarehouse',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Task to insert data into PostgreSQL table
insert_into_table_task = PythonOperator(
    task_id='insert_into_table',
    python_callable=insert_into_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_into_table_task
