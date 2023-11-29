from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'csv_ingest_dag',
    default_args=default_args,
    description='A simple DAG to ingest CSV file into PostgreSQL',
    schedule_interval=None,
)

# Function to ingest CSV file into a Pandas DataFrame
def ingest_csv(**kwargs):
    data_folder = '/opt/airflow/data'
    csv_file_path = os.path.join(data_folder, 'your_csv_file.csv')  # Update with your CSV file name
    df = pd.read_csv(csv_file_path)
    return df

# Task to ingest CSV file
ingest_task = PythonOperator(
    task_id='ingest_csv',
    python_callable=ingest_csv,
    provide_context=True,
    dag=dag,
)

# Task to create PostgreSQL table and insert data
create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
    CREATE TABLE IF NOT EXISTS geolocation_data (
        geolocation_zip_code_prefix VARCHAR(10),
        geolocation_lat DOUBLE PRECISION,
        geolocation_lng DOUBLE PRECISION,
        geolocation_city VARCHAR(255),
        geolocation_state VARCHAR(2)
    );
    ''',
    postgres_conn_id='your_postgres_conn_id',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Task to insert data into PostgreSQL table
insert_into_table_task = PostgresOperator(
    task_id='insert_into_table',
    sql='''
    INSERT INTO geolocation_data (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
    SELECT * FROM geolocation_data_temp;  -- Use the main table name instead of a temporary table
    ''',
    postgres_conn_id='your_postgres_conn_id',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_into_table_task
