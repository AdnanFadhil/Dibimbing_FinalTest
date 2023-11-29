from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'real13',
    default_args=default_args,
    description='A simple DAG to ingest CSV file into PostgreSQL',
    schedule_interval=None,
)

# Task to create PostgreSQL table and insert data
create_and_insert_task = PostgresOperator(
    task_id='create_and_insert',
    sql=[
        '''
        CREATE TABLE IF NOT EXISTS geolocation_data (
            geolocation_zip_code_prefix VARCHAR(10),
            geolocation_lat DOUBLE PRECISION,
            geolocation_lng DOUBLE PRECISION,
            geolocation_city VARCHAR(255),
            geolocation_state VARCHAR(2)
        );
        ''',
        '''
        COPY geolocation_data FROM '/opt/airflow/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;
        '''
    ],
    postgres_conn_id='PostgresWarehouse',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Set task dependencies
create_and_insert_task
