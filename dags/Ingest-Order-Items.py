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
    'Ingest-Items',
    default_args=default_args,
    description='A DAG to ingest JSON file into PostgreSQL',
    schedule_interval=None,
)

# Function to ingest JSON file into a Pandas DataFrame
def ingest_json(**kwargs):
    data_folder = '/opt/airflow/data'
    json_file_path = os.path.join(data_folder, 'olist_order_items_dataset.json')  # Update with your JSON file name
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
    df = pd.json_normalize(data)
    return df

# Function to insert data into PostgreSQL table
def insert_json_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_json')  # Retrieve the DataFrame from the output of 'ingest_json' task

    # Assuming the PostgreSQL connection ID is 'PostgresWarehouse'
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Replace 'orders' with the actual table name in PostgreSQL
    table_name = 'orders_items'

    # Write the DataFrame to the PostgreSQL table
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

# Task to ingest JSON file
ingest_task = PythonOperator(
    task_id='ingest_json',
    python_callable=ingest_json,
    provide_context=True,
    dag=dag,
)

# Task to create PostgreSQL table (optional if table already exists)
create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS orders_items (
            order_id VARCHAR(255),
            order_item_id VARCHAR(255),
            product_id VARCHAR(255),
            seller_id VARCHAR(255),
            shipping_limit_date TIMESTAMP,
            price FLOAT,
            freight_value FLOAT
        );
    """,
    postgres_conn_id='PostgresWarehouse',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Task to insert data into PostgreSQL table
insert_json_table_task = PythonOperator(
    task_id='insert_json_table',
    python_callable=insert_json_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_json_table_task
