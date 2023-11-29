from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
import xlrd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'xlsIngst',
    default_args=default_args,
    description='A simple DAG to ingest Excel file into PostgreSQL',
    schedule_interval=None,
)

# Function to ingest Excel file into a Pandas DataFrame using xlrd
def ingest_xls(**kwargs):
    data_folder = '/opt/airflow/data'
    xls_file_path = os.path.join(data_folder, 'olist_products_dataset.xls')  # Update with your Excel file name
    df = pd.read_excel(xls_file_path, engine='xlrd')  # Specify 'xlrd' as the engine
    return df

# Function to insert data into PostgreSQL table
def insert_xls_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_xls')  # Retrieve the DataFrame from the output of 'ingest_xls' task

    # Assuming the PostgreSQL connection ID is 'your_postgres_conn_id'
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Replace 'your_table_name' with the actual table name in PostgreSQL
    table_name = 'product'

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

# Task to ingest Excel file
ingest_task = PythonOperator(
    task_id='ingest_xls',
    python_callable=ingest_xls,
    provide_context=True,
    dag=dag,
)

# Task to create PostgreSQL table
create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS product (
            product_id VARCHAR(255),
            product_category_name VARCHAR(255),
            product_name_length INT,
            product_description_length INT,
            product_photos_qty INT,
            product_weight_g INT,
            product_length_cm INT,
            product_height_cm INT,
            product_width_cm INT
        );
    """,
    postgres_conn_id='PostgresWarehouse',  # Update with your PostgreSQL connection ID
    autocommit=True,
    dag=dag,
)

# Task to insert data into PostgreSQL table
insert_xls_table_task = PythonOperator(
    task_id='insert_xls_table',
    python_callable=insert_xls_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_xls_table_task
