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
    'Ingest-Product',
    default_args=default_args,
    description='A DAG to ingest Excel file into PostgreSQL',
    schedule_interval=None,
)

# Function to ingest Excel file into a Pandas DataFrame
def ingest_xlsx(**kwargs):
    xlsx_file_path = os.path.join('/opt/airflow/data/olist_products_dataset.xlsx')  # Update with your Excel file name
    df = pd.read_excel(xlsx_file_path)
    return df

# Function to insert data into PostgreSQL table
def insert_xlsx_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_xlsx')  # Retrieve the DataFrame from the output of 'ingest_xlsx' task

    # Assuming the PostgreSQL connection ID is 'your_postgres_conn_id'
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Replace 'your_table_name' with the actual table name in PostgreSQL
    table_name = 'products'

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

# Task to ingest Excel file
ingest_task = PythonOperator(
    task_id='ingest_xlsx',
    python_callable=ingest_xlsx,
    provide_context=True,
    dag=dag,
)

# Task to create PostgreSQL table (optional if table already exists)
create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""
        CREATE TABLE IF NOT EXISTS products (
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
insert_xlsx_table_task = PythonOperator(
    task_id='insert_xlsx_table',
    python_callable=insert_xlsx_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_xlsx_table_task