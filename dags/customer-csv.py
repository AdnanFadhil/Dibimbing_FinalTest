from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

# Define the DAG
dag = DAG(
    dag_id="extract_and_load_xls_to_postgres",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
)

# Define connection parameters
conn_params = {
    "host": "dataeng-warehouse-postgres",
    "port": 5432,
    "user": "user",
    "password": "password",
    "database": "data_warehouse"
}

# Functions for loading data to PostgreSQL
def load_data_to_postgres(table_name, dataframe, conn_params):
    engine = create_engine(f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
    
    try:
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    finally:
        engine.dispose()

def create_table_in_postgres(table_name, table_schema, conn_params):
    engine = create_engine(f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
    
    try:
        engine.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})")
    finally:
        engine.dispose()

# Function for extracting and loading customers
def extract_and_load_customers_to_postgres(**kwargs):
    file_paths = [
        '/opt/airflow/data/customer_0.csv',
        '/opt/airflow/data/customer_1.csv',
        '/opt/airflow/data/customer_2.csv',
        '/opt/airflow/data/customer_3.csv',
        '/opt/airflow/data/customer_4.csv',
        '/opt/airflow/data/customer_5.csv',
        '/opt/airflow/data/customer_6.csv',
        '/opt/airflow/data/customer_7.csv',
        '/opt/airflow/data/customer_8.csv',
        '/opt/airflow/data/customer_9.csv',
    ]
    data_frames = [pd.read_csv(file) for file in file_paths]
    combined_data = pd.concat(data_frames)
    
    table_name = "customers"
    table_schema = "id INTEGER PRIMARY KEY, first_name VARCHAR(100), last_name VARCHAR(100), address VARCHAR(20), gender VARCHAR(200), zip_code VARCHAR(200)"  # Table schema
    
    # Load data into PostgreSQL
    create_table_in_postgres(table_name, table_schema, conn_params)
    load_data_to_postgres(table_name, combined_data, conn_params)

# Define task for extracting and loading customer data
extract_and_load_customers_task = PythonOperator(
    task_id="extract_and_load_customers_to_postgres",
    python_callable=extract_and_load_customers_to_postgres,
    provide_context=True,  # Add this line to accept context information
    dag=dag,
)
