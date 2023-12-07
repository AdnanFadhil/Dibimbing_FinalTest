from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

# Set up PostgreSQL connection parameters
conn_id = "PostgresWarehouse"

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'extract_load_login_attempts_to_postgres',
    default_args=default_args,
    description='Extract and load login attempts data to PostgreSQL',
    schedule_interval=None,  # Adjust as needed
    catchup=False,
)

# Define the extract and load task
def extract_and_load_login_attempts_to_postgres(**kwargs):
    file_paths = [
        '/opt/airflow/data/login_attempts_0.json',
        '/opt/airflow/data/login_attempts_1.json',
        '/opt/airflow/data/login_attempts_2.json',
        '/opt/airflow/data/login_attempts_3.json',
        '/opt/airflow/data/login_attempts_4.json',
        '/opt/airflow/data/login_attempts_5.json',
        '/opt/airflow/data/login_attempts_6.json',
        '/opt/airflow/data/login_attempts_7.json',
        '/opt/airflow/data/login_attempts_8.json',
        '/opt/airflow/data/login_attempts_9.json',
    ]
    
    data_frames = []
    
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            file_data = json.load(file)
            data_frames.append(pd.DataFrame(file_data))
    
    combined_data = pd.concat(data_frames)
    
    table_name = "login_attempts_history"
    table_schema = "id INTEGER PRIMARY KEY, customer_id INTEGER, login_success BOOLEAN, attempted_at TIMESTAMP"
    
    # Create table in PostgreSQL using the specified connection
    create_table_in_postgres_with_connection(conn_id, table_name, table_schema)
    
    # Load data into PostgreSQL using the specified connection
    load_data_to_postgres_with_connection(conn_id, table_name, combined_data)

def create_table_in_postgres_with_connection(conn_id, table_name, table_schema):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    try:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
        cursor.execute(create_table_query)
        connection.commit()
    finally:
        cursor.close()
        connection.close()

def load_data_to_postgres_with_connection(conn_id, table_name, data_frame):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    connection_info = postgres_hook.get_connection(conn_id)

    engine_uri = f"postgresql+psycopg2://{connection_info.login}:{connection_info.password}@{connection_info.host}:{connection_info.port}/{connection_info.schema}"
    engine = create_engine(engine_uri)
    
    data_frame.to_sql(table_name, engine, if_exists='replace', index=False)

extract_and_load_task = PythonOperator(
    task_id='extract_and_load_login_attempts_task',
    python_callable=extract_and_load_login_attempts_to_postgres,
    provide_context=True,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
