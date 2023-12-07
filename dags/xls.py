from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd


# Replace 'your_postgres_connection_id' with the actual connection ID you are using for PostgreSQL
conn_id = 'your_postgres_connection_id'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'extract_and_load_xls_to_postgres',
    default_args=default_args,
    description='A DAG to extract and load data into PostgreSQL tables',
    schedule_interval=None,  # Set to None if you want to manually trigger the DAG
)

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
    engine = PostgresHook.get_sqlalchemy_engine(conn_id)
    data_frame.to_sql(table_name, engine, if_exists='replace', index=False)

product_category_schema = "id INTEGER PRIMARY KEY, name VARCHAR(50)"
products_schema = "id INTEGER PRIMARY KEY, name VARCHAR(100), price FLOAT, category_id INTEGER, supplier_id INTEGER"
suppliers_schema = "id SERIAL PRIMARY KEY, name VARCHAR(100), country VARCHAR(100)"

product_category_task = PythonOperator(
    task_id='extract_and_load_product_category',
    python_callable=create_table_in_postgres_with_connection,
    op_args=[conn_id, "product_category", product_category_schema],
    dag=dag,
)

products_task = PythonOperator(
    task_id='extract_and_load_products',
    python_callable=create_table_in_postgres_with_connection,
    op_args=[conn_id, "products", products_schema],
    dag=dag,
)

suppliers_task = PythonOperator(
    task_id='extract_and_load_suppliers',
    python_callable=create_table_in_postgres_with_connection,
    op_args=[conn_id, "suppliers", suppliers_schema],
    dag=dag,
)

# Set the task dependencies
product_category_task >> products_task >> suppliers_task
