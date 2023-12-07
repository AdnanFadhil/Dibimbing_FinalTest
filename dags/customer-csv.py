from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine  # Import the create_engine function

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-UserData',
    default_args=default_args,
    description='A simple DAG to ingest CSV files into PostgreSQL',
    schedule_interval='@yearly',
)

data_folder = '/opt/airflow/data'
csv_files = ['customer_0.csv', 'customer_1.csv', 'customer_2.csv', 'customer_3.csv', 'customer_4.csv', 'customer_5.csv',
             'customer_6.csv', 'customer_7.csv', 'customer_8.csv', 'customer_9.csv']  # Add more CSV files as needed

def ingest_csv(file_name, **kwargs):
    csv_file_path = os.path.join(data_folder, file_name)    
    df = pd.read_csv(csv_file_path)
    return df

def insert_into_postgres(file_name, **kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids=f'ingest_csv_{file_name}')

    # Modify the connection string based on your PostgreSQL setup
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    # Dynamically generate table name based on the CSV file name
    table_name = f'user_data_{file_name.replace(".csv", "")}'

    df.to_sql(table_name, con=engine, index=False, if_exists='append')

ingest_tasks = []
insert_tasks = []

for csv_file in csv_files:
    ingest_task = PythonOperator(
        task_id=f'ingest_csv_{csv_file}',
        python_callable=ingest_csv,
        op_args=[csv_file],
        provide_context=True,
        dag=dag,
    )

    insert_task = PythonOperator(
        task_id=f'insert_into_table_{csv_file}',
        python_callable=insert_into_postgres,
        op_args=[csv_file],
        provide_context=True,
        dag=dag,
    )

    ingest_tasks.append(ingest_task)
    insert_tasks.append(insert_task)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
    CREATE TABLE IF NOT EXISTS user_data (
        id INT,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        gender CHAR(1),
        address VARCHAR(255),
        zip_code VARCHAR(10)
    );
    ''',
    postgres_conn_id='PostgresWarehouse',
    autocommit=True,
    dag=dag,
)

create_table_task >> ingest_tasks >> insert_tasks
