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
    'Ingest-Sellers',
    default_args=default_args,
    description='A DAG to ingest Excel file into PostgreSQL',
    schedule_interval='@yearly',
)


def ingest_xlsx(**kwargs):
    xlsx_file_path = os.path.join('/opt/airflow/data/olist_sellers_dataset.xlsx')  
    df = pd.read_excel(xlsx_file_path)
    return df


def insert_xlsx_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_xlsx') 

    
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    
    table_name = 'sellers'

    df.to_sql(table_name, con=engine, index=False, if_exists='replace')


ingest_task = PythonOperator(
    task_id='ingest_xlsx',
    python_callable=ingest_xlsx,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql="""CREATE TABLE IF NOT EXISTS sellers (
            seller_id VARCHAR(50),
            seller_zip_code_prefix INT,
            seller_city VARCHAR(255),
            seller_state VARCHAR(2)
        );
    """,
    postgres_conn_id='PostgresWarehouse',
    autocommit=True,
    dag=dag,
)


insert_xlsx_table_task = PythonOperator(
    task_id='insert_xlsx_table',
    python_callable=insert_xlsx_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
ingest_task >> create_table_task >> insert_xlsx_table_task