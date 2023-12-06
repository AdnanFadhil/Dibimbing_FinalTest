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
    'Ingest-Review',
    default_args=default_args,
    description='A DAG to ingest CSV file into PostgreSQL',
    schedule_interval='@yearly',
)


def ingest_csv(**kwargs):
    data_folder = '/opt/airflow/data'
    csv_file_path = os.path.join(data_folder, 'olist_order_reviews_dataset.csv')  
    df = pd.read_csv(csv_file_path)
    return df


def insert_into_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_csv')  

    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    table_name = 'review'

    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'])


    df.to_sql(table_name, con=engine, index=False, if_exists='replace')


ingest_task = PythonOperator(
    task_id='ingest_csv',
    python_callable=ingest_csv,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
    CREATE TABLE IF NOT EXISTS review (
        review_id VARCHAR(50),
        order_id VARCHAR(50),
        review_score INT,
        review_comment_title VARCHAR(255),
        review_comment_message TEXT,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP
    );
    ''',
    postgres_conn_id='PostgresWarehouse',  
    autocommit=True,
    dag=dag,
)


insert_into_table_task = PythonOperator(
    task_id='insert_into_table',
    python_callable=insert_into_postgres,
    provide_context=True,
    dag=dag,
)


ingest_task >> create_table_task >> insert_into_table_task
