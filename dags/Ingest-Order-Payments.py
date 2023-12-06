from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import fastavro
import os

default_args = {
    'owner': 'Adnan',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Ingest-Order-Payment',
    default_args=default_args,
    description='A DAG to ingest Avro file into PostgreSQL',
    schedule_interval='@yearly',
)


def ingest_avro(**kwargs):
    avro_file_path = '/opt/airflow/data/olist_order_payments_dataset.avro' 
    table_name = 'order_payment_avro'

    # Read Avro file into a Pandas DataFrame
    with open(avro_file_path, 'rb') as avro_file:
        avro_reader = fastavro.reader(avro_file)
        avro_data = [record for record in avro_reader]

    table_df = pd.DataFrame(avro_data)
    return table_df


def insert_avro_postgres(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='ingest_avro') 
    engine = create_engine('postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse')
    table_name = 'order_payment'
    df.to_sql(table_name, con=engine, index=False, if_exists='replace')

ingest_task = PythonOperator(
    task_id='ingest_avro',
    python_callable=ingest_avro,
    provide_context=True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    sql=f"""
        CREATE TABLE IF NOT EXISTS order_payment (
            order_id VARCHAR(255),
            payment_sequential INT,
            payment_type VARCHAR(255),
            payment_installments INT,
            payment_value FLOAT
            
        );
    """,
    postgres_conn_id='PostgresWarehouse',
    autocommit=True,
    dag=dag,
)

insert_avro_table_task = PythonOperator(
    task_id='insert_avro_table',
    python_callable=insert_avro_postgres,
    provide_context=True,
    dag=dag,
)

ingest_task >> create_table_task >> insert_avro_table_task
