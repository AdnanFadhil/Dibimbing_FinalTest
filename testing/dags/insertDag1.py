from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pyarrow.parquet as pq
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'insert_data_into_dw_postgres',
    default_args=default_args,
    description='A DAG to insert data into DW PostgreSQL',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

datasets = [
    {'file_path': '/opt/airflow/data/olist_customers_dataset.json', 'table_name': 'customers_dw'},
    {'file_path': '/opt/airflow/data/olist_geolocation_dataset.csv', 'table_name': 'geolocation_dw'},
    {'file_path': '/opt/airflow/data/olist_order_items_dataset.json', 'table_name': 'order_items_dw'},
    {'file_path': '/opt/airflow/data/olist_order_payments_dataset.avro', 'table_name': 'order_payments_dw'},
    {'file_path': '/opt/airflow/data/olist_order_reviews_dataset.csv', 'table_name': 'order_reviews_dw'},
    {'file_path': '/opt/airflow/data/olist_orders_dataset.parquet', 'table_name': 'orders_dw'},
    {'file_path': '/opt/airflow/data/olist_products_dataset.xls', 'table_name': 'products_dw'},
    {'file_path': '/opt/airflow/data/olist_sellers_dataset.xls', 'table_name': 'sellers_dw'},
    {'file_path': '/opt/airflow/data/product_category_name_translation.xls', 'table_name': 'category_translation_dw'},
]

prev_insert_task = None  # Initialize outside the loop

def insert_data_to_postgres(file_path, table_name, **kwargs):
    file_extension = file_path.split('.')[-1]

    try:
        if file_extension == 'json':
            df = pd.read_json(file_path)
        elif file_extension == 'csv':
            df = pd.read_csv(file_path)
        elif file_extension == 'avro':
            with open(file_path, 'rb') as avro_file:
                reader = DataFileReader(avro_file, DatumReader())
                records = [record for record in reader]
                df = pd.DataFrame(records)
        elif file_extension == 'parquet':
            table = pq.read_table(file_path)
            df = table.to_pandas()
        elif file_extension == 'xls':
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        # Connect to DW PostgreSQL using a scoped session
        engine = create_engine('postgresql+psycopg2://user:password@5433/data_warehouse')
        session_factory = sessionmaker(bind=engine)
        Session = scoped_session(session_factory)

        with Session() as session:
            # Insert data into DW PostgreSQL
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            session.commit()

    except SQLAlchemyError as e:
        # Log the exception
        logging.error(f"Error during data insertion to PostgreSQL: {e}")
        # Optionally, you can raise the exception or handle it as needed.

# Create tasks dynamically for each dataset
for i, dataset in enumerate(datasets):
    task_id = f'insert_task_{i}'
    insert_task = PythonOperator(
        task_id=task_id,
        python_callable=insert_data_to_postgres,
        provide_context=True,
        op_args=[dataset['file_path'], dataset['table_name']],
        dag=dag,
    )

    if prev_insert_task is not None:
        insert_task >> prev_insert_task  # Set up task dependencies

    prev_insert_task = insert_task
