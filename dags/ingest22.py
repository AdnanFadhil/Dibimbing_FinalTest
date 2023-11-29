from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'ingest_xls_to_postgres123',
    default_args=default_args,
    description='A DAG to ingest data from Excel to PostgreSQL',
    schedule_interval=timedelta(days=1),  # You can adjust the schedule interval
)

def ingest_data():
    try:
        # PostgreSQL connection details
        postgres_conn_str = 'postgresql+psycopg2://user:password@dataeng-warehouse-postgres:5432/data_warehouse'
        
        # Excel file path using os.path
        excel_file_path = os.path.join('/opt/airflow/data', 'olist_products_dataset.xls')
        
        # Check if the Excel file exists
        if not os.path.exists(excel_file_path):
            logging.error(f'Excel file not found at {excel_file_path}')
            return

        # Table name
        table_name = 'product'
        
        # Read Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_file_path)
        logging.info(f'Number of rows in DataFrame: {len(df)}')
        
        # Create a SQLAlchemy engine
        engine = create_engine(postgres_conn_str)
        
        # Create table if not exists
        df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Write the DataFrame to the PostgreSQL table
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logging.info(f'Data ingested into table {table_name} successfully. Number of rows inserted: {len(df)}')
    except Exception as e:
        logging.error(f'Error ingesting data: {str(e)}')

with dag:
    ingest_data_task = PythonOperator(
        task_id='ingest_data_task',
        python_callable=ingest_data,
    )
