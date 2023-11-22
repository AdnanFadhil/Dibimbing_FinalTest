from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
from sqlalchemy import create_engine

# Define the DAG
dag = DAG(
    'data_insertion_dag',
    description='DAG for inserting data into PostgreSQL',
    schedule_interval=None,  # You can set a schedule interval if needed
    start_date=datetime(2023, 1, 1),  # Adjust the start date
)

# Function to insert JSON data into PostgreSQL
def insert_json_data():
    # Load JSON data
    with open('/path/to/data/data.json', 'r') as json_file:
        json_data = json.load(json_file)

    # Connect to PostgreSQL and insert data
    engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/database')
    pd.DataFrame(json_data).to_sql('your_table_name', con=engine, if_exists='replace', index=False)

# Function to insert CSV data into PostgreSQL
def insert_csv_data():
    # Load CSV data
    csv_data = pd.read_csv('/path/to/data/data.csv')

    # Connect to PostgreSQL and insert data
    engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/database')
    csv_data.to_sql('your_table_name', con=engine, if_exists='replace', index=False)

# Define the tasks
insert_json_task = PythonOperator(
    task_id='insert_json_data',
    python_callable=insert_json_data,
    dag=dag,
)

insert_csv_task = PythonOperator(
    task_id='insert_csv_data',
    python_callable=insert_csv_data,
    dag=dag,
)

# Set task dependencies
insert_json_task >> insert_csv_task
