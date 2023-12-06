from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from sqlalchemy.orm import sessionmaker
from airflow.utils.db import provide_session

default_args = {
    'owner': 'Adnan',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'create_connection_dag',
    default_args=default_args,
    description='A DAG for creating or updating a connection',
    schedule_interval=None,
)

@provide_session
def create_postgres_connection_func(session=None, **kwargs):
    conn_id = 'PostgresWarehouse'
    conn_type = 'postgres'
    host = 'dataeng-warehouse-postgres'
    database = 'data_warehouse'
    user = 'user'
    password = 'password'
    port = 5432

    try:
        # Try to get the existing connection
        existing_conn = BaseHook.get_connection(conn_id)
        print(f"Connection {conn_id} already exists.")
    except Exception as e:
        # Connection does not exist, create a new connection
        print(f"Connection {conn_id} does not exist. Creating...")

        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            schema=database,
            login=user,
            password=password,
            port=port,
        )

        # Add the connection to the metadata database
        session.add(conn)
        session.commit()
        print(f"Connection {conn_id} created.")

create_postgres_connection_task = PythonOperator(
    task_id='create_postgres_connection_task',
    python_callable=create_postgres_connection_func,
    provide_context=True,
    dag=dag,
)

create_postgres_connection_task
