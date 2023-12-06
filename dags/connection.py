from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'Adnan',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_connection_dag',
    default_args=default_args,
    description='A DAG for creating a connection',
    schedule_interval=None,
)

def create_postgres_connection_func(**kwargs):
    # Define the connection parameters
    conn_id = 'PostgresWarehouse'
    conn_type = 'postgres'
    host = 'dataeng-warehouse-postgres'
    database = 'data_warehouse'
    user = 'user'
    password = 'password'
    port = 5432

    # Check if the connection already exists
    existing_conn = BaseHook.get_connection(conn_id)

    if existing_conn:
        # Update the connection if parameters are different
        if (
            existing_conn.host != host
            or existing_conn.schema != database
            or existing_conn.login != user
            or existing_conn.password != password
            or existing_conn.port != port
        ):
            existing_conn.host = host
            existing_conn.schema = database
            existing_conn.login = user
            existing_conn.password = password
            existing_conn.port = port
            existing_conn.update()
    else:
        # Create a new connection
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
        session = kwargs['session']
        session.add(conn)
        session.commit()

# Define tasks
create_postgres_connection_task = PythonOperator(
    task_id='create_postgres_connection_task',
    python_callable=create_postgres_connection_func,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
create_postgres_connection_task >> end_task
