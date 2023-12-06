from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

def create_airflow_connection(conn_id, conn_type, login, password, host, port, schema):
    # Check if the connection already exists
    existing_conn = BaseHook.get_connection(conn_id)
    if existing_conn:
        print(f"Connection {conn_id} already exists. Skipping creation.")
    else:
        # Create a new connection
        conn = BaseHook.get_connection(conn_id)
        conn.conn_id = conn_id
        conn.conn_type = conn_type
        conn.login = login
        conn.password = password
        conn.host = host
        conn.port = port
        conn.schema = schema
        conn.save()
        print(f"Connection {conn_id} created successfully.")

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
    'create_airflow_connections',
    default_args=default_args,
    description='A DAG to create Airflow connections',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

create_connections_task = PythonOperator(
    task_id='create_connections',
    python_callable=create_airflow_connection,
    op_args=['postgres_ops', 'postgres', 'user', 'password', 'host', 5432, 'database'],
    dag=dag,
)

# Add more tasks for creating additional connections if needed

if __name__ == "__main__":
    dag.cli()

