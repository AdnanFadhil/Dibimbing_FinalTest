from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator

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
    'data_ingestion_dag',
    default_args=default_args,
    description='A DAG to ingest data into dw-postgres',
    schedule_interval=timedelta(days=1),  
)

def copy_data():
    local_data_dir = '../data'
    container_data_dir = '/data'

    try:
        
        file_to_copy = 'olist_geolocation_dataset.csv'

        
        src_path = os.path.join(local_data_dir, file_to_copy)
        dest_path = os.path.join(container_data_dir, file_to_copy)

        
        shutil.copy(src_path, dest_path)

        print(f"Successfully copied {file_to_copy} to the container.")

    except Exception as e:
        print(f"Error copying data: {str(e)}")

copy_data_task = PythonOperator(
    task_id='copy_data_task',
    python_callable=copy_data,
    dag=dag,
)


trigger_create_table_task = TriggerDagRunOperator(
    task_id='trigger_create_table_task',
    trigger_dag_id='create_table_dag',
    dag=dag,
)

ingest_data_task = PostgresOperator(
    task_id='ingest_data_task',
    postgres_conn_id='dw_postgres_conn',
    sql='path/to/your_script.sql',
    dag=dag,
)

copy_data_task >> trigger_create_table_task
trigger_create_table_task >> ingest_data_task
