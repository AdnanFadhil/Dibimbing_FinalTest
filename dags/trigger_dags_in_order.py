from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    'trigger_dags_in_order',
    default_args=default_args,
    description='A DAG to trigger other DAGs in a specific order',
    schedule_interval='@yearly',  # You can set a schedule if needed
)

# Trigger create_connection_dag DAG
trigger_create_connection_dag = TriggerDagRunOperator(
    task_id='trigger_create_connection_dag',
    trigger_dag_id='create_connection_dag',
    dag=dag,
    trigger_rule='all_success',
)

# Trigger Ingest-Category DAG
trigger_ingest_category_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_category_dag',
    trigger_dag_id='Ingest-Category',
    dag=dag,
)

trigger_ingest_customers_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_customers_dag',
    trigger_dag_id='Ingest-Customers',
    dag=dag,
)

trigger_ingest_geolocation_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_geolocation_dag',
    trigger_dag_id='Ingest-Geolocation',
    dag=dag,
)

trigger_ingest_orders_items_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_orders_items_dags',
    trigger_dag_id='Ingest-Items',
    dag=dag,
)

trigger_ingest_payment_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_payment',
    trigger_dag_id='Ingest-Order-Payment',
    dag=dag,
)

trigger_ingest_orders_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_Orders_dag',
    trigger_dag_id='Ingest-Orders',
    dag=dag,
)

trigger_ingest_products_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_products_dag',
    trigger_dag_id='Ingest-Product',
    dag=dag,
)

trigger_ingest_reviews_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_reviews_dag',
    trigger_dag_id='Ingest-Review',
    dag=dag,
)

trigger_ingest_sellers_dag = TriggerDagRunOperator(
    task_id='trigger_ingest_sellers_dag',
    trigger_dag_id='Ingest-Sellers',
    dag=dag,
)

trigger_clean_and_transform_all_tables_dag = TriggerDagRunOperator(
    task_id='trigger_clean_and_transform_all_tables_dag',
    trigger_dag_id='clean_and_transform_all_tables_dag',
    dag=dag,
    trigger_rule='all_success',
)

trigger_create_dimension_tables_dag = TriggerDagRunOperator(
    task_id='trigger_create_dimension_tables_dag',
    trigger_dag_id='create_dimension_tables_dag',
    dag=dag,
    trigger_rule='all_success',
)

# Set task dependencies
trigger_create_connection_dag >> trigger_ingest_category_dag >> trigger_ingest_customers_dag >> trigger_ingest_geolocation_dag >> trigger_ingest_orders_items_dag >> trigger_ingest_payment_dag >> trigger_ingest_orders_dag >> trigger_ingest_products_dag >> trigger_ingest_reviews_dag >> trigger_ingest_sellers_dag >> trigger_clean_and_transform_all_tables_dag >> trigger_create_dimension_tables_dag
