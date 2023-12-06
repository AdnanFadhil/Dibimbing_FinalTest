from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'create_dimension_tables_dag',
    default_args=default_args,
    description='A DAG for creating dimension tables',
    schedule_interval='@yearly',
)

create_item_delivered_dimension_query = """
CREATE TABLE IF NOT EXISTS item_delivered_dimension AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    oi.price AS item_price,
    CASE
        WHEN o.order_status = 'delivered' THEN 'yes'
        ELSE 'no'
    END AS delivered
FROM
    orders_items oi
JOIN
    orders o ON oi.order_id = o.order_id
JOIN
    customers c ON o.customer_id = c.customer_id;
"""

create_product_dimension_query = """
CREATE TABLE IF NOT EXISTS product_dimension AS
SELECT
    p.product_id,
    pc.product_category_name_english AS product_name,
    o.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    o.price
FROM products p
JOIN orders_items o ON p.product_id = o.product_id
JOIN sellers s ON o.seller_id = s.seller_id
JOIN product_categories pc ON p.product_category_name = pc.product_category_name;
"""

create_review_dimension_query = """
CREATE TABLE IF NOT EXISTS review_dimension AS
SELECT
    r.review_id,
    oi.product_id,
    c.customer_id,
    r.review_score
FROM
    review r
JOIN
    orders o ON r.order_id = o.order_id
JOIN
    orders_items oi ON r.order_id = oi.order_id
JOIN
    customers c ON o.customer_id = c.customer_id;
"""

create_customer_payment_dimension_query = """
CREATE TABLE IF NOT EXISTS customer_payment_dimension AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    SUM(CAST(id.item_price AS NUMERIC)) AS total_amount_paid
FROM
    item_delivered_dimension id
JOIN
    customers c ON id.customer_id = c.customer_id
GROUP BY
    c.customer_id, c.customer_unique_id;
"""

create_item_delivered_dimension_task = PostgresOperator(
    task_id='create_item_delivered_dimension_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_item_delivered_dimension_query,
    dag=dag,
)

create_product_dimension_task = PostgresOperator(
    task_id='create_product_dimension_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_product_dimension_query,
    dag=dag,
)

create_review_dimension_task = PostgresOperator(
    task_id='create_review_dimension_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_review_dimension_query,
    dag=dag,
)

create_customer_payment_dimension_task = PostgresOperator(
    task_id='create_customer_payment_dimension_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_customer_payment_dimension_query,
    dag=dag,
)

create_item_delivered_dimension_task >> [create_product_dimension_task, create_review_dimension_task, create_customer_payment_dimension_task]
