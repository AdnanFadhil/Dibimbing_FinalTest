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
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'create_dimension_tables_dag1',
    default_args=default_args,
    description='A DAG for creating dimension tables',
    schedule_interval='@yearly',
)

# Assume your tables are named as follows:
# customers, products, product_categories, suppliers, login_attempts_history, coupons, orders, order_item

# Add your query for creating a product_order_summary dimension table
create_product_order_summary_query = """
CREATE TABLE IF NOT EXISTS product_order_summary AS
SELECT
    pc.name AS category,
    SUM(oi.amount) AS total_ordered,
    SUM(CASE WHEN oi.coupon_id IS NOT NULL THEN (oi.amount * dd.discount_percent / 100) ELSE 0 END) AS total_discount,
    SUM(oi.amount) + SUM(CASE WHEN oi.coupon_id IS NOT NULL THEN (oi.amount * dd.discount_percent / 100) ELSE 0 END) AS total_amount
FROM
    order_item oi
JOIN
    product p ON oi.product_id = p.id
JOIN
    product_categories pc ON p.category_id = pc.id
LEFT JOIN
    discount_data dd ON oi.coupon_id = dd.id
GROUP BY
    pc.name;
"""

# Create a task for the product_order_summary dimension table
create_product_order_summary_task = PostgresOperator(
    task_id='create_product_order_summary_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_product_order_summary_query,
    dag=dag,
)

# Add your query for creating a consolidated order dimension table
create_order_dimension_query = """
CREATE TABLE IF NOT EXISTS order_dimension AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.status,
    o.created_at,
    oi.id AS order_item_id,
    oi.product_id,
    oi.amount,
    oi.coupon_id
FROM
    orders o
JOIN
    order_item oi ON o.id = oi.order_id;
"""

# Create a task for the consolidated order dimension table
create_order_dimension_task = PostgresOperator(
    task_id='create_order_dimension_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_order_dimension_query,
    dag=dag,
)

# Add your query for creating dim_supplier dimension table
create_dim_supplier_query = """
CREATE TABLE IF NOT EXISTS dim_supplier AS
SELECT
    s.id AS supplier_id,
    s.name AS supplier_name,
    COUNT(DISTINCT p.id) AS total_products_sold,
    SUM(oi.amount * p.price) AS total_money_received
FROM
    order_item oi
JOIN
    product p ON oi.product_id = p.id
JOIN
    supplier s ON p.supplier_id = s.id
JOIN
    orders o ON oi.order_id = o.id
GROUP BY
    s.id, s.name;
"""

# Create a task for the dim_supplier dimension table
create_dim_supplier_task = PostgresOperator(
    task_id='create_dim_supplier_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_dim_supplier_query,
    dag=dag,
)

# Add your query for creating monthly_product_sales table
create_monthly_product_sales_query = """
CREATE TABLE IF NOT EXISTS monthly_product_sales AS
SELECT
    TO_CHAR(DATE_TRUNC('day', o.created_at), 'DD') AS day,
    COUNT(oi.id) AS total_products_sold
FROM
    orders o
JOIN
    order_item oi ON o.id = oi.order_id
GROUP BY
    day
ORDER BY
    day;
"""

# Create a task for the monthly_product_sales table
create_monthly_product_sales_task = PostgresOperator(
    task_id='create_monthly_product_sales_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_monthly_product_sales_query,
    dag=dag,
)

# Add your query for creating the login_attempts_summary table
create_login_attempts_summary_query = """
CREATE TABLE IF NOT EXISTS login_attempts_summary AS
SELECT
    TO_CHAR(DATE_TRUNC('day', o.attempted_at), 'DD') AS day,
    CASE
        WHEN EXTRACT(HOUR FROM o.attempted_at) >= 0 AND EXTRACT(HOUR FROM o.attempted_at) < 6 THEN 'Dini Hari'
        WHEN EXTRACT(HOUR FROM o.attempted_at) >= 6 AND EXTRACT(HOUR FROM o.attempted_at) < 12 THEN 'Pagi'
        WHEN EXTRACT(HOUR FROM o.attempted_at) >= 12 AND EXTRACT(HOUR FROM o.attempted_at) < 18 THEN 'Siang-Sore'
        WHEN EXTRACT(HOUR FROM o.attempted_at) >= 18 AND EXTRACT(HOUR FROM o.attempted_at) <= 23 THEN 'Malam'
        ELSE 'Unknown'
    END AS waktu,
    COUNT(*) FILTER (WHERE login_successful) AS successful_logins,
    COUNT(*) FILTER (WHERE NOT login_successful) AS unsuccessful_logins
FROM
    login_attempts_history o
GROUP BY
    day, waktu
ORDER BY
    day, waktu;
"""

# Create a task for the login_attempts_summary table
create_login_attempts_summary_task = PostgresOperator(
    task_id='create_login_attempts_summary_task',
    postgres_conn_id='PostgresWarehouse',
    sql=create_login_attempts_summary_query,
    dag=dag,
)

# Set up task dependencies
create_product_order_summary_task >> create_order_dimension_task >> create_dim_supplier_task >> create_monthly_product_sales_task >> create_login_attempts_summary_task
