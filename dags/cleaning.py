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
    'clean_and_transform_all_tables_dag',
    default_args=default_args,
    description='A DAG for cleaning and transforming all 9 tables',
    schedule_interval=None,
)

# Define SQL queries for cleaning and transforming
clean_and_transform_customers_query = """
-- Your SQL query for cleaning and transforming the customers table
DELETE FROM customers
WHERE (customer_id) IN (
    SELECT customer_id
    FROM customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
);
"""

clean_and_transform_orders_query = """
-- Your SQL query for cleaning and transforming the orders table
DELETE FROM orders
WHERE order_purchase_timestamp IS NULL OR order_purchase_timestamp < '2000-01-01';
"""

clean_and_transform_order_items_query = """
-- Your SQL query for cleaning and transforming the orders_items table
DELETE FROM orders_items
WHERE price IS NULL;
"""

clean_and_transform_sellers_query = """
-- Your SQL query for cleaning and transforming the sellers table
DELETE FROM sellers
WHERE seller_id IS NULL OR LENGTH(seller_id) <> 32 OR seller_zip_code_prefix IS NULL;
"""

clean_and_transform_geolocation_data_query = """
-- Your SQL query for cleaning and transforming the geolocation_data table
DELETE FROM geolocation
WHERE geolocation_lat IS NULL OR geolocation_lng IS NULL;
"""

clean_and_transform_product_categories_query = """
-- Your SQL query for cleaning and transforming the product_categories table
DELETE FROM product_categories
WHERE product_category_name IS NULL OR product_category_name IN (
    SELECT product_category_name
    FROM product_categories
    GROUP BY product_category_name
    HAVING COUNT(*) > 1
);
"""

clean_and_transform_reviews_query = """
-- Your SQL query for cleaning and transforming the reviews table
DELETE FROM review
WHERE review_score IS NULL OR review_score < 1 OR review_score > 5;
"""

# Define tasks
clean_and_transform_customers_task = PostgresOperator(
    task_id='clean_and_transform_customers_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_customers_query,
    dag=dag,
)

clean_and_transform_orders_task = PostgresOperator(
    task_id='clean_and_transform_orders_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_orders_query,
    dag=dag,
)

# Add more tasks for other tables

clean_and_transform_order_items_task = PostgresOperator(
    task_id='clean_and_transform_order_items_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_order_items_query,
    dag=dag,
)

clean_and_transform_sellers_task = PostgresOperator(
    task_id='clean_and_transform_sellers_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_sellers_query,
    dag=dag,
)

clean_and_transform_geolocation_data_task = PostgresOperator(
    task_id='clean_and_transform_geolocation_data_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_geolocation_data_query,
    dag=dag,
)

clean_and_transform_product_categories_task = PostgresOperator(
    task_id='clean_and_transform_product_categories_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_product_categories_query,
    dag=dag,
)

clean_and_transform_reviews_task = PostgresOperator(
    task_id='clean_and_transform_reviews_task',
    postgres_conn_id='PostgresWarehouse',
    sql=clean_and_transform_reviews_query,
    dag=dag,
)

# Set task dependencies
clean_and_transform_order_items_task >> clean_and_transform_reviews_task
clean_and_transform_product_categories_task >> clean_and_transform_reviews_task
clean_and_transform_geolocation_data_task >> clean_and_transform_orders_task
clean_and_transform_reviews_task >> []  # Add dependencies for other tables



# Continue the pattern for additional cleaning and transformation steps
