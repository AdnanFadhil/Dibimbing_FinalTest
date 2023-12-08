from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from os import listdir
from os.path import isfile, join
from sqlalchemy import create_engine

def extract_and_load(*args, **kwargs):
    path = "/opt/airflow/data"
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

    df_product = pd.DataFrame()
    df_product_category = pd.DataFrame()
    df_supplier = pd.DataFrame()

    for i in onlyfiles:
        if ".xls" in i and "product" in i:
            if "product_category" in i:
                df_product_category = pd.read_excel(path + "/" + i)
            else:
                df_product = pd.read_excel(path + "/" + i)

        # supplier (xls)
        if ".xls" in i and "supplier" in i:
            df_supplier = pd.read_excel(path + "/" + i)

    connection = psycopg2.connect(
        host="dataeng-warehouse-postgres",
        port=5432,
        dbname="data_warehouse",
        user="user",
        password="password"
    )

    cursor = connection.cursor()
    engine = create_engine('postgresql://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    tables = {
        'product': df_product,
        'product_categories': df_product_category,
        'supplier': df_supplier,
    }

    for table_name, df in tables.items():
        df.to_sql(table_name, engine, index=False, if_exists='replace')

    cursor.close()
    connection.commit()
    connection.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='extract_and_load_xls_to_postgres',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

task_1 = PythonOperator(
    task_id='extract_and_load',
    python_callable=extract_and_load,
    provide_context=True,
    dag=dag
)

task_1
