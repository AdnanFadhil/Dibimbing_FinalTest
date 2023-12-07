from airflow.providers.postgres.hooks import postgres as PostgresHook
from sqlalchemy import create_engine

def create_table_in_postgres_with_connection(conn_id, table_name, table_schema):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    try:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})"
        cursor.execute(create_table_query)
        connection.commit()
    finally:
        cursor.close()
        connection.close()

def load_data_to_postgres_with_connection(conn_id, table_name, data_frame):
    engine = create_engine(PostgresHook.get_uri(conn_id))
    data_frame.to_sql(table_name, engine, if_exists='replace', index=False)
