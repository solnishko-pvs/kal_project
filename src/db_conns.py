import psycopg2
from psycopg2.extensions import connection

def get_pg_conn(db_name: str, user_name: str, password: str, host_name: str, port: str) -> connection:

    conn = psycopg2.connect(
        database=db_name, user=user_name, password=password, host=host_name, port=port
    )

    return conn

