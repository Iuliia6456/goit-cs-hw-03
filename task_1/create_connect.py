import psycopg2
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    try:
        conn = psycopg2.connect(database="task_1", user="postgres", password="123456789", host="localhost", port="5432")
        try:
            yield conn
        finally:
            conn.close()
    except psycopg2.OperationalError:
        print("Database connection error")
        raise

