import logging
from psycopg2 import DatabaseError
from create_connect import get_db_connection

def create_table():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        fullname VARCHAR(100),
                        email VARCHAR(100) UNIQUE 
                    );""")
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS status (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(50) UNIQUE 
                    );""")
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tasks (
                        id SERIAL PRIMARY KEY,
                        title VARCHAR(100),
                        description TEXt,
                        status_id INTEGER REFERENCES status(id) 
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
                        user_id INTEGER REFERENCES users(id)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
                    );""")
                
                conn.commit()
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()
           

create_table()