import logging
from psycopg2 import DatabaseError
from create_connect import get_db_connection
from random import randint  
from faker import Faker

fake = Faker("uk-UA")
count = 100

def insert_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO status (name) VALUES 
                        ('new'),
                        ('in progress'),
                        ('completed');""")
                
                for _ in range(count):
                    fullname = fake.name()
                    email = fake.email() 
                    titel = fake.sentence()
                    description = fake.text()
                    status_id = randint(1, 3)
                        
                    cur.execute("""
                        INSERT INTO users (fullname, email) VALUES (%s, %s) RETURNING id; 
                        """, (fullname, email))
                    user_id = cur.fetchone()[0]

                    cur.execute("""
                        INSERT INTO tasks (title, description, status_id, user_id) VALUES (%s, %s, %s, %s);
                        """, (titel, description, status_id, user_id))                   
                    
                conn.commit()
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()


insert_data()