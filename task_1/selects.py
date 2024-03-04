import logging
from psycopg2 import DatabaseError
from create_connect import get_db_connection

def find_task_by_userid(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT title, description 
                    FROM tasks 
                    WHERE user_id = %s;""", (user_id,))
                rows = cur.fetchall()
                if not rows:
                    print(f"\nNo users found.\n")
                else:
                    for row in rows:
                        print(f"\nTasks for user_id = {user_id}: {row}\n")

                conn.commit()

            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def delete_user_by_id(id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    DELETE FROM users WHERE id = %s; 
                """, (id,))

                if cur.rowcount > 0:
                    print(f"\nUser with id = {id} is deleted\n")
                else:
                    print(f"\nNo user found with id = {id}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def find_tasks_by_status(status_name):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT id, title, description 
                    FROM tasks 
                    WHERE status_id = (SELECT id FROM status WHERE name = %s)
                    ORDER BY title;
                """, (status_name,))
                
                rows = cur.fetchall()

                if not rows:
                    print(f"\nNo tasks found.\n")
                else:
                    for row in rows:
                        print(f"\nTasks with status '{status_name}': {row}\n")
    
                conn.commit()

            except DatabaseError as e:
                logging.error(e)

def update_status_of_task(status_name, task_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    UPDATE tasks SET status_id = (SELECT id FROM status WHERE name = %s) 
                    WHERE id = %s;""", (status_name, task_id))
                
                print(f"\nStatus for task with id = {task_id} is updated to '{status_name}'\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def set_title_description_to_NULL(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    UPDATE tasks SET title = NULL, description = NULL
                    WHERE user_id = %s;
                """, (user_id,))

                print(f"\nTitle and description for tasks with user_id = {user_id} are deleted\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def find_user_without_tasks():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT id, fullname
                    FROM users
                    WHERE id NOT IN (SELECT user_id FROM tasks WHERE title IS NOT NULL or description IS NOT NULL);
                """)
                rows = cur.fetchall()
                if not rows:
                    print("\nNo users without tasks found.\n")
                else:
                    for row in rows:
                        print(f"\nUsers without tasks: {row}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def add_new_tasks(title, description, user_id, status_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO tasks (title, description, user_id, status_id) 
                    VALUES (%s, %s, %s, %s);
                """, (title, description, user_id, status_id))

                conn.commit()
                print(f"\nNew task added for the user with id = {user_id}.\n")

            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_uncompleted_tasks():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT user_id
                    FROM tasks
                    WHERE status_id = 1 OR status_id = 2;
                """)
                rows = cur.fetchall()
                
                if not rows:
                    print("\nNo users with uncompleted tasks found.\n")
                else:
                    user_ids = [row[0] for row in rows]
                    user_ids_str = ', '.join(map(str, set(user_ids)))
                    print(f"\nUsers with uncompleted tasks: {user_ids_str}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def delete_task(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    DELETE FROM tasks WHERE user_id = %s; 
                """, (user_id,))

                if cur.rowcount > 0:
                    print(f"\nTasks for user_id = {user_id} are deleted\n")
                else:
                    print(f"\nNo tasks found for user_id = {user_id}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_user_by_email(sample):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT fullname, email
                    FROM users
                    WHERE email LIKE %s;
                """, (sample,))
                rows = cur.fetchall()
                
                if not rows:
                    print("\nNo users found.\n")
                else:
                    for row in rows:
                        print(f"\nUsers found: {row}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def update_fullname(new_name, user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    UPDATE users SET fullname = %s
                    WHERE id = %s;
                """, (new_name, user_id,))
                print(f"\nThe fullname for user_id = {user_id} is updated\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_tasks_count_by_status():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT count(*), status_id 
                    FROM tasks
                    GROUP BY status_id
                    ;
                """)
                rows = cur.fetchall()
                
                for row in rows:
                    print(f"\nCount of tasks by status_id: count = {row[0]}, status_id = {row[1]}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_tasks_count_domain_depended(email_domain):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT tasks.id
                    FROM tasks
                    JOIN users ON tasks.user_id = users.id
                    WHERE users.email LIKE %s;
                """, ('%' + email_domain,))
                rows = cur.fetchall()
                
                if not rows:
                    print(f"\nNo tasks found with empty description.\n")
                else:
                    task_ids = [row[0] for row in rows]
                    task_ids_str = ', '.join(map(str, task_ids))
                    print(f"\nTasks with {email_domain} domain: {task_ids_str}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_tasks_without_decription():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT tasks.id
                    FROM tasks
                    WHERE description IS NULL;
                """)
                rows = cur.fetchall()
                
                if not rows:
                    print(f"\nNo tasks found with empty description.\n")
                else:
                    print(f"\nTasks with empty description: {rows}\n")

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_users_tasks_in_progress(status_name):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT tasks.title, users.fullname
                    FROM tasks
                    JOIN users ON tasks.user_id = users.id
                    WHERE tasks.status_id = (SELECT id FROM status WHERE name = %s);
                """, (status_name,))
                rows = cur.fetchall()
                
                if not rows:
                    print(f"\nNo tasks and users found.\n")
                else:
                    for row in rows:
                        print(f"\nUser: {row[1]}, task: {row[0]}\n")
                        

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()

def select_users_and_tasks_count():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT users.id, users.fullname, COUNT(tasks.id) 
                    FROM users
                    LEFT JOIN tasks ON users.id = tasks.user_id
                    GROUP BY users.id, users.fullname;
                """)
                rows = cur.fetchall()
                
                if not rows:
                    print(f"\nNo tasks and users found.\n")
                else:
                    for row in rows:
                        print({"User_id": row[0], "User_fullname": row[1], "Tasks_count": row[2]})
                        

                conn.commit()
            
            except DatabaseError as e:
                logging.error(e)
                conn.rollback()
            
# find_task_by_userid(user_id=85)
# delete_user_by_id(id=2)
# find_tasks_by_status(status_name='new')
# update_status_of_task(status_name='in progress', task_id=4)
# set_title_description_to_NULL(user_id=10)
# find_user_without_tasks()
# add_new_tasks(title="Story", description="Story description", user_id=2, status_id=1)
# select_uncompleted_tasks()
# delete_task(user_id=13)
# select_user_by_email(sample='%b%@example.com')
# update_fullname(new_name='Ivan Borcsh', user_id=1)
# select_tasks_count_by_status()
# select_tasks_count_domain_depended(email_domain='@example.net')
# select_tasks_without_decription()
# select_users_tasks_in_progress(status_name='in progress')
# select_users_and_tasks_count()