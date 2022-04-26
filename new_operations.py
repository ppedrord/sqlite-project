"""

author: Pedro Paulo Monteiro Muniz Barbosa
e-mail: pedropaulommb@gmail.com

SQLite Operations

DML - Data Manipulation
C - CREATE
R - READ
U - UPDATE
D - DELETE

"""
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_project(conn: sqlite3.Connection, project):
    """
    Create a new project into the projects table
    :param conn: sqlite3.Connection
    :param project: The information of the project
    :return: The project id
    """
    table_project = """
        CREATE TABLE IF NOT EXISTS projects (
            project_id   INTEGER PRIMARY KEY,
            project_name TEXT    NOT NULL,
            begin_date   TEXT    NOT NULL,
            end_date     TEXT
        )
    """
    cur = conn.cursor()
    cur.execute(table_project)
    sql = ''' INSERT INTO projects(project_name,begin_date,end_date)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    if len(project) == 1:
        cur.execute(sql, project[0])
        conn.commit()
    elif len(project) > 1:
        for i in project:
            cur.execute(sql, i)
            conn.commit()
    return cur.lastrowid


def create_task(conn, task):
    """
    Create a new task
    :param conn: sqlite3.Connection
    :param task: The information of the task
    :return: The task id
    """
    table_task = """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id        INTEGER PRIMARY KEY,
            task_name      TEXT    NOT NULL,
            priority       INTEGER NOT NULL,
            status_id      INTEGER NOT NULL,
            begin_date     TEXT,
            end_date TEXT,
            project_id     INTEGER NOT NULL,
            FOREIGN KEY (
                project_id
            )
            REFERENCES projects (project_id) ON UPDATE CASCADE
                                             ON DELETE CASCADE
        )
            """
    cur = conn.cursor()
    cur.execute(table_task)
    sql = ''' INSERT INTO tasks(task_name,priority,status_id,project_id,begin_date,end_date)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid


def main():
    database = r"C:\Users\Pedro Paulo\sqlite-project\base.db"

    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new project
        project = ('Cool App with SQLite & Python', '2015-01-01', '2015-01-30')
        project_id = create_project(conn, project)

        # tasks
        task_1 = ('Analyze the requirements of the app', 1, 1, project_id, '2015-01-01', '2015-01-02')
        task_2 = ('Confirm with user about the top requirements', 1, 1, project_id, '2015-01-03', '2015-01-05')

        # create tasks
        create_task(conn, task_1)
        create_task(conn, task_2)

    conn.close()


if __name__ == '__main__':
    main()
