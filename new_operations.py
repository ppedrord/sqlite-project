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
            begin_date     TEXT    NOT NULL,
            end_date       TEXT,
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


def select_project_by_id(conn, id_selected):
    """
        Find a project by the project id
    :param conn: sqlite3.Connection
    :param id_selected: The project id
    :return: The project selected
    """
    sql = f"""
    SELECT *
    FROM projects
    WHERE project_id = {id_selected}"""
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchone()
    return data


def select_task_by_begin_date(conn, begin_date):
    """
        Find a project by the project id
    :param conn: sqlite3.Connection
    :param begin_date: The date when the task was assigned
    :return: The project selected
    """
    sql = f"""
    SELECT *
    FROM tasks
    WHERE begin_date LIKE '%{begin_date}%'"""
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data


def main():
    database = r"C:\Users\Pedro Paulo\sqlite-project\base.db"

    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new project
        project = [("MongoDB and Fixtures", "2022-02-07", "2022-02-17"),
                   ("Web Scraping + Data Analysis", "2022-03-07", "2022-04-07")]
        project_id = create_project(conn, project)

        # tasks
        task_1 = ('Perform a Presentation About Pytest Fixtures', 1, 1, 1, '2022-02-12', "2022-02-17")
        task_2 = ('Perform a Presentation About Web Scraping', 2, 1, 3, '2022-03-07', "2022-04-07")

        # create tasks
        create_task(conn, task_1)
        create_task(conn, task_2)

        # select project by id
        project_selected_01 = select_project_by_id(conn, 1)
        print(project_selected_01)
        project_selected_02 = select_project_by_id(conn, 2)
        print(project_selected_02)

        # select task by begin date
        tasks_selected = select_task_by_begin_date(conn, '2022-02-12')
        print(tasks_selected)

    conn.close()


if __name__ == '__main__':
    main()
