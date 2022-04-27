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
import pprint
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
            project_begin_date   TEXT    NOT NULL,
            project_end_date     TEXT
        )
    """
    cur = conn.cursor()
    cur.execute(table_project)
    sql = ''' INSERT INTO projects(project_name, project_begin_date, project_end_date)
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
    sql = ''' INSERT INTO tasks(task_name, priority, status_id, project_id, begin_date, end_date)
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


def select_project_and_its_tasks(conn, project_id):
    """
        Find a project and its tasks by the project id
    :param conn: sqlite3.Connection
    :param project_id: The id of the specific project
    :return: The project selected and its tasks
    """
    sql = f"""
    SELECT  task_id,
            task_name,
            priority,
            status_id,
            begin_date,
            end_date,
            
            project_name, 
            project_begin_date, 
            project_end_date
            
    FROM tasks
    INNER JOIN projects ON projects.project_id = tasks.project_id
    WHERE projects.project_id = {project_id}"""
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data


def select_projects_that_have_tasks(conn):
    """
        Find a project and its tasks by the project id
    :param conn: sqlite3.Connection
    :return: The projects that have tasks
    """
    sql = """
    SELECT project_name,
           project_begin_date,
           project_end_date,
           task_id,
           task_name,
           priority,
           status_id,
           begin_date,
           end_date

    FROM projects
    LEFT JOIN tasks ON tasks.project_id = projects.project_id
    WHERE tasks.project_id IS NOT NULL"""
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data


def select_all_projects_and_tasks_related(conn):
    """
        Find a project and its tasks by the project id
    :param conn: sqlite3.Connection
    :return: The projects and tasks organized
    """
    sql = """
    SELECT project_name,
           project_begin_date,
           project_end_date,
           task_id,
           task_name,
           priority,
           status_id,
           begin_date,
           end_date
           
    FROM projects
    LEFT JOIN tasks USING(project_id)
    UNION ALL 
    
    SELECT project_name,
           project_begin_date,
           project_end_date,
           task_id,
           task_name,
           priority,
           status_id,
           begin_date,
           end_date
           
    FROM tasks
    LEFT JOIN projects USING(project_id)
    WHERE tasks.project_id IS NULL
    """
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data


def delete_projects_that_do_not_have_tasks(conn):
    """
        Delete a project that don't have a task
    :param conn: sqlite3.Connection
    :return: 0 if the operation was completed correctly and 1 if it wasn't
    """
    cur = conn.cursor()
    sql_0 = """
        SELECT count(*)
        FROM projects
    """
    find_rows_before_tuple = cur.execute(sql_0)
    for i in find_rows_before_tuple:
        find_rows_before = i[0]
    sql_1 = """
        SELECT 
               project_name,
               project_begin_date,
               project_end_date,
               task_id,
               task_name,
               priority,
               status_id,
               begin_date,
               end_date

        FROM projects
        LEFT JOIN tasks ON tasks.project_id = projects.project_id
        WHERE tasks.project_id IS NULL"""
    cur.execute(sql_1)
    data = cur.fetchone()
    print(data)
    sql_2 = f"""
        DELETE FROM projects
        WHERE project_name LIKE '%{data[0]}%' 
    """
    cur.execute(sql_2)
    sql_3 = """
            SELECT count(*)
            FROM projects
        """
    find_rows_after_tuple = cur.execute(sql_3)
    for i in find_rows_after_tuple:
        find_rows_after = i[0]

    if find_rows_before > find_rows_after:
        print(find_rows_before, find_rows_after)

    return find_rows_before - find_rows_after


def main():
    database = r"C:\Users\Pedro Paulo\sqlite-project\base.db"

    # create a database connection
    conn = create_connection(database)
    with conn:
        # create a new project
        projects = [("MongoDB and Fixtures", "2022-02-07", "2022-02-17"),
                    ('Create a Flask Project', '2022-02-17', '2022-03-08'),
                    ("Web Scraping + Data Analysis", "2022-03-07", "2022-04-07"),
                    ("Learn SQL using SQLite3", "2022-04-14", None)]
        project_id = create_project(conn, projects)

        # tasks
        task_1 = ('Perform a Presentation About Pytest Fixtures', 1, 1, 1, '2022-02-12', "2022-02-17")
        task_2 = ('Put the application on the web', 3, 1, 2, '2022-02-18', "2022-02-28")
        task_3 = ('Perform a Presentation About Web Scraping', 2, 1, 3, '2022-03-07', "2022-04-07")

        # create tasks
        create_task(conn, task_1)
        create_task(conn, task_2)
        create_task(conn, task_3)

        # select project by id
        project_selected_01 = select_project_by_id(conn, 1)
        print("project_selected_01:", project_selected_01)
        project_selected_02 = select_project_by_id(conn, 3)
        print("project_selected_02:", project_selected_02, "\n")

        # select task by begin date
        tasks_selected = select_task_by_begin_date(conn, '2022-02-12')
        print("tasks_selected:", tasks_selected, "\n")

        # select project and its tasks by the id of the project
        project_and_tasks_01 = select_project_and_its_tasks(conn, 1)
        print("project_and_tasks_01:", project_and_tasks_01)
        project_and_tasks_02 = select_project_and_its_tasks(conn, 2)
        print("project_and_tasks_02:", project_and_tasks_02)
        project_and_tasks_03 = select_project_and_its_tasks(conn, 3)
        print("project_and_tasks_03:", project_and_tasks_03, "\n")

        # select project and its tasks by the id of the project
        projects_with_tasks_01 = select_projects_that_have_tasks(conn)
        print("projects_with_tasks_01:", projects_with_tasks_01, "\n")

        # select project and its tasks by the id of the project
        all_projects_and_tasks = select_all_projects_and_tasks_related(conn)
        print("all_projects_and_tasks:", all_projects_and_tasks, "\n")

        # delete the project that don't have tasks related
        project_deleted = delete_projects_that_do_not_have_tasks(conn)

    conn.close()


if __name__ == '__main__':
    main()
