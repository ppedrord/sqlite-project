"""

author: Pedro Paulo Monteiro Muniz Barbosa
e-mail: pedropaulommb@gmail.com

SQLite Operations - Tests

"""
import sqlite3
from sqlite3 import Error
import new_operations
import pytest

TABLE_PARAMETER = "{TABLE_PARAMETER}"
DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
GET_TABLES_SQL = "SELECT name FROM sqlite_schema WHERE type='table';"


def get_tables(con):
    cur = con.cursor()
    cur.execute(GET_TABLES_SQL)
    tables = cur.fetchall()
    cur.close()
    return tables


def delete_tables(con, tables):
    cur = con.cursor()
    for table, in tables:
        sql = DROP_TABLE_SQL.replace(TABLE_PARAMETER, table)
        if table != 'sqlite_sequence':
            cur.execute(sql)
    cur.close()


@pytest.fixture
def create_connection():
    """ create a database connection to the SQLite database specified by db_file
    :return: Connection object or None
    """
    db_file = r"C:\Users\Pedro Paulo\sqlite-project\base.db"
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    yield conn
    tables = get_tables(conn)
    delete_tables(conn, tables)


@pytest.fixture
def populate_project_table(create_connection):
    projects = [("MongoDB and Fixtures", "2022-02-07", "2022-02-17")]
    project_id = new_operations.create_project(create_connection, projects)
    print(project_id)
    return project_id


@pytest.fixture
def populate_projects_and_tasks(create_connection):
    projects = [("MongoDB and Fixtures", "2022-02-07", "2022-02-17"),
                ('Create a Flask Project', '2022-02-17', '2022-03-08'),
                ("Web Scraping + Data Analysis", "2022-03-07", "2022-04-07"),
                ("Learn SQL using SQLite3", "2022-04-14", None)]
    new_operations.create_project(create_connection, projects)
    task_00 = ('Perform a Presentation About Pytest Fixtures', 1, 1, 1, '2022-02-12', "2022-02-17")
    new_operations.create_task(create_connection, task_00)
    task_01 = ('Put the application on the web', 3, 1, 2, '2022-02-18', "2022-02-28")
    new_operations.create_task(create_connection, task_01)
    task_02 = ('Perform a Presentation About Web Scraping', 2, 1, 3, '2022-03-07', "2022-04-07")
    new_operations.create_task(create_connection, task_02)
    return True


project_0 = [("MongoDB and Fixtures", "2022-02-07", "2022-02-17")]
project_1 = [('Create a Flask Project', '2022-02-17', '2022-03-08'),
             ("Web Scraping + Data Analysis", "2022-03-07", "2022-04-07")]


def test_create_project(create_connection):
    assert new_operations.create_project(create_connection, project_0) == 1
    assert new_operations.create_project(create_connection, project_1) == 3


task_0 = ('Perform a Presentation About Pytest Fixtures', 1, 1, 1, '2022-02-12', "2022-02-17")
task_1 = ('Perform a Presentation About Web Scraping', 2, 1, 2, '2022-03-07', "2022-04-07")


def test_create_task(create_connection, populate_project_table):
    assert new_operations.create_task(create_connection, task_0) == 1
    assert new_operations.create_task(create_connection, task_1) == 2


project_to_find_00 = (1, "MongoDB and Fixtures", "2022-02-07", "2022-02-17")


def test_select_project_by_id(create_connection, populate_project_table):
    assert new_operations.select_project_by_id(create_connection, 1) == project_to_find_00


task_selected_0 = [(1, 'Perform a Presentation About Pytest Fixtures', 1, 1, '2022-02-12', '2022-02-17', 1)]


def test_select_task_by_begin_date(create_connection, populate_projects_and_tasks):
    assert new_operations.select_task_by_begin_date(create_connection, '2022-02-12') == task_selected_0


project_and_tasks_1 = [(1, 'Perform a Presentation About Pytest Fixtures', 1, 1, '2022-02-12', '2022-02-17',
                        'MongoDB and Fixtures', '2022-02-07', '2022-02-17')]


def test_select_project_and_its_tasks(create_connection, populate_projects_and_tasks):
    assert new_operations.select_project_and_its_tasks(create_connection, 1) == project_and_tasks_1


projects_with_tasks_01 = [
    ('MongoDB and Fixtures', '2022-02-07', '2022-02-17', 1, 'Perform a Presentation About Pytest Fixtures', 1, 1,
     '2022-02-12', '2022-02-17'),
    ('Create a Flask Project', '2022-02-17', '2022-03-08', 2, 'Put the application on the web', 3, 1, '2022-02-18',
     '2022-02-28'),
    ('Web Scraping + Data Analysis', '2022-03-07', '2022-04-07', 3, 'Perform a Presentation About Web Scraping', 2, 1,
     '2022-03-07', '2022-04-07')
    ]


def test_projects_that_have_tasks(create_connection, populate_projects_and_tasks):
    assert new_operations.select_projects_that_have_tasks(create_connection) == projects_with_tasks_01


all_tasks_and_projects = [
    ('MongoDB and Fixtures', '2022-02-07', '2022-02-17', 1, 'Perform a Presentation About Pytest Fixtures', 1, 1,
     '2022-02-12', '2022-02-17'),
    ('Create a Flask Project', '2022-02-17', '2022-03-08', 2, 'Put the application on the web', 3, 1, '2022-02-18',
     '2022-02-28'),
    ('Web Scraping + Data Analysis', '2022-03-07', '2022-04-07', 3, 'Perform a Presentation About Web Scraping', 2, 1,
     '2022-03-07', '2022-04-07'),
    ('Learn SQL using SQLite3', '2022-04-14', None, None, None, None, None, None, None)
    ]


def test_select_all_projects_and_tasks_related(create_connection, populate_projects_and_tasks):
    assert new_operations.select_all_projects_and_tasks_related(create_connection) == all_tasks_and_projects


def test_delete_projects_that_do_not_have_tasks(create_connection, populate_projects_and_tasks):
    assert new_operations.delete_projects_that_do_not_have_tasks(create_connection) == 1

