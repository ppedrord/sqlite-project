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


project_0 = [("MongoDB and Fixtures", "2022-02-07", "2022-02-2022")]
project_1 = [('Create a Flask Project', '2022-02-17', '2022-03-08'), ("Web Scraping + Data Analysis", "2022-03-07", "2022-04-07")]


def test_create_project(create_connection):
    assert new_operations.create_project(create_connection, project_0) == 1
    breakpoint()
    assert new_operations.create_project(create_connection, project_1) == 3
    breakpoint()
