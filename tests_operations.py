"""

author: Pedro Paulo Monteiro Muniz Barbosa
e-mail: pedropaulommb@gmail.com

SQLite Operations - Tests

"""
import sqlite3
import operations
import pytest

con = sqlite3.connect('base.db')


@pytest.fixture
def create_table():
    cur = con.cursor()
    table = """
                CREATE TABLE users (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL 
                )
            """
    cur.execute(table)


users_insert = [('Pedro', '999999999', 'pedro@gmail.com'),
                ('Lauro', '999999998', 'lauro@gmail.com')]


def test_db_insert(create_table):
    assert operations.db_insert(name, phone, email) == expected
