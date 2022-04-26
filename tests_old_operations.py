"""

author: Pedro Paulo Monteiro Muniz Barbosa
e-mail: pedropaulommb@gmail.com

SQLite Operations - Tests

"""
import sqlite3
import old_operations
import pytest


@pytest.fixture
def create_table_users():
    con = sqlite3.connect('base.db')
    cur = con.cursor()
    table_users = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL
    )
        """
    cur.execute(table_users)
    yield table_users
    drop_table_users = """
        DROP TABLE users
    """
    cur.execute(drop_table_users)


@pytest.fixture
def create_table_addresses():
    con = sqlite3.connect('base.db')
    cur = con.cursor()
    table_addresses = """
    CREATE TABLE IF NOT EXISTS addresses (
        address_id INTEGER PRIMARY KEY,
        house_no TEXT,
        street TEXT,
        city TEXT,
        postal_code TEXT,
        country TEXT
    )
            """
    cur.execute(table_addresses)
    yield table_addresses
    drop_table_addresses = """
        DROP TABLE addresses
        """
    cur.execute(drop_table_addresses)


users_01 = [("Pedro", "999999999", "pedro@gmail.com"),
            ("Lauro", "999999998", "lauro@gmail.com")]


@pytest.mark.parametrize(["name", "phone", "email"], users_01)
def test_db_insert_to_users(create_table_users, name, phone, email):
    assert_value = old_operations.db_insert_to_users(name, phone, email)
    print(assert_value)
    assert assert_value is not None


addresses_01 = [("504", "Rua A", "Campos dos Goytacazes", "20000-000", "Brazil"),
                ("001", "Rua B", "Itaipava", "21000-001", "Brazil")]


@pytest.mark.parametrize(["house_no", "street", "city", "postal_code", "country"], addresses_01)
def test_db_insert_to_addresses(create_table_addresses, house_no, street, city, postal_code, country):
    assert_value_1 = old_operations.db_insert_to_addresses(house_no, street, city, postal_code, country)
    print(assert_value_1)
    assert assert_value_1 is not None


