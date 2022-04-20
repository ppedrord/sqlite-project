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


def commit_close(func):
    def decorator(*args):
        con = sqlite3.connect('base.db')
        cur = con.cursor()
        try:
            table = """
                CREATE TABLE users (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
                )
            """
            cur.execute(table)
        except:
            pass
        sql = func(*args)
        cur.execute(sql)
        con.commit()
        con.close()

    return decorator


@commit_close
def db_insert(name, phone, email):
    return f"""
    INSERT INTO users(name, phone, email)
        VALUES('{name}', '{phone}', '{email}')
    """


db_insert("Pedro", "999999999", "pedro@gmail.com")
db_insert("Lauro", "999999998", "lauro@gmail.com")


@commit_close
def db_update(name, email):
    return f"""
    UPDATE users SET name = '{name}' WHERE email = '{email}'
    """


db_update("Pedro Paulo", "pedro@gmail.com")


@commit_close
def db_delete(email):
    return f"""
    DELETE FROM users WHERE email='{email}'
    """


def db_select(data, field):
    con = sqlite3.connect('base.db')
    cur = con.cursor()
    sql = f"""
    SELECT id, name, phone, email
    FROM users
    WHERE {field}={data}"""

    cur.execute(sql)
    data = cur.fetchall()
    con.close()
    return data
