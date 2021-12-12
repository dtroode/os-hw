import json
import sqlite3
from sqlite3 import Error

# Подключение к базе данных SQLite
def connect_db(db):
    connection = None
    try:
        connection = sqlite3.connect(db, check_same_thread=False)
        return connection
    except Error as e:
        print(f"Произошка ошибка '{e}'.")


# Функция для генерации запросов создания таблиц
def create_table_query(name):
    return f"""
    create table if not exists {name}(
        post_id integer primary key,
        post_tags text,
        unique (post_id, post_tags)
    )
    """

# Запись в базу данных текста новости
def write_db_text(cursor, key, value, table):
    insert = f"insert or ignore into {table} values (:key, :value)"
    cursor.execute(insert, (key, value))


# Запись в базу данных картинок новости
def write_db_pics(cursor, key, value, table):
    insert = f"insert or ignore into {table} values (:key, :value)"
    cursor.execute(insert, (key, json.dumps(value)))


# Запись в базу данных тегов и ссылок новости
def write_db_tags(cursor, key, value, table):
    insert = f"insert or ignore into {table} values (:key, :value)"
    cursor.execute(insert, (key, json.dumps(value)))