import json
import sqlite3
import threading
import socket
from time import sleep
from sqlite3 import Error


# Лок для записи в базу данных
db_lock = threading.Lock()


# Адрес сокета
address = ('', 8890)

# Файлы для чтения
path = '/Users/davidkistauri/Desktop/'
file_names = ['file1.json', 'file2.json', 'file3.json']
file_paths = []
for i in file_names:
    file_paths.append(path + i)

# База данных и таблицы
db_name = 'db.sqlite'
tables = ['table1', 'table2', 'table3']


# Подключение к базе данных SQLite
def connect_db():
    connection = None
    try:
        connection = sqlite3.connect(path + db_name, check_same_thread=False)
        print("Подключение к базе данных")
        return connection
    except Error as e:
        print(f"Произошка ошибка '{e}'.")


# Функция для генерации запросов создания таблиц
def create_tables(type):
    query = """
    create table if not exists {}(
        post_id integer primary key,
        post_tags text,
        unique (post_id, post_tags)
    )
    """
    if type == 'text':
        return query.format(tables[0])
    elif type == 'pics':
        return query.format(tables[1])
    elif type == 'tags':
        return query.format(tables[2])


# Метод для выбора типа чтения
def read(type, db):
    if type == 'text':
        read_from(file_paths[0], write_db_text, db)
    elif type == 'pics':
        read_from(file_paths[1], write_db_pics, db)
    elif type == 'tags':
        read_from(file_paths[2], write_db_tags, db)


# Метод для чтения файла и записи его в таблицу
def read_from(file, func, db):
    data = {}
    # Если файл есть, то он прочитается и запишется в словарь
    try:
        f = open(file, "r")
        data = json.loads(f.read())
        for key, value in data.items():
            print(threading.current_thread().name + " — " + file)
            func(db, key, value)
            sleep(0.5)
        f.close()
    # Если файла нет, то выводится сообщение
    except IOError:
        print("Файла " + file + " нет, читать нечего")


# Запись в базу данных текста новости
def write_db_text(db, key, value):
    insert = f"insert or ignore into {tables[0]} values (:key, :value)"
    db_lock.acquire()
    cursor = db.cursor()
    cursor.execute(insert, (key, value))
    db_lock.release()


# Запись в базу данных картинок новости
def write_db_pics(db, key, value):
    insert = f"insert or ignore into {tables[1]} values (:key, :value)"
    db_lock.acquire()
    cursor = db.cursor()
    cursor.execute(insert, (key, json.dumps(value)))
    db_lock.release()


# Запись в базу данных тегов и ссылок новости
def write_db_tags(db, key, value):
    insert = f"insert or ignore into {tables[2]} values (:key, :value)"
    db_lock.acquire()
    cursor = db.cursor()
    cursor.execute(insert, (key, json.dumps(value)))
    db_lock.release()


# Метод для чтения. Используется как поток
def readproc():
    # Подключение к бд
    db = connect_db()

    # Создание таблиц
    cursor = db.cursor()
    cursor.execute(create_tables("text"))
    cursor.execute(create_tables("pics"))
    cursor.execute(create_tables("tags"))

    # Создание сокета
    sock = socket.socket()
    # Настройки сокета, которые позволяют использовать один и тот же адрес
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen(1)

    # Приём сигнала
    conn, addr = sock.accept()

    try:
        print('Подключен к сокету: ', addr)
        while True:
            data = conn.recv(1)
            if not data:
                break
            print("Получаю управление от первого процесса")

            text_thread = threading.Thread(target=read, args=("text", db))
            pics_thread = threading.Thread(target=read, args=("pics", db))
            tags_thread = threading.Thread(target=read, args=("tags", db))
            text_thread.name = "п2т1"
            pics_thread.name = "п2т2"
            tags_thread.name = "п2т3"

            print("Запускаю потоки для чтения")
            text_thread.start()
            pics_thread.start()
            tags_thread.start()

            # Синхронизация, чтоб процесс чтения дождался пока всё прочитается
            text_thread.join()
            pics_thread.join()
            tags_thread.join()

            db.commit()

            # Передача управления другому потоку
            conn.sendall((1).to_bytes(1, 'big'))
    except KeyboardInterrupt:
        print("\nЗакрываю соединение с базой данных")
        db.close()
        print("Закрываю сокет")
        sock.close()


if __name__ == '__main__':
    readproc()
