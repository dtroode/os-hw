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
path = '/Users/davidkistauri/Desktop/Education/ос/homework/data/'
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
def read(type):
    if type == 'text':
        read_from(file_paths[0], write_db_text)
    elif type == 'pics':
        read_from(file_paths[1], write_db_pics)
    elif type == 'tags':
        read_from(file_paths[2], write_db_tags)


# Метод для чтения файла и записи его в таблицу
def read_from(file, func):
    data = {}
    # Если файл есть, то он прочитается и запишется в словарь
    try:
        f = open(file, "r")
        data = json.loads(f.read())
        for key, value in data.items():
            print(threading.current_thread().name + " — " + file)
            db = connect_db()
            cursor = db.cursor()
            func(cursor, key, value)
            cursor.close()
            db.commit()
            db.close()
            sleep(0.2)
        f.close()
    # Если файла нет, то выводится сообщение
    except IOError:
        print("Файла " + file + " нет, читать нечего")


# Запись в базу данных текста новости
def write_db_text(cursor, key, value):
    insert = f"insert or ignore into {tables[0]} values (:key, :value)"
    cursor.execute(insert, (key, value))


# Запись в базу данных картинок новости
def write_db_pics(cursor, key, value):
    insert = f"insert or ignore into {tables[1]} values (:key, :value)"
    cursor.execute(insert, (key, json.dumps(value)))


# Запись в базу данных тегов и ссылок новости
def write_db_tags(cursor, key, value):
    insert = f"insert or ignore into {tables[2]} values (:key, :value)"
    cursor.execute(insert, (key, json.dumps(value)))


# Метод для чтения. Используется как поток
def readproc():
    # Подключение к бд
    print("Подключаюсь к базе данных")
    db = connect_db()

    # Создание таблиц
    cursor = db.cursor()
    cursor.execute(create_tables("text"))
    cursor.execute(create_tables("pics"))
    cursor.execute(create_tables("tags"))

    # Закрываемс соединение с бд
    # Для каждого потока в дальнейшем будет установлено новое соединение
    cursor.close()
    db.commit()
    db.close()

    # Создание сокета
    sock = socket.socket()
    # Настройки сокета, которые позволяют использовать один и тот же адрес
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen(1)

    try:
        while True:
            # Приём сигнала
            conn, addr = sock.accept()
            print('Подключен к сокету: ', addr)
            while True:
                data = conn.recv(1)
                if not data:
                    break
                print("Получаю управление от первого процесса")

                text_thread = threading.Thread(target=read, args=("text",))
                pics_thread = threading.Thread(target=read, args=("pics",))
                tags_thread = threading.Thread(target=read, args=("tags",))
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

                # Передача управления другому потоку
                conn.sendall((1).to_bytes(1, 'big'))
    except KeyboardInterrupt:
        print("Закрываю сокет")
        sock.close()


if __name__ == '__main__':
    readproc()
