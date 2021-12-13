import json
import threading
import socket
from time import sleep
from database import *


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


# Метод для выбора типа чтения
def read_write(type):
    if type == 'text':
        read_write_from(file_paths[0], write_db_text, tables[0])
    elif type == 'pics':
        read_write_from(file_paths[1], write_db_pics, tables[1])
    elif type == 'tags':
        read_write_from(file_paths[2], write_db_tags, tables[2])


# Метод для чтения файла и записи его в таблицу
def read_write_from(file, func, table):
    data = {}
    # Если файл есть, то он прочитается и запишется в словарь
    try:
        with open(file, "r") as f:
            data = json.loads(f.read())
        for key, value in data.items():
            print(threading.current_thread().name + " — " + file)
            db = connect_db(path + db_name)
            cursor = db.cursor()
            func(cursor, key, value, table)
            cursor.close()
            db.commit()
            db.close()
            sleep(0.2)
    # Если файла нет, то выводится сообщение
    except IOError:
        print("Файла " + file + " нет, читать нечего")



# Метод для чтения. Используется как поток
def readproc():
    # Подключение к бд
    print("Подключаюсь к базе данных")
    db = connect_db(path + db_name)

    # Создание таблиц
    cursor = db.cursor()
    for table in tables:
        cursor.execute(create_table_query(table))

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
                print("\nПолучаю управление от первого процесса")

                text_thread = threading.Thread(target=read_write, args=("text",), daemon=True)
                pics_thread = threading.Thread(target=read_write, args=("pics",), daemon=True)
                tags_thread = threading.Thread(target=read_write, args=("tags",), daemon=True)
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
                print("Передаю управление первому процессу")
                conn.sendall((1).to_bytes(1, 'big'))
    except KeyboardInterrupt:
        print("\nЗакрываю сокет")
        sock.close()


if __name__ == '__main__':
    readproc()
