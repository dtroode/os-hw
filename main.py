from time import sleep
import threading
import socket
import json
from collect import *
from get_posts import get_posts


# Локи для процессов
text = threading.Lock()
pics = threading.Lock()
tags = threading.Lock()

# Барьер для контроля количества итераций четвёртого процесса
# Когда три потока пройдут барьер, завершить четвёртый процесс
barrier = threading.Barrier(4)


# Адрес сокета
address = ('localhost', 8890)

# Файлы, в которые будем записывать
path = './data/'
file_names = ['file1.json', 'file2.json', 'file3.json']
file_paths = []
for i in file_names:
    file_paths.append(path + i)

# Количество постов
count = 10

# Токен для получения данных :)
TOKEN = 'b90e9d2bda67118b2c50aa83d48a9c5aaf6f7f5ea716103b0ff2ffe22392ef7d8d3198e55abbb86b1f133'


# Метод для выбора типа записи
def write(feed, type, e):
    if type == 'text':
        write_to(feed, collect_text, file_paths[0], text, e)
    elif type == 'pics':
        write_to(feed, collect_pics, file_paths[1], pics, e)
    elif type == 'tags':
        write_to(feed, collect_tags, file_paths[2], tags, e)


# Метод для записи и вызова необходимого метода сбора данных
def write_to(feed, func, file, locker, e):
    data = {}
    for p in feed:
        # Если файл есть, то он прочитается и запишется в словарь
        try:
            with open(file, "r") as f:
                filedata = f.read()
                data = json.loads(filedata) if len(filedata) != 0 else {}
                # print(data)
        # Если файла нет, работа продолжится с пустым словарём
        except IOError:
            print("Файла для записи текста нет, создам его")
        locker.acquire()
        # Функция соберёт данные и добавит их в словарь
        data = func(p, data)
        # Файл перезапишется, вместо него будет наш словарь
        with open(file, "w") as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=4))
        sleep(0.2)
        locker.release()
        e.set()
    barrier.wait()

def read_from(file, locker, color="\33[43m"):
    # Пробуем получить лок на файл. Если не получаем, ждём
    if locker.acquire():
        with open(file, "r") as f:
            print(color + threading.current_thread().name + " – " + file + ": " + str(len(json.loads(f.read()))) + "\33[0m")
        sleep(0.2)
        locker.release()

# Метод для чтения файлов между итерациями записи (четвёртый поток)
def read(e):
    i = 0
    e.wait()
    # barrier.n_waiting – количество потоков, которые дошли до wait()
    # barrier.parties – необходимое количество потоков для прохождения
    # Когда останется только 1 поток (этот), завершить итерацию
    while barrier.n_waiting != barrier.parties - 1:
        # Переменная i отвечает за действие. 0 – читать текст, 1 – читать картинки, 2 – читать ссылки и тэги
        if i == 0:
            read_from(file_paths[0], text)
            i = 1
        elif i == 1:
            read_from(file_paths[1], pics)
            i = 2
        else:
            read_from(file_paths[2], tags)
            i = 0
    barrier.wait()


# Метод для записи. Используется как поток
def writeproc():
    sock = None

    try:
        # Сокет на котором слушает второй процесс
        sock = socket.socket()
        sock.connect(address)
    except ConnectionRefusedError:
        print("Не удалось подключиться к серверу для записи в базу данных")

    try:
        # Бесконечно получаем и записываем новые посты
        while True:
            # Ивент для того, чтоб четвёртый поток не забрал лок самым первым
            e4 = threading.Event()

            print("Получаю посты")
            feed = get_posts(TOKEN, count)

            # Создание потоков, запись: текста новости, картинок, тэгов и ссылок – и чтение
            text_thread = threading.Thread(
                target=write, args=(feed, "text", e4), daemon=True)
            pics_thread = threading.Thread(
                target=write, args=(feed, "pics", e4), daemon=True)
            tags_thread = threading.Thread(
                target=write, args=(feed, "tags", e4), daemon=True)
            read_thread = threading.Thread(target=read, args=(e4,), daemon=True)
            text_thread.name = "п1т1"
            pics_thread.name = "п1т2"
            tags_thread.name = "п1т3"
            read_thread.name = "п1т4"

            print("Запускаю потоки для записи и чтения")
            text_thread.start()
            pics_thread.start()
            tags_thread.start()
            read_thread.start()

            # Синхронизация, чтоб процесс записи дождался пока всё запишется
            text_thread.join()
            pics_thread.join()
            tags_thread.join()
            read_thread.join()

            # Передача управления другому потоку
            print("Передаю управление второму процессу")
            try:
                # Отправляем сигнал
                sock.sendall((1).to_bytes(1, 'big'))
                # Ждём ответ от сервера, по сути неважно какой
                data = int.from_bytes(sock.recv(1), 'big')
            except BrokenPipeError:
                print("Ответ на сервер не отправился, попробую подключиться ещё раз")
                try:
                    sock = socket.socket()
                    sock.connect(address)
                except:
                    print("Подключиться к серверу не удалось")
    except KeyboardInterrupt:
        print("\nОстанавливаю программу")
        


if __name__ == '__main__':
    writeproc()
