from time import sleep
import threading
import requests
import socket
import json
import re


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

# Токен для получения данных
TOKEN = ''


# Получаем посты
def get_posts(offset):
    params = {
        'access_token': TOKEN,
        'filters': 'post',
        'count': count,
        'start_from': offset,
        'v': '5.131'
    }
    return requests.get(
        'https://api.vk.com/method/newsfeed.get',
        params=params
    ).json()['response']['items']


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
    # Если файл есть, то он прочитается и запишется в словарь
    try:
        f = open(file, "r")
        data = json.loads(f.read())
    # Если файла нет, работа продолжится с пустым словарём
    except IOError:
        print("Файла для записи текста нет, создам его")
    for p in feed:
        # Функция соберёт данные и добавит их в словарь
        data = func(p, data)
        # Файл перезапишется, вместо него будет наш словарь
        f = open(file, "w")
        f.write(json.dumps(data))
        f.close()
        locker.release()
        e.set()
        # iter_barrier.wait()
    barrier.wait()


# Методы для сбора. Трай-кэтч для обработки случаев, когда необходимого поля нет

# Метод для сбора текста  (первый поток)
def collect_text(post, data):
    text.acquire()
    print("\33[41m" + threading.current_thread().name + " — файл 1" + "\33[0m")
    # Пробуем посмотреть post['text']
    try:
        data[post['post_id']] = post['text']
    except KeyError:
        data[post['post_id']] = ""
    sleep(0.5)
    return data


# Метод для сборка картинок (второй поток)
def collect_pics(post, data):
    pics.acquire()
    print("\33[42m" + threading.current_thread().name + " — файл 2" + "\33[0m")
    data[post['post_id']] = []
    # Пробуем посмотреть post['attachments']
    try:
        for a in post['attachments']:
            if a['type'] == 'photo':
                data[post['post_id']].append(a['photo']['sizes'][0]['url'])
    except KeyError:
        data[post['post_id']] = []
    sleep(0.5)
    return data


# Метод для сбора хэштегов (третий поток)
def collect_tags(post, data):
    tags.acquire()
    print("\33[44m" + threading.current_thread().name + " — файл 3" + "\33[0m")

    attachments = []
    # Пробуем посмотреть post['attachments']
    try:
        # Получаем прикреплённые ссылки
        try:
            for a in post['attachments']:
                if a['type'] == 'link':
                    attachments.append(a['link']['url'])
        except KeyError:
            pass
    except KeyError:
        # Пробуем посмотреть post['text']
        try:
            # Регулярка для хэштегов: символ, буквы, цифры, подчёркивание
            hastags = re.findall(r'#[а-яА-Яa-zA-Z\d_]+', post['text'])
            for tag in hastags:
                attachments.append(
                    'https://vk.com/feed?section=search&q=' + tag.replace('#', '%23'))

            # Регулярка для ссылок из текста
            links = re.findall(
                r'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&\/\/=]*)',
                post['text'])
            for link in links:
                attachments.append(link)
        except KeyError:
            pass
    data[post['post_id']] = attachments
    sleep(0.5)
    return data


# Метод для чтения файлов между итерациями записи (четвёртый поток)
def read_files(e):
    i = 0
    e.wait()
    # barrier.n_waiting – количество потоков, которые дошли до wait()
    # barrier.parties – необходимое количество потоков для прохождения
    # Когда останется только 1 поток (этот), завершить итерацию
    while barrier.n_waiting != barrier.parties - 1:
        # Переменная i отвечает за действие. 0 – читать текст, 1 – читать картинки, 2 – читать ссылки и тэги
        if i == 0:
            # Пробуем получить лок на первый файл
            if text.acquire():
                f = open(file_paths[0], "r")
                print("\33[43m" + threading.current_thread().name +
                      " — файл 1: " + str(len(json.loads(f.read()))) + "\33[0m")
                f.close()
                sleep(0.5)
                text.release()
                i = 1
        elif i == 1:
            # Пробуем получить лок на второй файл
            if pics.acquire():
                f = open(file_paths[1], "r")
                print("\33[43m" + threading.current_thread().name +
                      " — файл 2: " + str(len(json.loads(f.read()))) + "\33[0m")
                print(f.read())
                f.close()
                sleep(0.5)
                pics.release()
                i = 2
        else:
            # Пробуем получить лок на третий файл
            if tags.acquire():
                f = open(file_paths[2], "r")
                print("\33[43m" + threading.current_thread().name +
                      " — файл 3: " + str(len(json.loads(f.read()))) + "\33[0m")
                print(f.read())
                f.close()
                sleep(0.5)
                tags.release()
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

    # Бесконечно получаем и записываем новые посты
    while True:
        print("Получаю управление от второго процесса")
        # Ивент для того, чтоб четвёртый поток не забрал лок самым первым
        e4 = threading.Event()

        print("Получаю посты")
        feed = get_posts(count)

        # Создание потоков, запись: текста новости, картинок, тэгов и ссылок – и чтение
        text_thread = threading.Thread(
            target=write, args=(feed, "text", e4))
        pics_thread = threading.Thread(
            target=write, args=(feed, "pics", e4))
        tags_thread = threading.Thread(
            target=write, args=(feed, "tags", e4))
        read_thread = threading.Thread(target=read_files, args=(e4,))
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


if __name__ == '__main__':
    writeproc()
