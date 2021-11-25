import vk_api
import json
import re
import threading

# Файлы, в которые будем записывать
path = '/Users/davidkistauri/Desktop/'
file_names = ['file1.json', 'file2.json', 'file3.json']
file_paths = []
for i in file_names:
    file_paths.append(path + i)

# Метод создания сессии с ВК
def login():
    login
    pwd
    vk_session = vk_api.VkApi(login, pwd)
    vk_session.auth()
    vk = vk_session.get_api()
    return vk

# Метод для подготовки ленты
# От АПИ получаем необходимое количество постов,
# оставляем только те, которые действительно являются постами
def get_feed(vk, count):
    feed = list(
        filter(
            lambda item: item['type'] == 'post',
            vk.newsfeed.get(count=count)['items']))
    return feed

# Метод для выбора типа записи
def write(feed, type):
    if type == 'text':
        write_to(feed, collect_text, file_paths[0])
    elif type == 'pics':
        write_to(feed, collect_pics, file_paths[1])
    elif type == 'tags':
        write_to(feed, collect_tags, file_paths[2])

# Метод для записи и вызова необходимого метода сбора данных
def write_to(feed, func, file):
    data = {}
    # Если файл есть, то он прочитается и запишется в словарь
    try:
        f = open(file, "r")
        data = json.loads(f.read())
    # Если файла нет, работа продолжится с пустым словарём
    except IOError:
        print("Файла для записи текста нет, создам его")
    # Функция соберёт данные и добавит их в словарь
    data = func(feed, data)
    # Файл перезапишется, вместо него будет наш словарь
    f = open(file, "w")
    f.write(json.dumps(data))
    f.close()


# Методы для сбора. Трай-кэтч для обработки случаев, когда необходимого поля нет

# Метод для сбора текста
def collect_text(feed, data):
    for p in feed:
        try:
            data[p['post_id']] = p['text']
        except KeyError:
            data[p['post_id']] = ""
    return data

# Метод для сборка картинок
# Берётся первая картинка (возможно не самого лучшего качества)
def collect_pics(feed, data):
    for p in feed:
        data[p['post_id']] = []
        try:
            for a in p['attachments']:
                if a['type'] == 'photo':
                    data[p['post_id']].append(a['photo']['sizes'][0]['url'])
        except KeyError:
            data[p['post_id']] = []
    return data

# Метод для сбора хэштегов
def collect_tags(feed, data):
    for p in feed:
        try:
            # Регулярка для хэштега: символ, буквы, цифры, подчёркивание
            matches = re.findall(r'#[а-яА-Яa-zA-Z\d_]+', p['text'])
            matches = list(map(lambda tag: tag.replace("#", ""), matches))
            if not matches:
                data[p['post_id']] = []
            else:
                data[p['post_id']] = matches
        except KeyError:
            data[p['post_id']] = []
    return data


# Метод для запуска потоков записи
def write_data(feed):
    text_thread = threading.Thread(target=write, args=(feed, "text"))
    pics_thread = threading.Thread(target=write, args=(feed, "pics"))
    tags_thread = threading.Thread(target=write, args=(feed, "tags"))
    text_thread.start()
    pics_thread.start()
    tags_thread.start()

def main():
    vk = login()
    feed = get_feed(vk, 5)
    write_data(feed)

main()