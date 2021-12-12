import threading
import re

# Методы для сбора. Трай-кэтч для обработки случаев, когда необходимого поля нет

# Метод для сбора текста  (первый поток)
def collect_text(post, data):
    print("\33[41m" + threading.current_thread().name + " — файл 1" + "\33[0m")
    # Пробуем посмотреть post['text']
    try:
        data[post['post_id']] = post['text']
    except KeyError:
        data[post['post_id']] = ""
    return data


# Метод для сборка картинок (второй поток)
def collect_pics(post, data):
    print("\33[42m" + threading.current_thread().name + " — файл 2" + "\33[0m")
    data[post['post_id']] = []
    # Пробуем посмотреть post['attachments']
    try:
        for a in post['attachments']:
            if a['type'] == 'photo':
                data[post['post_id']].append(a['photo']['sizes'][0]['url'])
    except KeyError:
        data[post['post_id']] = []
    return data


# Метод для сбора хэштегов (третий поток)
def collect_tags(post, data):
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
    return data
