import requests

# Получаем посты
def get_posts(token, count=10, offset=0):
    params = {
        'access_token': token,
        'filters': 'post',
        'count': count,
        'start_from': offset,
        'v': '5.131'
    }
    return requests.get(
        'https://api.vk.com/method/newsfeed.get',
        params=params
    ).json()['response']['items']