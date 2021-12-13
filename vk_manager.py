import requests
import vk_api
import user_data
from re import findall


def captcha_handler(captcha):
    key = input(f"Enter captcha code {captcha.get_url()}: ")
    return captcha.try_again(key)


def get_news_list():
    try:
        vk_session = vk_api.VkApi(user_data.login, user_data.password, captcha_handler=captcha_handler)
        vk_session.auth()
        vk = vk_session.get_api()
        data = vk.newsfeed.get(filters='post')['items']
        return data[-(len(data) % 3 + 1)::-1]
    except requests.exceptions.ConnectionError:
        return []


def get_photos_list(news_row):
    photos = []
    try:
        attachments = news_row['attachments']
        for attachment in attachments:
            if attachment['type'] == 'photo':
                photos.append(max(attachment['photo']['sizes'], key=lambda x: x['height'])['url'])
    except KeyError:
        pass
    return photos


def get_links_list(news_row):
    links = findall(r'#\w+', news_row['text'])
    try:
        links += list(
            map(lambda x: x['link']['url'], list(filter(lambda x: x['type'] == 'link', news_row['attachments']))))
    except KeyError:
        pass
    return links


def get_text(news_row):
    return news_row['text']


def get_news_id(news_row):
    return str(news_row['source_id']) + '_' + str(news_row['post_id'])
