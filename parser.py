import urllib.request
import urllib.error
from lxml.html import parse
from lxml.html import fromstring
import json
from multiprocessing.dummy import Pool as ThreadPool
import random
import string
import log_model
import automobile_model
import time
import http.client
import sys
import configparser


def get_auto_data(id_auto):
    """Парсит страницу id_auto и возвращает данные авто в виде словаря."""
    url = site_url_car.format(id_auto)
    data = {}
    try:
        r = urllib.request.urlopen(url)
        page = parse(r).getroot()
        characteristics = page.find_class('au-offer-card__tech-item')
        for x in characteristics:
            title = x.find_class('au-offer-card__tech-title').pop().find_class('au-offer-card__tech-txt').pop().text
            value = x.find_class('au-offer-card__tech-value').pop().find_class('au-offer-card__tech-txt').pop()
            if value.text is None:
                value = value.find('strong')
            data[title] = value.text.replace(u'\xa0', ' ')
    except urllib.error.HTTPError as e:
        print('Ошибка {} при парсинге данных авто с id={}'.format(e.getcode(), id_auto))
        log_model.create(id_auto, 'props', e.getcode())
    except:
        print('Неожиданная ошибка', sys.exc_info()[0])

    return data


def get_phone_number(id_auto):
    """Отправляет POST запрос, парсит HTML-ответ и возвращает данные продавца в виде словаря."""
    result = {
        'name': None,
        'phone': None,
    }

    # Json для отправки на сервер
    data = {
        "jsonrpc": "2.0",
        "method": "getOfferContacts",
        "params": [{"id": id_auto, "context": "card"}]
    }

    # Подготовка заголовков запроса
    req = urllib.request.Request(site_url_phone)
    req.add_header('Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8')
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0')
    # Значение ключа Cookie ngs_uid каждый раз меняется, чтобы не вызывать подозрений
    random_cookie_val = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))
    req.add_header('Cookie', 'ngs_uid={}==;'.format(random_cookie_val))
    req.add_header('X-Requested-With', 'XMLHttpRequest')
    json_data = json.dumps(data)
    json_data_as_bytes = json_data.encode('utf-8')
    req.add_header('Content-Length', len(json_data_as_bytes))

    # Отправка запроса на сервер
    try:
        http_response = urllib.request.urlopen(req, json_data_as_bytes)
    except urllib.error.HTTPError as e:
        print('Ошибка {} при парсинге  номера телефона авто с id={}'.format(e.getcode(), id_auto))
        log_model.create(id_auto, 'phone', e.getcode())
    except http.client.RemoteDisconnected:
        print('Соединение с сервером утеряно')
        log_model.create(id_auto, 'phone', 'RemoteDisconnected')
    except:
        print('Неожиданная ошибка', sys.exc_info()[0])
    else:
        # Разбор ответа сервера
        json_response = json.loads(http_response.read().decode("utf-8"))
        result_html = json_response.get('result')

        if len(result_html) > 0:
            parsed_html = fromstring(result_html)
            try:
                name_tag = parsed_html.find_class('au-offer-card__contacts-phone-notice')
                if name_tag:
                    result['name'] = name_tag.pop().attrib.get('title')
                else:
                    # Проверка названия автосалона
                    auto_store_tag = parsed_html.find_class('au-offer-card__contacts-txt')
                    if auto_store_tag:
                        for x in auto_store_tag:
                            if x.tag == 'strong':
                                result['name'] = x.text

                result['phone'] = parsed_html.find_class('au-offer-card__contacts-phone-txt').pop().text
            except IndexError:
                pass

    return result


def get_pages_count():
    """Получение количества страниц с автомобилями"""
    r = urllib.request.urlopen(site_url_ids)
    pages_count = int(parse(r).getroot().find_class('au-pagination__list').pop().findall('li').pop().find('a').text)
    return pages_count


def get_ids_from_page(page_num):
    """Получение идентификаторов авто с определённой страницы"""
    res = []
    try:
        r = urllib.request.urlopen(site_url_ids + str(page_num))
        page = parse(r).getroot()
        links = page.find_class('au-elements__title__link_table')
        for l in links:
            res.append(int(l.attrib.get('href').split('/').pop()))
    except urllib.error.HTTPError as e:
        print('Ошибка {} при парсинге id авто со страницы {}'.format(e.getcode(), page_num))
    except:
        print('Неожиданная ошибка', sys.exc_info()[0])
    return res


def get_auto_ids(threads_count=10):
    """Получение идентификаторов автомобилей, которые сейчас есть на сайте"""
    res = []
    pages_count = get_pages_count()

    # Получение id автомобилей в несколько потоков
    pool = ThreadPool(threads_count)
    pool_results = pool.map(get_ids_from_page, range(1, 1 + 1))
    pool.close()
    pool.join()

    for x in pool_results:
        res.extend(x)

    # Возвращаем уникальные id
    return list(set(res))


def parse_auto_to_db(auto_id):
    """Получение данных по автомобилю и добавление их в БД"""
    #pool = ThreadPool(2)
    #results = pool.map(lambda f: f(auto_id), [get_auto_data, get_phone_number])
    #pool.close()
    #pool.join()

    if not automobile_model.get_by_e1_id(auto_id):
        auto_data = get_auto_data(auto_id)
        phone_data = get_phone_number(auto_id)
        # Запись в БД
        automobile_model.create(auto_id, phone_data, auto_data)


if __name__ == '__main__':
    # Загрузка параметров
    config = configparser.ConfigParser()
    config.read('settings.ini')
    site_url = config['URLS']['SiteUrl']
    site_url_car = site_url + config['URLS']['SiteUriCar']
    site_url_phone = site_url + config['URLS']['SiteUriPhone']
    site_url_ids = site_url + config['URLS']['SiteUriIds']

    # Установка Proxy
    if config['PROXY'].getboolean('UseProxy'):
        proxy_support = urllib.request.ProxyHandler({
            'http': config['PROXY']['HTTP_PROXY'],
            'https': config['PROXY']['HTTPS_PROXY']
        })
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

    t1 = time.time()
    ids = get_auto_ids(config['THREADS'].getint('ThreadsCountGetIds'))
    print('Время на получение списка id, элементов: {}, сек.: {}'.format(len(ids), time.time() - t1))

    t1 = time.time()
    pool = ThreadPool(config['THREADS'].getint('ThreadsCountGetResults'))
    results = pool.map(parse_auto_to_db, ids)
    pool.close()
    pool.join()
    print('Время на получение данных и добавление их в БД, сек.: {}'.format(time.time() - t1))

    # print(get_auto_data(8059410))
    # print(get_phone_number(8059410))
