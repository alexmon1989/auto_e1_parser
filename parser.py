import urllib.request
import urllib.error
from lxml.html import fromstring
import json
from multiprocessing.dummy import Pool as ThreadPool
import random
import string
import time
import http.client
import sys
import configparser
import os.path
from datetime import datetime
import log_model
import automobile_model
import price_model
import cheapened_auto_model


def get_auto_data(id_auto):
    """Парсит страницу id_auto и возвращает данные авто в виде словаря."""
    url = site_url_car.format(id_auto)
    data = {}
    try:
        # Подготовка заголовков запроса
        req = urllib.request.Request(url)
        # Cookie для корректного парсинга по 50 записей со странице, иначе - больше "повторок"
        cookie_val = 'ngs_ttq=u:38a2bf169e503bd7667a7e8f30088b24; ngs_uid=w127Cle8DwUFKE5nAyQ/Ag==; ' \
                     'isMobile=false; feature_new_card=disabled'
        req.add_header('Cookie', cookie_val)
        result_html = urllib.request.urlopen(req).read()
    except urllib.error.HTTPError as e:
        print('{}: Ошибка {} при парсинге данных авто с id={}'.format(
            datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
            e.getcode(),
            id_auto
        ))
        log_model.create(id_auto, 'props', e.getcode())
    except:
        print('Неожиданная ошибка', sys.exc_info()[0])
    else:
        parsed_html = fromstring(result_html)
        # Производитель, марка
        auto_model_in_breadcrumbs = parsed_html.find_class('au-breadcrumbs au-breadcrumbs_inner') \
            .pop() \
            .find_class('au-breadcrumbs__link')
        data['model'] = auto_model_in_breadcrumbs.pop().text.strip()
        data['manufacturer'] = auto_model_in_breadcrumbs.pop().text.strip()

        # Свойства (характеристики)
        characteristics = parsed_html.find_class('au-offer-card__tech-item')
        for x in characteristics:
            title = x.find_class('au-offer-card__tech-title').pop().find_class('au-offer-card__tech-txt').pop().text
            value = x.find_class('au-offer-card__tech-value').pop().find_class('au-offer-card__tech-txt').pop()
            if value.text is None:
                value = value.find('strong')
            data[title] = value.text

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
        print('{}: Ошибка {} при парсинге  номера телефона авто с id={}'.format(
            datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
            e.getcode(),
            id_auto
        ))
        log_model.create(id_auto, 'phone', e.getcode())
    except http.client.RemoteDisconnected:
        print('{}: Соединение с сервером утеряно'.format(datetime.now().strftime('%d.%m.%Y %H:%M:%S')))
        log_model.create(id_auto, 'phone', 'RemoteDisconnected')
    except:
        print('{}: Неожиданная ошибка'.format(datetime.now().strftime('%d.%m.%Y %H:%M:%S')), sys.exc_info()[0])
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
    result_html = urllib.request.urlopen(site_url_ids).read()
    pages_count = int(fromstring(result_html)
                      .find_class('au-pagination__list')
                      .pop().findall('li')
                      .pop().find('a')
                      .text)
    return pages_count


def get_autos_data_from_table_page(page_num):
    """Получение данных авто из таблицы на определённой странице"""
    res = []
    try:
        # Подготовка заголовков запроса
        req = urllib.request.Request(site_url_ids + str(page_num))
        # Cookie для корректного парсинга по 50 записей со странице, без такого cookie больше "повторок"
        cookie_val = 'ngs_ttq=u:04c612d9d5172a4244c1eb2d0f8b2593; ngs_uid=w127CVe7WMmkzkL6BrobAg==; ' \
                     'search_offers_persistent=a%3A3%3A%7Bs%3A8%3A%22currency%22%3Bs%3A3%3A%22rur%22%3Bs%3A5%3A%22' \
                     'limit%22%3Bi%3A50%3Bs%3A4%3A%22sort%22%3Ba%3A16%3A%7Bs%3A2%3A%22id%22%3BN%3Bs%3A4%3A%22' \
                     'date%22%3BN%3Bs%3A14%3A%22date_and_price%22%3BN%3Bs%3A10%3A%22mark_model%22%3BN%3Bs%3A4%3A%22' \
                     'year%22%3BN%3Bs%3A10%3A%22horsepower%22%3BN%3Bs%3A12%3A%22transmission%22%3BN%3Bs%3A6%3A%22' \
                     'engine%22%3BN%3Bs%3A8%3A%22capacity%22%3BN%3Bs%3A11%3A%22engine_type%22%3BN%3Bs%3A5%3A%22' \
                     'wheel%22%3BN%3Bs%3A4%3A%22city%22%3BN%3Bs%3A8%3A%22run_size%22%3BN%3Bs%3A4%3A%22gear%22%3BN' \
                     '%3Bs%3A5%3A%22price%22%3BN%3Bs%3A11%3A%22views_total%22%3BN%3B%7D%7D;'
        req.add_header('Cookie', cookie_val)

        result_html = urllib.request.urlopen(req).read()
        parsed_html = fromstring(result_html)
        tr = parsed_html.find_class('au-elements__item')
        for x in tr:
            auto_id = int(x.find_class('au-elements__title__link_table')
                          .pop()
                          .attrib
                          .get('href')
                          .split('/')
                          .pop())
            price = int(x.find_class('au-elements__section-price__cost')
                        .pop()
                        .text
                        .strip()
                        .replace(u'\xa0', ''))
            try:
                mileage = x.find_class('au-elements__section-mileage').pop().find('span').text.replace(u'\xa0', '')
            except AttributeError:
                mileage = 'нет данных'
            res.append({'auto_id': auto_id, 'price': price, 'mileage': mileage})
    except urllib.error.HTTPError as e:
        print('{}: Ошибка {} при парсинге id авто со страницы {}'.format(
            datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
            e.getcode(),
            page_num
        ))
    except:
        print('{}: Неожиданная ошибка при получении данных со страницы № {} с таблицей автомобилей'.format(
            datetime.now().strftime('%d.%m.%Y %H:%M:%S'), page_num), sys.exc_info()[0]
        )
    return res


def get_autos_data_from_table(threads_count=10, all_pages=True, pages_count=1):
    """Получение идентификаторов автомобилей, которые сейчас есть на сайте"""
    res = []
    if all_pages:
        pages_count = get_pages_count()

    # Получение данных автомобилей (id, цена) в несколько потоков
    pool = ThreadPool(threads_count)
    pool_results = pool.map(get_autos_data_from_table_page, range(1, pages_count + 1))
    pool.close()
    pool.join()

    for x in pool_results:
        res.extend(x)

    return res


def parse_auto_to_db(auto_table_data):
    """Получение данных по автомобилю и добавление их в БД"""
    automobile = automobile_model.get_by_e1_id(auto_table_data['auto_id'])
    if automobile:
        # Поиск последней цены и сравнение её с текущей (если не совпадает, то добавление её в БД)
        last_price = price_model.get_last_auto_price(automobile['_id'])
        if last_price is None or last_price != auto_table_data['price']:
            price_model.create(auto_table_data['price'], auto_table_data['mileage'], automobile['_id'])
        if last_price is not None and last_price > auto_table_data['price']:
            cheapened_auto_model.create(automobile, last_price - auto_table_data['price'])
    else:
        # Иначе - создать запись
        auto_data = get_auto_data(auto_table_data['auto_id'])
        if auto_data:
            phone_data = get_phone_number(auto_table_data['auto_id'])
            # Запись в коллекцию automobiles данных о характеристиках авто
            if auto_data.get('Цена'):
                del auto_data['Цена']
            automobile = automobile_model.create(auto_table_data['auto_id'], phone_data, auto_data)
            # Запись цены в коллекцию prices
            if auto_table_data.get('price'):
                price_model.create(auto_table_data['price'], auto_table_data['mileage'], automobile)


if __name__ == '__main__':
    # Загрузка параметров
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
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
    autos_data_from_table = get_autos_data_from_table(
        config['THREADS'].getint('ThreadsCountGetIds'),
        all_pages=True
    )
    print('{}: Время на получение списка id и цен, элементов: {}, сек.: {}'.format(
        datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
        len(autos_data_from_table),
        time.time() - t1
    ))

    t1 = time.time()
    pool = ThreadPool(config['THREADS'].getint('ThreadsCountGetResults'))
    results = pool.map(parse_auto_to_db, autos_data_from_table)
    pool.close()
    pool.join()
    print('{}: Время на получение данных и добавление их в БД, сек.: {}'.format(
        datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
        time.time() - t1
    ))

    # print(get_auto_data(8064468))
    # print(get_phone_number(8059410))