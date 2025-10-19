import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон через API

    Args:
        last_id (str): Идентификатор последнего товара в предыдущем запросе.
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        dict: Объект с результатами запроса, содержащий список товаров и параметры пагинации.

    Пример корректного использования:
        >>> get_product_list("", "12345", "abcd1234")
        {'items': [...], 'total': 1000, 'last_id': 'xyz'}

    Пример некорректного использования:
        >>> get_product_list("", "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Функция выполняет последовательные запросы к API, постранично загружая все товары магазина.
    Возвращает список артикулов, пригодных для дальнейшего обновления цен или остатков.

    Args:
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        list[str]: Список артикулов (offer_id) всех товаров магазина.

    Пример корректного использования:
        >>> get_offer_ids("12345", "abcdef123456")
        ['A123', 'B456', 'C789']

    Пример некорректного использования:
        >>> get_offer_ids("wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Функция отправляет список цен для обновления в личном кабинете продавца.

    Args:
        prices (list): Список словарей с информацией о ценах. Каждый элемент должен содержать
            поля offer_id, price, old_price и currency_code.
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        dict: Ответ API с результатом обработки — информация о статусе обновления цен.

    Пример корректного использования:
        >>> prices = [
        ...     {"offer_id": "A123", "price": "5990", "old_price": "0", "currency_code": "RUB"},
        ... ]
        >>> update_price(prices, "12345", "abcdef123456")
        {'result': {'updated_count': 1}}

    Пример некорректного использования:
        >>> update_price([], "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    Функция передаёт информацию о количестве доступных товаров на складе,
    чтобы синхронизировать наличие между внутренней системой и маркетплейсом.

    Args:
        stocks (list): Список словарей с данными об остатках.
            Каждый элемент должен содержать поля offer_id и stock.
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        dict: Ответ API, содержащий информацию о результате обновления остатков.

    Пример корректного использования:
        >>> stocks = [
        ...     {"offer_id": "A123", "stock": 15},
        ...     {"offer_id": "B456", "stock": 0},
        ... ]
        >>> update_stocks(stocks, "12345", "abcdef123456")
        {'result': {'updated_count': 2}}

    Пример некорректного использования:
        >>> update_stocks([], "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio

        Функция скачивает архив с сайта, извлекает из него Excel-файл с остатками,
    считывает данные в формате pandas DataFrame и преобразует их в список словарей.
    После обработки временный файл удаляется.

    Args:
        Нет аргументов.

    Returns:
        list[dict]: Список словарей, где каждый элемент содержит информацию о товаре
        (например, код, количество, цену и другие поля из Excel-файла).

    Пример корректного использования:
        >>> remnants = download_stock()
        >>> type(remnants)
        <class 'list'>
        >>> len(remnants) > 0
        True

    Пример некорректного использования:
        >>> # Ошибка при отсутствии соединения с сайтом
        >>> download_stock()
        Traceback (most recent call last):
            ...
        requests.exceptions.ConnectionError: HTTPSConnectionPool(host='timeworld.ru', port=443): Max retries exceeded
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков товаров для отправки в Ozon.

    Функция сопоставляет загруженные остатки с артикулами, доступными на площадке,
    корректирует количество в зависимости от значений из файла и добавляет недостающие
    товары с нулевым остатком.

    Args:
        watch_remnants (list[dict]): Список словарей с информацией о товарах
            из локальной системы, содержащий поля "Код" и "Количество".
        offer_ids (list[str]): Список артикулов товаров, загруженных на Ozon.

    Returns:
        list[dict]: Список словарей с полями "offer_id" и "stock", готовый для обновления
        остатков через API.

    Пример корректного использования:
        >>> watch_remnants = [{"Код": "A123", "Количество": ">10"}, {"Код": "B456", "Количество": "1"}]
        >>> offer_ids = ["A123", "B456", "C789"]
        >>> create_stocks(watch_remnants, offer_ids)
        [{'offer_id': 'A123', 'stock': 100}, {'offer_id': 'B456', 'stock': 0}, {'offer_id': 'C789', 'stock': 0}]

    Пример некорректного использования:
        >>> create_stocks(None, ["A123"])
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для товаров, подготовленный для отправки в Ozon.

    Функция сопоставляет локальные данные о товарах с артикулами, доступными на площадке,
    конвертирует цену в числовой формат и формирует словарь с необходимыми полями для API.

    Args:
        watch_remnants (list[dict]): Список словарей с информацией о товарах из локальной системы,
            содержащий поля "Код" и "Цена".
        offer_ids (list[str]): Список артикулов товаров, загруженных на Ozon.

    Returns:
        list[dict]: Список словарей с полями "offer_id", "price", "old_price",
        "currency_code" и "auto_action_enabled", готовый для обновления цен через API.

    Пример корректного использования:
        >>> watch_remnants = [{"Код": "A123", "Цена": "5'990.00 руб."}, {"Код": "B456", "Цена": "12 340.00 руб."}]
        >>> offer_ids = ["A123", "B456", "C789"]
        >>> create_prices(watch_remnants, offer_ids)
        [
            {'offer_id': 'A123', 'price': '5990', 'old_price': '0', 'currency_code': 'RUB', 'auto_action_enabled': 'UNKNOWN'},
            {'offer_id': 'B456', 'price': '12340', 'old_price': '0', 'currency_code': 'RUB', 'auto_action_enabled': 'UNKNOWN'}
        ]

    Пример некорректного использования:
        >>> create_prices(None, ["A123"])
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в числовой формат без символов и пробелов.

    Удаляет все нецифровые символы (пробелы, апострофы, валютные обозначения и т.д.)
    и возвращает строку, содержащую только цифры. Используется для подготовки цен
    перед отправкой в API или записью в базу данных.

    Args:
        price (str): Цена в текстовом формате, например "5'990.00 руб.".

    Пример корректного использования:
        price_conversion("5'990.00 руб.")
        '5990'

    Пример некорректного использования:
        price_conversion(None)
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части заданного размера.

    Функция используется для разбивки большого списка на более мелкие подсписки
    по n элементов, что удобно при отправке данных пакетами через API.

    Args:
        lst (list): Исходный список элементов для разделения.
        n (int): Размер каждой части (подсписка).

    Yields:
        list: Подсписки исходного списка, каждый размером до n элементов.

    Пример корректного использования:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

    Пример некорректного использования:
        >>> list(divide(None, 2))
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает и обновляет цены товаров на Ozon через API.

    Функция получает список всех артикулов магазина, формирует для них
    актуальные цены из локальных данных и отправляет их пакетами по 1000 элементов.

    Args:
        watch_remnants (list[dict]): Список словарей с информацией о товарах
            из локальной системы, содержащий поля "Код" и "Цена".
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        list[dict]: Список словарей с ценами, которые были отправлены на Ozon.

    Пример корректного использования:
        upload_prices(watch_remnants, "12345", "abcdef123456"))
        [{'offer_id': 'A123', 'price': '5990', 'old_price': '0'}]

    Пример некорректного использования:
        upload_prices(None, "12345", "abcdef123456"))
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает и обновляет остатки товаров на Ozon через API.

    Функция получает список всех артикулов магазина, формирует для них
    актуальные остатки из локальных данных и отправляет их пакетами по 100 элементов.
    Также возвращает отдельно список товаров с ненулевыми остатками.

    Args:
        watch_remnants (list[dict]): Список словарей с информацией о товарах
            из локальной системы, содержащий поля "Код" и "Количество".
        client_id (str): Уникальный идентификатор клиента (продавца) Ozon.
        seller_token (str): Токен авторизации продавца для доступа к API.

    Returns:
        tuple:
            list[dict]: Список словарей с остатками товаров, у которых stock != 0.
            list[dict]: Полный список словарей с остатками товаров, отправленных на Ozon.

    Пример корректного использования:

        upload_stocks(watch_remnants, "12345", "abcdef123456"))
        (
            [{'offer_id': 'A123', 'stock': 100}],
            [{'offer_id': 'A123', 'stock': 100}]
        )

    Пример некорректного использования:
        upload_stocks(None, "12345", "abcdef123456"))
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
