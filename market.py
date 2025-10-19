import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров Яндекс.Маркета для указанной страницы кампании.

    Args:
        page (str): Токен страницы для постраничной загрузки товаров.
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Объект с результатами запроса, содержащий товары и параметры пагинации.

    Пример корректного использования:
        >>> get_product_list("", "123456", "token_abcdef")
        {'offerMappingEntries': [...], 'paging': {...}}

    Пример некорректного использования:
        >>> get_product_list("", "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Отправляет обновлённые остатки товаров на Яндекс.Маркет.

    Args:
        stocks (list[dict]): Список словарей с остатками товаров (sku, warehouseId, items).
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Ответ API с информацией о результате обновления остатков.

    Пример корректного использования:
        >>> stocks = [{"sku": "A123", "warehouseId": 1, "items": [{"count": 10, "type": "FIT", "updatedAt": "2025-10-19T12:00:00Z"}]}]
        >>> update_stocks(stocks, "123456", "token_abcdef")
        {'result': {'updated': 1}}

    Пример некорректного использования:
        >>> update_stocks([], "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Отправляет обновлённые цены товаров на Яндекс.Маркет.

    Args:
        prices (list[dict]): Список словарей с ценами товаров.
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        access_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        dict: Ответ API с информацией о результате обновления цен.

    Пример корректного использования:
        >>> prices = [{"id": "A123", "price": {"value": 5990, "currencyId": "RUR"}}]
        >>> update_price(prices, "123456", "token_abcdef")
        {'result': {'updated': 1}}

    Пример некорректного использования:
        >>> update_price([], "wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает список артикулов товаров (shopSku) для указанной кампании.

    Args:
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        market_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        list[str]: Список артикулов товаров, зарегистрированных в кампании.

    Пример корректного использования:
        >>> get_offer_ids("123456", "token_abcdef")
        ['A123', 'B456', 'C789']

    Пример некорректного использования:
        >>> get_offer_ids("wrong_id", "bad_token")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Формирует список остатков товаров для отправки на Яндекс.Маркет.

    Args:
        watch_remnants (list[dict]): Локальные данные об остатках товаров, содержащие поля "Код" и "Количество".
        offer_ids (list[str]): Список артикулов товаров, доступных на Яндекс.Маркете.
        warehouse_id (str): Идентификатор склада на Маркете.

    Returns:
        list[dict]: Список словарей с остатками товаров, готовый для отправки через API.

    Пример корректного использования:
        >>> watch_remnants = [{"Код": "A123", "Количество": ">10"}]
        >>> offer_ids = ["A123", "B456"]
        >>> create_stocks(watch_remnants, offer_ids, "1")
        [
            {'sku': 'A123', 'warehouseId': '1', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2025-10-19T12:00:00Z'}]},
            {'sku': 'B456', 'warehouseId': '1', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2025-10-19T12:00:00Z'}]}
        ]

    Пример некорректного использования:
        >>> create_stocks(None, ["A123"], "1")
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен товаров для отправки на Яндекс.Маркет.

    Args:
        watch_remnants (list[dict]): Локальные данные о товарах с полями "Код" и "Цена".
        offer_ids (list[str]): Список артикулов товаров, доступных на Яндекс.Маркете.

    Returns:
        list[dict]: Список словарей с ценами товаров, готовый для обновки через API.

    Пример корректного использования:
        >>> watch_remnants = [{"Код": "A123", "Цена": "5'990.00 руб."}]
        >>> offer_ids = ["A123"]
        >>> create_prices(watch_remnants, offer_ids)
        [{'id': 'A123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Отправляет цены товаров на Яндекс.Маркет пакетами.

    Args:
        watch_remnants (list[dict]): Локальные данные о товарах с полями "Код" и "Цена".
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        market_token (str): Токен доступа к API Яндекс.Маркета.

    Returns:
        list[dict]: Список словарей с ценами, которые были отправлены на Маркет.

    Пример корректного использования:
        upload_prices(watch_remnants, "123456", "token_abcdef"))
        [{'id': 'A123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

    Пример некорректного использования:
        upload_prices(None, "123456", "token_abcdef"))
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Отправляет остатки товаров на Яндекс.Маркет пакетами.

    Args:
        watch_remnants (list[dict]): Локальные данные об остатках товаров.
        campaign_id (str): Идентификатор кампании Яндекс.Маркета.
        market_token (str): Токен доступа к API Яндекс.Маркета.
        warehouse_id (str): Идентификатор склада на Маркете.

    Returns:
        tuple:
            list[dict]: Список товаров с ненулевыми остатками.
            list[dict]: Полный список остатков, отправленных на Маркет.

    Пример корректного использования:
        upload_stocks(watch_remnants, "123456", "token_abcdef", "1"))
        ([{'sku': 'A123', 'warehouseId': '1', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2025-10-19T12:00:00Z'}]}],
         [{'sku': 'A123', 'warehouseId': '1', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2025-10-19T12:00:00Z'}]}])

    Пример некорректного использования:
        upload_stocks(None, "123456", "token_abcdef", "1"))
        Traceback (most recent call last):
            ...
        TypeError: 'NoneType' object is not iterable
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
