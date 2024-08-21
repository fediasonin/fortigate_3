import requests
import json
import time
import aiohttp
import asyncio
import logging
import ipaddress



def get_bearer_token(root_url_api, username, password, client_secret):
    requests.packages.urllib3.disable_warnings()
    url=f'{root_url_api}:3334/connect/token'

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "username": username,
        "password": password,
        "client_id": "mpx",
        "client_secret": client_secret,
        "grant_type": "password",
        "response_type": "code id_token token",
        "scope": "offline_access mpx.api ptkb.api"
    }

    response = requests.request(method='POST', url=url, headers=headers, data=data, verify=False)
    token = json.loads(response.text)["access_token"]

    return token


import requests
import json


def get_events_by_filter(root_url_api, access_token, filter, time_from, time_to=None):
    # Отключаем предупреждения, связанные с SSL-сертификатами
    requests.packages.urllib3.disable_warnings()

    # Формируем URL для API
    url = f'{root_url_api}/api/events/v2/events?limit=10000'

    # Устанавливаем заголовки с токеном доступа для авторизации
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # Подготавливаем данные для POST-запроса
    data = {
        "filter": filter,
        "groupValues": [],
        "timeFrom": time_from
    }

    # Добавляем "timeTo" в данные, если оно предоставлено
    if time_to is not None:
        data["timeTo"] = time_to

    # Выполняем POST-запрос к API
    response = requests.post(url, headers=headers, data=json.dumps(data), verify=False)

    # Проверяем, успешен ли запрос
    if response.status_code != 200:
        raise Exception(f"Ошибка при запросе к API: {response.status_code}, {response.text}")

    # Парсим JSON-ответ
    response_json = response.json()

    # Проверяем наличие ключа "events" в ответе
    if "events" not in response_json:
        raise KeyError('Ключ "events" отсутствует в ответе от API. Ответ: ' + response.text)

    # Извлекаем данные
    events = response_json["events"]
    total_count = response_json.get("totalCount", 0)

    # Определяем время последнего события, если события есть
    last_incident_time = events[-1]["time"] if events else None

    return events, total_count, last_incident_time





async def get_country_by_ip(session, ip_address):
    url = 'https://geoip.noc.gov.ru/api/geoip'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    try:
        async with session.get(f'{url}?ip={ip_address}', headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('country_name')
            else:
                logging.error(f"API error {response.status}: {await response.text()}")
                return None
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка запроса: {e}")
        return None
    except Exception as e:
        logging.error(f"Неизвестная ошибка: {e}")
        return None



