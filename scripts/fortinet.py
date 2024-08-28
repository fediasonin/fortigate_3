import time
import datetime
import os
import json
import pandas as pd
import logging
import asyncio
import aiohttp

import api_client
from pdql_filteres import event_filters
from event_analyzer import dataparse, visualize


def run_api(d1, d2):
    new_column_names = {
        "time": "Время",
        "event_src.host": "fortigate",
        "src.ip": "Атакующий",
        "object.type": "Сигнатура",
        "text": "Описание"
    }

    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    current_time = time.localtime()
    formatted_time = time.strftime('_%d-%m-%Y_%H-%M-%S', current_time)

    output_file_pdf = os.path.join(output_dir, f'stats{formatted_time}.pdf')
    output_file_xlsx = os.path.join(output_dir, f'events{formatted_time}.xlsx')

    writer = pd.ExcelWriter(output_file_xlsx, engine='xlsxwriter')

    ip_whitelist = dataparse.parse_ip_file("config/filtered_addresses.txt")


    #json_list = dataparse.csv_to_json_list("input.csv", ip_whitelist)

    json_list = asyncio.run(get_json_list_from_API(ip_whitelist, d1, d2))

    df = dataparse.json_to_dataframe(json_list)
    dataparse.dataframe_to_excel(df, writer, 'Все события', new_column_names)
    dataparse.group_by_src_ip_to_excel(df, writer, 'Группировка по атакующим', new_column_names)
    dataparse.group_by_unique_src_ip(df, writer, 'Уникальные атакующие адреса', new_column_names)
    dataparse.group_by_unique_dst_combinations(df, writer, 'Уникальные ресурсы', new_column_names)
    dataparse.create_summary_statistics(df, writer, 'Статистика')

    writer._save()
    print(f"Saving the output to {output_file_xlsx}")

    visualize.visualize_data_to_pdf(json_list, output_file_pdf)
    print(f"Saving the output to {output_file_pdf}")

    print("\nDone!")


async def run_csv():
    new_column_names = {
        "time": "Время",
        "event_src.host": "fortigate",
        "src.ip": "Атакующий",
        "object.type": "Сигнатура",
        "text": "Описание"
    }

    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    current_time = time.localtime()
    formatted_time = time.strftime('_%d-%m-%Y_%H-%M-%S', current_time)

    output_file_pdf = os.path.join(output_dir, f'stats{formatted_time}.pdf')
    output_file_xlsx = os.path.join(output_dir, f'events{formatted_time}.xlsx')

    writer = pd.ExcelWriter(output_file_xlsx, engine='xlsxwriter')

    ip_whitelist = dataparse.parse_ip_file("config/filtered_addresses.txt")

    json_list = dataparse.csv_to_json_list("input.csv", ip_whitelist)

    #json_list = asyncio.run(get_json_list_from_API(ip_whitelist, d1, d2))

    df = dataparse.json_to_dataframe(json_list)
    ip_cache = {}

    async with aiohttp.ClientSession() as session:
        count = 0
        tasks = []


        # Проходимся по каждой строке DataFrame
        for idx, row in df.iterrows():
            src_ip = row.get('src.ip')
            dst_ip = row.get('dst.ip')

            # Обработка src_ip
            if src_ip and not dataparse.ip_in_list(src_ip, ip_whitelist):
                if src_ip not in ip_cache:
                    task = asyncio.ensure_future(api_client.get_country_by_ip(session, src_ip))
                    tasks.append((idx, 'src', task))  # Сохраняем индекс, тип IP и задачу

            # Обработка dst_ip
            if dst_ip and not dataparse.ip_in_list(dst_ip, ip_whitelist):
                if dst_ip not in ip_cache:
                    task = asyncio.ensure_future(api_client.get_country_by_ip(session, dst_ip))
                    tasks.append((idx, 'dst', task))  # Сохраняем индекс, тип IP и задачу

        # Выполняем асинхронные запросы
        results = await asyncio.gather(*[task for _, _, task in tasks])

        # Обновляем DataFrame новыми данными
        for (idx, ip_type, _), result in zip(tasks, results):
            if ip_type == 'src':
                ip_cache[df.at[idx, 'src.ip']] = result
                df.at[idx, 'src.geo.country'] = result
            elif ip_type == 'dst':
                ip_cache[df.at[idx, 'dst.ip']] = result
                df.at[idx, 'dst.geo.country'] = result
            count += 1

        print(f"Processed {count} IPs")

    dataparse.dataframe_to_excel(df, writer, 'Все события', new_column_names)
    dataparse.group_by_src_ip_to_excel(df, writer, 'Группировка по атакующим', new_column_names)
    dataparse.group_by_unique_src_ip(df, writer, 'Уникальные атакующие адреса', new_column_names)
    dataparse.group_by_unique_dst_combinations(df, writer, 'Уникальные ресурсы', new_column_names)
    dataparse.create_summary_statistics(df, writer, 'Статистика')

    writer._save()
    print(f"Saving the output to {output_file_xlsx}")

    visualize.visualize_data_to_pdf(json_list, output_file_pdf)
    print(f"Saving the output to {output_file_pdf}")

    print("\nDone!")







async def get_json_list_from_API(ip_list, date1, date2):
    # Загружаем учетные данные из файла
    with open('config/credentials.json', 'r') as file:
        creds = json.load(file)

    ROOT_URL_API = creds["url_root_api"]
    USERNAME = creds["username"]
    PASSWORD = creds["password"]
    CLIENT_SECRET = creds["secret"]

    # Получаем токен доступа
    bearer_token = api_client.get_bearer_token(
        root_url_api=ROOT_URL_API,
        username=USERNAME,
        password=PASSWORD,
        client_secret=CLIENT_SECRET
    )

    # Преобразуем даты в таймстемпы
    time_from = int(time.mktime(time.strptime(date1, '%Y-%m-%d %H:%M:%S')))
    time_to = int(time.mktime(time.strptime(date2, '%Y-%m-%d %H:%M:%S')))

    events_buffer = []
    logging.basicConfig(level=logging.DEBUG)
    previous_last_incident_time = None

    # Цикл получения событий с фильтрацией по времени
    while True:
        events, total_count, last_incident_time = api_client.get_events_by_filter(
            root_url_api=ROOT_URL_API,
            access_token=bearer_token,
            filter=event_filters.fortinet_attacks,
            time_from=time_from,
            time_to=time_to
        )

        logging.debug(f"Total Count: {total_count}, Last Incident Time: {last_incident_time}, Time To: {time_to}")
        events_buffer.extend(events)

        # Прерываем цикл, если количество событий меньше 10000 или нет новых событий
        if total_count <= 10000 or last_incident_time == previous_last_incident_time:
            break

        previous_last_incident_time = last_incident_time
        time_to = int(datetime.datetime.strptime(last_incident_time, "%Y-%m-%dT%H:%M:%S.%f0Z").timestamp()) + 1

    # Удаляем дубликаты событий
    unique_events = {event['uuid']: event for event in events_buffer}
    total_events = list(unique_events.values())

    count = 0
    ip_cache = {}
    filtered_events = []

    # Создаем сессию aiohttp для асинхронных запросов
    async with aiohttp.ClientSession() as session:
        tasks = []

        # Фильтрация событий по IP и добавление информации о стране
        for event in total_events:
            src_ip = event.get('src.ip')
            dst_ip = event.get('dst.ip')
            skip_event = False

            if src_ip and not dataparse.ip_in_list(src_ip, ip_list):
                if src_ip not in ip_cache:
                    task = asyncio.ensure_future(api_client.get_country_by_ip(session, src_ip))
                    ip_cache[src_ip] = await task
                    count += 1
                event['src.geo.country'] = ip_cache[src_ip]
            else:
                skip_event = True

            if dst_ip and not dataparse.ip_in_list(dst_ip, ip_list):
                if dst_ip not in ip_cache:
                    task = asyncio.ensure_future(api_client.get_country_by_ip(session, dst_ip))
                    ip_cache[dst_ip] = await task
                    count += 1
                event['dst.geo.country'] = ip_cache[dst_ip]
            else:
                skip_event = True

            if not skip_event:
                filtered_events.append(event)

    # Возвращаем отфильтрованные события
    print(len(filtered_events))
    print(f"{count} IPs")
    return filtered_events


