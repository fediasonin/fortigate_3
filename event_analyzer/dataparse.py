import json
import csv
import ipaddress
import argparse
import pandas as pd

from pandas import to_datetime
from datetime import datetime, timedelta


def parse_ip_file(filename):
    with open(filename, 'r') as file:
        lines = file.readlines()

    parsed_ips = []

    for line in lines:
        line = line.strip()
        if '/' in line:
            address, mask = line.split('/')
            network = f"{address}/{mask}"
            parsed_ips.append(ipaddress.ip_network(network, strict=False))
        elif ':' in line:
            start_address, end_address = line.split(':')
            start_ip = ipaddress.ip_address(start_address)
            end_ip = ipaddress.ip_address(end_address)
            parsed_ips.append((start_ip, end_ip))
        else:
            parsed_ips.append(ipaddress.ip_address(line))

    return parsed_ips


def ip_in_list(src_ip, ip_list):
    src_ip = ipaddress.ip_address(src_ip)

    for ip_item in ip_list:
        if isinstance(ip_item, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
            if src_ip in ip_item:
                return True
        elif isinstance(ip_item, tuple):
            start_ip, end_ip = ip_item
            if start_ip <= src_ip <= end_ip:
                return True
        else:
            if src_ip == ip_item:
                return True

    return False


def parse_json_to_dict(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле {file_path}.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")


def csv_to_json_list(filepath, ip_list):
    json_list = []
    with open(filepath, mode='r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file, delimiter=';')

        for row in csv_reader:
            if not ip_in_list(row['src.ip'], ip_list):
                processed_row = {key: value for key, value in row.items()}
                json_list.append(processed_row)

    return json_list


def json_to_dataframe(json_list):
    return pd.DataFrame(json_list)


def dataframe_to_excel(df, writer, sheet_name, columns_renamed):
    df_renamed = df.rename(columns=columns_renamed)
    df_renamed['Время'] = pd.to_datetime(df_renamed['Время'], utc=True).dt.tz_convert('Europe/Moscow').dt.tz_localize(None)
    df_sorted = df_renamed.sort_values(by='Время', ascending=False)
    df_sorted.to_excel(writer, sheet_name=sheet_name, index=False)


def group_by_src_ip_to_excel(df, writer, sheet_name, columns_renamed):
    df = df.rename(columns=columns_renamed)
    df['Время'] = pd.to_datetime(df['Время']).dt.tz_localize(None)
    df_sorted = df.sort_values(by='Время', ascending=False)

    grouped = df_sorted.groupby('Атакующий', sort=False)
    startrow = 0
    first_table = True

    for name, group in grouped:
        group.to_excel(writer, sheet_name=sheet_name, startrow=startrow, index=False, header=first_table)
        if first_table:
            startrow += len(group) + 1
            first_table = False
        else:
            startrow += len(group)
        startrow += 1


def group_by_unique_src_ip(df, writer, sheet_name, columns_renamed):
    df = df.rename(columns=columns_renamed)
    df['Время'] = pd.to_datetime(df['Время']).dt.tz_localize(None)
    df_sorted = df.sort_values(by='Время', ascending=True)

    df_unique_src_ip = df_sorted.drop_duplicates(subset=['Атакующий'], keep='first')
    df_unique_src_ip.to_excel(writer, sheet_name=sheet_name, index=False)


def group_by_unique_dst_combinations(df, writer, sheet_name, columns_renamed):
    df = df.rename(columns=columns_renamed)
    df['Время'] = pd.to_datetime(df['Время']).dt.tz_localize(None)
    df_sorted = df.sort_values(by='Время', ascending=True)

    df_unique_dst_combinations = df_sorted.drop_duplicates(subset=['dst.host', 'dst.ip', 'dst.port'], keep='first')
    df_unique_dst_combinations.to_excel(writer, sheet_name=sheet_name, index=False)


def create_summary_statistics(df, writer, sheet_name):
    # Подсчет количества уникальных src.ip и их сортировка
    src_ip_counts = df['src.ip'].value_counts().reset_index()
    src_ip_counts.columns = ['src.ip', 'count']

    # Добавление src.geo.country в таблицу
    src_geo_country = df[['src.ip', 'src.geo.country']].drop_duplicates()
    src_ip_counts = src_ip_counts.merge(src_geo_country, on='src.ip', how='right')

    src_ip_counts_sorted = src_ip_counts.sort_values(by='count', ascending=False)

    # Группировка по dst.host, dst.ip, dst.geo.country, dst.port и подсчет их количества
    dst_group = df.groupby(['dst.host', 'dst.ip', 'dst.geo.country', 'dst.port']).size().reset_index(name='count')
    dst_group_sorted = dst_group.sort_values(by='count', ascending=False)

    # Сохранение таблиц в Excel
    src_ip_counts_sorted.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)
    dst_group_sorted.to_excel(writer, sheet_name=sheet_name, index=False, startcol=4, startrow=0)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_file', type=str, help='The name of the input file')

    args = parser.parse_args()
    return args
