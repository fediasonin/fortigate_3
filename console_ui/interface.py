import asyncio
import time

from console_ui import raws
from scripts import fortinet
from date_util import date_parser
from scripts import vpn_mosreg

def print_main():
    print(raws.GREETING)
    print(raws.SCENARIOS)
    print(raws.GREETING_LINE, end='')


def select_script(num):
    if num == '1':
        handle_api_or_csv()
    elif num == '2':
        handle_vpn_task()
    else:
        print(raws.WRONG_ARGS)


def start():
    print_main()

    script_num = input()

    select_script(script_num)



def get_time_interval():
    start = date_parser.get_datetime()
    stop = date_parser.get_datetime()
    if start == stop:
        print(raws.MATCH_DATES)
        return None, None
    return start, stop

def execute_with_timing(task_func, *args):
    start_time = time.time()
    task_func(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Программа выполнялась {execution_time:.2f} секунд.")



def run_vpn():
    start, stop = get_time_interval()
    if start and stop:
        execute_with_timing(vpn_mosreg.run_vpn, start, stop)

def handle_api_or_csv():
    print(raws.SCENARIOS_DOWLD)
    print(raws.GREETING_LINE, end='')
    select = input()
    if select == '1':
        start, stop = get_time_interval()
        fortinet.run_api(start, stop)
    elif select == '2':
        asyncio.run(fortinet.run_csv())
    else:
        print(raws.WRONG_ARGS)

def handle_vpn_task():
    print(raws.SCENARIOS_VPN)
    print(raws.GREETING_LINE, end='')
    select = input()
    if select == '1':
        print(raws.SCENARIOS_DOWLD)
        print(raws.GREETING_LINE, end='')
        select = input()
        if select == '1':
            start, stop = get_time_interval()
            vpn_mosreg.run_vpn(start, stop)
        if select == '2':
            vpn_mosreg.run_vpn_csv()

    elif select == '2':
        input_f = input('введите файл для обработки: ')
        output_f = input('введите файл для выгрузки: ')
        vpn_mosreg.run_vpn_to_excel_week(input_f, output_f)
    elif select == '3':
        output_f = input('введите файл для вывода: ')
        year = int(input('введите год: '))
        month = int(input('введите месяц: '))
        vpn_mosreg.run_vpn_month(output_f, year, month)

    else:
        print(raws.WRONG_ARGS)







