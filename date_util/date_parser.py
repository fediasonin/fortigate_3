from datetime import datetime


#ВВОД ДАТЫ С ПРОВЕРКОЙ И ФОРМАТИРОВАНИЕМ В СТРОКУ
def get_datetime():

    while True:
        date_input = input("Введите дату и время в формате 'YYYY-MM-DD HH:MM:SS': ")

        try:
            # Парсим введённую строку в объект datetime
            parsed_date = datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')

            # Преобразуем объект datetime обратно в строку в том же формате
            formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')

            print(f"Вы ввели: {formatted_date}")
            break  # Завершаем цикл, если формат правильный
        except ValueError:
            print("Неверный формат даты. Попробуйте снова.")
    return formatted_date