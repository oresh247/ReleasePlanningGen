# coding:utf-8
import configparser
import html
import json
import re
import warnings
import sys

import numpy as np
import pandas as pd
import requests

import openpyxl

import gspread
from gspread.exceptions import APIError

from oauth2client.service_account import ServiceAccountCredentials

import time
import logging

from datetime import datetime  # Добавляем импорт

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.width', 1000)

logging.basicConfig(
    filename='google_sync.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Глобальная переменная для хранения подключения
_client = None

warnings.filterwarnings("ignore")

config = configparser.ConfigParser()
configJira = configparser.ConfigParser()
config.read("config.ini", encoding='utf-8')
configJira.read("configFields.ini", encoding='utf-8')

# СФЕРА параметры
devUser = config["SFERAUSER"]["devUser"]
devPassword = config["SFERAUSER"]["devPassword"]
sferaUrlSearch = config["SFERA"]["sferaUrlSearch"]
sferaUrlLogin = config["SFERA"]["sferaUrlLogin"]

spreadsheetUrl = config["GOOGLE"]["spreadsheetUrl"]
SHEET_NAME = config["GOOGLE"]["SHEET_NAME"]

COLUMS = json.loads(config["DF"]["COLUMS"])
STATUS_MAPPING = json.loads(config["DF"]["STATUS_MAPPING"])

session = None
session = requests.Session()
session.post(sferaUrlLogin, json={"username": devUser, "password": devPassword}, verify=False)

def get_release_tasks(release):
    """
    Функция возвращает список задач релиза в виде массива json
    :param release: str
         Номер релиза
    :return json.loads(response.text): str
        Текст ответа на запрос в формате json
    """
    # Формируем запрос
    query = 'label%20%3D%20%27' + release + '%27&size=1000&page=0&attributesToReturn=checkbox%2Cnumber%2Cname%2CactualSprint%2Cpriority%2Cstatus%2Cassignee%2Cowner%2CdueDate%2Clabel%2CparentNumber%2Ccomponent%2CgantStartDate%2CgantEndDate'
    url = sferaUrlSearch + "?query=" + query
    # Делаем запрос задач по фильтру
    response = session.get(url, verify=False)
    if response.ok != True:
        raise Exception("Error get sprint data " + response)
    return json.loads(response.text)


def create_empty_dataframe():
    # Создаем пустой DataFrame с заданными колонками
    empty_df = pd.DataFrame(columns=COLUMS)
    return empty_df


def fill_dataframe(release, data_dict, empty_df):
    # Извлекаем данные из JSON
    data = []
    for item in data_dict['content']:
        gantStartDate = None
        gantEndDate = None
        component_name = item['component'][0]['name'] if item['component'] else ''
        testing_date = item.get('dueDate', '')
        if 'gantStartDate' in item:
            gantStartDate = item['gantStartDate'][0].split('T')[0]
        if 'gantEndDate' in item:
            gantEndDate = item['gantEndDate'][0].split('T')[0]
        assignee = f"{item['assignee']['firstName']}" if 'assignee' in item else ''

        # Определяем статус: если исполнитель не найден - ставим "Бэклог"
        status = STATUS_MAPPING.get(assignee, "Бэклог")  # Изменено здесь

        row_data = {
            COLUMS[0]: release,
            COLUMS[1]: item['number'],
            COLUMS[2]: item['name'],
            COLUMS[3]: component_name,
            COLUMS[4]: status,
            COLUMS[5]: item['priorityId']-2,
            COLUMS[6]: '',  # В вашем JSON нет поля для estimation
            COLUMS[7]: gantStartDate,
            COLUMS[8]: gantEndDate,
            COLUMS[9]: testing_date,
            COLUMS[10]: assignee,
        }

        data.append(row_data)

    # Заполняем DataFrame
    filled_df = pd.DataFrame(data)

    return filled_df


def get_dataframe(release):
    tasks = get_release_tasks(release)

    # # ---------------- для мока
    # # Определите путь к файлу JSON
    # file_path = 'data/tasks.json'
    #
    # # Загрузите данные из файла JSON
    # with open(file_path, 'r', encoding='utf-8') as file:
    #     tasks = json.load(file)
    # # --------------------------

    if tasks == None:
        print("Выборка не вернула задачи!")
        sys.exit(1)

    df = create_empty_dataframe()
    df = fill_dataframe(release, tasks, df)
    # df.to_excel('output.xlsx',
    #             sheet_name='Sheet1',
    #             index=False,
    #             engine='openpyxl',
    #             na_rep='Н/Д',
    #             float_format='%.2f')
    return df


def connect_to_google_sheets(credentials_path):
    """
    Подключается к Google Sheets API с использованием сервисного аккаунта

    :param credentials_path: Путь к JSON-файлу с учетными данными сервисного аккаунта
    """
    global _client

    # Определяем необходимые разрешения
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        _client = gspread.authorize(credentials)
    except Exception as e:
        raise RuntimeError(f"Ошибка подключения: {str(e)}")


def get_sheet_as_dataframe(spreadsheet_url, sheet_name):
    """
    Получает данные из указанного листа Google-таблицы и возвращает их как DataFrame

    :param spreadsheet_url: Ссылка на Google-таблицу
    :param sheet_name: Название листа для обработки
    :return: pandas DataFrame с данными из таблицы
    """
    if _client is None:
        raise RuntimeError("Сначала выполните подключение через connect_to_google_sheets()")

    try:
        # Открываем таблицу по URL
        spreadsheet = _client.open_by_url(spreadsheet_url)

        # Выбираем нужный лист
        worksheet = spreadsheet.worksheet(sheet_name)

        # Получаем все данные
        records = worksheet.get_all_records()

        # Создаем DataFrame
        return pd.DataFrame(records)

    except gspread.SpreadsheetNotFound:
        raise ValueError("Таблица не найдена. Проверьте URL")
    except gspread.WorksheetNotFound:
        raise ValueError("Лист не найден. Проверьте название листа")
    except Exception as e:
        raise RuntimeError(f"Ошибка при получении данных: {str(e)}")


def filter_release_data(df: pd.DataFrame, release: str, excluded_statuses: list) -> pd.DataFrame:
    """
    Фильтрует задачи по релизу и исключает указанные статусы

    Параметры:
    df - исходный DataFrame
    release - номер релиза (например, "R12")
    excluded_statuses - список исключаемых статусов (например, ["Готово", "Отмена"])

    Возвращает:
    Отфильтрованный DataFrame
    """
    try:
        # Применяем фильтры
        filtered_df = df[
            (df['Релиз'] == release) &
            (~df['Статус'].isin(excluded_statuses))
            ].copy()

        # Сброс индексов для красоты
        return filtered_df.reset_index(drop=True)

    except KeyError as e:
        raise ValueError(f"Ошибка в структуре данных: отсутствует колонка {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Ошибка фильтрации: {str(e)}")


def add_row_to_sheet(spreadsheet_url: str, sheet_name: str, row_data: dict) -> None:
    """
    Вставляет новую строку перед последней существующей строкой, чтобы сохранить форматирование

    :param spreadsheet_url: URL Google-таблицы
    :param sheet_name: Название листа
    :param row_data: Данные для вставки в формате {заголовок: значение}
    """
    global _client

    if _client is None:
        raise RuntimeError("Сначала выполните подключение через connect_to_google_sheets()")

    try:
        # Открываем таблицу и лист
        spreadsheet = _client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Получаем все данные
        all_values = worksheet.get_all_values()

        if len(all_values) < 1:
            raise ValueError("Лист пустой")

        # Определяем позицию для вставки
        insert_position = len(all_values)  # Нумерация строк начинается с 1

        # Получаем заголовки
        headers = all_values[0]

        # Формируем строку для вставки
        new_row = [row_data.get(header, "") for header in headers]

        # Вставляем строку
        worksheet.insert_row(
            new_row,
            index=insert_position,
            value_input_option='USER_ENTERED'
        )

        print(f"Строка вставлена перед последней строкой на позиции {insert_position}")

    except gspread.exceptions.APIError as e:
        raise RuntimeError(f"Ошибка Google API: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Ошибка: {str(e)}")


def get_unique_tasks(df_sfera, df_google) -> pd.DataFrame:
    """
    Возвращает записи из df_sfera, которых нет в df_google по полю 'Задача'

    Параметры:
    df_sfera - датафрейм с данными из Sfera
    df_google - датафрейм с данными из Google Sheets

    Возвращает:
    Фильтрованный датафрейм с уникальными задачами
    """
    try:
        # Получаем список задач из Google Sheets
        google_tasks = df_google['Задача'].tolist()

        # Фильтруем задачи из Sfera
        mask = ~df_sfera['Задача'].isin(google_tasks)
        result_df = df_sfera[mask].copy()

        # Сбрасываем индексы
        return result_df.reset_index(drop=True)

    except Exception as e:
        raise RuntimeError(f"Ошибка при сравнении данных: {str(e)}")


def add_filtered_records(spreadsheet_url: str,sheet_name: str, result_df: pd.DataFrame) -> None:
    """
    Добавляет все записи из result_df в Google Таблицу
    с сохранением форматирования и правил валидации

    :param spreadsheet_url: URL Google-таблицы
    :param sheet_name: Название листа
    :param result_df: Датафрейм с записями для добавления
    """
    try:
        if result_df.empty:
            print("Нет записей для добавления")
            return

        # Преобразуем датафрейм в список словарей
        records = result_df.replace({np.nan: None}).to_dict('records')

        # Добавляем каждую запись
        for idx, row_data in enumerate(records, 1):
            if idx % 10 == 0:
                time.sleep(5)  # Пауза каждые 10 записей
            try:
                # Форматируем даты
                formatted_data = {
                    key: value.strftime('%m/%d/%Y') if isinstance(value, datetime) else value
                    for key, value in row_data.items()
                }

                add_row_to_sheet(
                    spreadsheet_url=spreadsheet_url,
                    sheet_name=sheet_name,
                    row_data=formatted_data
                )
                print(f"Добавлена запись {idx}/{len(records)}")

            except Exception as e:
                print(f"Ошибка при добавлении записи {idx}: {str(e)}")
                continue

        print(f"Успешно добавлено {len(records)} записей")

    except Exception as e:
        logging.error(f"Ошибка при добавлении записи {row_data}: {str(e)}")
        raise RuntimeError(f"Ошибка при обработке данных: {str(e)}")


def get_changed_status_records(df_filtered, df_sfera) -> pd.DataFrame:
    """
    Возвращает записи из df_filtered, которые:
    1) Имеют совпадающие номера задач с df_sfera
    2) Имеют разные статусы
    """
    try:
        # Объединяем DataFrame по номеру задачи
        merged = pd.merge(
            df_filtered[['Задача', 'Статус']],
            df_sfera[['Задача', 'Статус']],
            on='Задача',
            suffixes=('_filtered', '_sfera'),
            how='inner'
        )

        # Фильтруем записи с разными статусами
        changed_status = merged[merged['Статус_filtered'] != merged['Статус_sfera']]

        # Возвращаем полные записи из df_filtered
        result_df = df_sfera[df_sfera['Задача'].isin(changed_status['Задача'])]

        return result_df.reset_index(drop=True)

    except KeyError as e:
        print(f"Ошибка: {str(e)}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Неизвестная ошибка: {str(e)}")
        return pd.DataFrame()


def update_google_sheet(df, spreadsheet_url ,sheet_name) -> None:
    """
    Обновляет статусы и исполнителей в Google Таблице на основе данных из result
    """
    global _client

    if _client is None:
        raise RuntimeError("Сначала выполните подключение через connect_to_google_sheets()")

    try:
        spreadsheet = _client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Получаем все задачи из колонки B (индекс 2)
        records = worksheet.get_all_records()
        tasks = [row['Задача'] for row in records]

        # Подготавливаем пакетное обновление
        updates = []

        for idx, row in df.iterrows():
            task = row['Задача']
            new_status = row['Статус']
            new_assignee = row['Исполнитель']

            try:
                # Находим индекс строки в Google Sheets (начинается с 1)
                sheet_row = tasks.index(task) + 2  # +2: заголовок + 0-based индекс

                # Формируем обновления для статуса (колонка E) и исполнителя (колонка K)
                updates.append({
                    'range': f'E{sheet_row}',
                    'values': [[new_status]]
                })
                updates.append({
                    'range': f'K{sheet_row}',
                    'values': [[new_assignee]]
                })

            except ValueError:
                print(f"Задача {task} не найдена в таблице")
                continue

        # Выполняем пакетное обновление
        if updates:
            worksheet.batch_update(updates)
            print(f"Обновлено {len(updates) // 2} записей")

    except APIError as e:
        raise RuntimeError(f"Ошибка Google API: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Ошибка: {str(e)}")


def update_google_table(release, add_flag, update_status_flag):
    df_sfera = get_dataframe(release)

    # Подключение к API
    connect_to_google_sheets('vibrant-arcana-432518-n5-bc68afc81222.json')

    # Получение данных
    df_google = get_sheet_as_dataframe(spreadsheet_url=spreadsheetUrl, sheet_name=SHEET_NAME)

    # Найти записи, которых нет в таблице google
    result_df = get_unique_tasks(df_sfera, df_google)
    print(result_df)

    if add_flag:
        # Добавление записей
        add_filtered_records(spreadsheet_url=spreadsheetUrl, sheet_name=SHEET_NAME, result_df=result_df)

    # Отбираем задачи только по релизу
    df_filtered = filter_release_data(df=df_google, release=release,
                                      excluded_statuses=['Бэклог', 'Отмена', 'Блок', 'Готово', 'Поставка'])

    # Обновляем статус по исполнителю (только для статусов Аналитика, Разработк, Тестирование)
    df_changed_status = get_changed_status_records(df_filtered, df_sfera)
    print(df_changed_status)

    if update_status_flag:
        update_google_sheet(df=df_changed_status, spreadsheet_url=spreadsheetUrl, sheet_name=SHEET_NAME)


release = 'OKR_20250406_ATM' # Метка релиза
update_google_table(release, True, True)





