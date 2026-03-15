"""
config.py — конфигурация корпусов техникума.

Каждый корпус описывается словарём с полями:
  id         — уникальный идентификатор (используется в боте и API)
  name       — отображаемое название
  folder_id  — ID папки Google Drive
  structure  — тип структуры папки:
                 "flat"    — файлы сразу в папке (корпуса 2, 3, 4)
                 "nested"  — папки → подпапки → файлы (корпус 1)
  table_format — формат таблицы:
                 "type_a"  — корпус 3 и 4: строка 1=дата, группа=объединённая ячейка A:G
                 "type_b"  — корпус 1 и 4: "Лист замен на DD.MM.YYYY", группа=объединённая ячейка
                 "type_c"  — корпус 2: строки 1-3 заголовок, группа=заголовок строки (слитно)
  file_filter — ключевые слова в названии файла для отбора нужных файлов
                (None = брать все)
"""

from datetime import date

CORPS = [
    {
        "id":           "corp1",
        "name":         "1 корпус",
        "folder_id":    "1vzKOEmF84_dr8PUXSKc9z3IkWG6-dmQq",
        "structure":    "nested",   # Архив / 1 семестр / 2 семестр → папки по дням → файлы
        "table_format": "type_b",   # "Лист замен на DD.MM.YYYY"
        "file_filter":  ["замен", "расписани"],  # ищем файл замен/расписания, игнорируем занятость
    },
    {
        "id":           "corp2",
        "name":         "2 корпус",
        "folder_id":    "1EHepY6k2IAYz-SexM6b0cGMM_cGgO9YS",
        "structure":    "flat",
        "table_format": "type_c",   # строки 1-3 заголовок, группы слитно
        "file_filter":  None,
    },
    {
        "id":           "corp3",
        "name":         "3 корпус",
        "folder_id":    "1fxehYVWNrEC5EoHnrzgaxoSyCDXCDTur",
        "structure":    "flat",
        "table_format": "type_a",   # "Расписание на DD.MM.YYYY"
        "file_filter":  None,
    },
    {
        "id":           "corp4",
        "name":         "4 корпус",
        "folder_id":    "1hjwU3dGqK5Ssxsez1KGyMp0urpOOSTxO",
        "structure":    "flat",
        "table_format": "type_b",   # "Лист замен на DD.MM.YYYY"
        "file_filter":  None,
    },
]

# Быстрый доступ по id
CORPS_BY_ID = {c["id"]: c for c in CORPS}


def get_current_semester() -> int:
    """
    Возвращает номер текущего семестра:
      1 — сентябрь–декабрь
      2 — январь–июнь
    Июль-август — технически каникулы, возвращаем 2 (ближайший прошедший).
    """
    month = date.today().month
    if month >= 9:
        return 1
    return 2
