"""
drive.py — работа с Google Drive API.
Получение списка файлов из папок, рекурсивный обход для корпуса 1.
"""

import os
import io
import re
import time
import logging
import requests
from datetime import date, datetime

# ─── TTL кэш ──────────────────────────────────────────────────────────────────
_FOLDER_TTL = 300   # 5 минут — список файлов меняется редко
_XLSX_TTL   = 600   # 10 минут — файл часто читается несколько раз подряд
_MAX_XLSX   = 20    # ~20 × ~200 КБ = ~4 МБ памяти

_folder_cache: dict[str, tuple[list, float]] = {}
_xlsx_cache:   dict[str, tuple[bytes, float]] = {}

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
REQUEST_TIMEOUT = 30


# ─── Базовые операции Drive API ───────────────────────────────────────────────

def list_folder(folder_id: str) -> list[dict]:
    """Возвращает содержимое папки. Результат кэшируется на 5 минут."""
    now = time.monotonic()
    cached = _folder_cache.get(folder_id)
    if cached and now - cached[1] < _FOLDER_TTL:
        return cached[0]

    url = "https://www.googleapis.com/drive/v3/files"
    params = {
        "q": f"'{folder_id}' in parents and trashed=false",
        "fields": "files(id,name,mimeType,createdTime)",
        "orderBy": "createdTime desc",
        "key": GOOGLE_API_KEY,
        "pageSize": 100,
    }
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    files = resp.json().get("files", [])
    _folder_cache[folder_id] = (files, time.monotonic())
    return files


def is_folder(item: dict) -> bool:
    return item.get("mimeType") == "application/vnd.google-apps.folder"


def is_spreadsheet(item: dict) -> bool:
    return item.get("mimeType") in (
        "application/vnd.google-apps.spreadsheet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def export_as_xlsx(file_id: str) -> bytes:
    """Экспортирует Google Sheets файл как xlsx. Результат кэшируется на 10 минут."""
    now = time.monotonic()
    cached = _xlsx_cache.get(file_id)
    if cached and now - cached[1] < _XLSX_TTL:
        return cached[0]

    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(
        url, params={"format": "xlsx"},
        headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True
    )
    resp.raise_for_status()
    if resp.content[:4] != b"PK\x03\x04":
        raise Exception(f"Ожидался xlsx, получено {len(resp.content)} байт")
    logger.info("Экспортирован файл %s, %d байт", file_id, len(resp.content))
    # Кэшируем с ограничением размера
    if len(_xlsx_cache) >= _MAX_XLSX:
        oldest = min(_xlsx_cache, key=lambda k: _xlsx_cache[k][1])
        del _xlsx_cache[oldest]
    _xlsx_cache[file_id] = (resp.content, time.monotonic())
    return resp.content


# ─── Получение файлов для каждого типа структуры ─────────────────────────────

def get_flat_files(folder_id: str, file_filter: list[str] | None = None) -> list[dict]:
    """
    Файлы сразу в папке (корпуса 2, 3, 4).
    Возвращает список файлов отсортированных по createdTime desc.
    """
    items = list_folder(folder_id)
    files = [i for i in items if is_spreadsheet(i)]
    if file_filter:
        filtered = [
            f for f in files
            if any(kw.lower() in f["name"].lower() for kw in file_filter)
        ]
        if filtered:
            files = filtered
    logger.info("Папка %s: найдено %d файлов", folder_id, len(files))
    return files


def get_nested_files(
    folder_id: str,
    semester: int,
    file_filter: list[str] | None = None,
) -> list[dict]:
    """
    Вложенная структура (корпус 1):
    корневая папка → [Архив, 1 семестр, 2 семестр]
                   → папки по дням
                   → файлы

    Выбирает папку нужного семестра по ключевому слову.
    """
    root_items = list_folder(folder_id)
    folders = [i for i in root_items if is_folder(i)]

    # Ищем папку семестра
    semester_kw = ["перв", "1 сем"] if semester == 1 else ["втор", "2 сем"]
    semester_folder = None
    for f in folders:
        name_lower = f["name"].lower()
        if any(kw in name_lower for kw in semester_kw):
            semester_folder = f
            break

    if not semester_folder:
        # Если не нашли — берём первую не-архивную папку
        for f in folders:
            if "архив" not in f["name"].lower():
                semester_folder = f
                break

    if not semester_folder:
        logger.warning("Папка семестра %d не найдена в %s", semester, folder_id)
        return []

    logger.info("Семестр %d: папка '%s'", semester, semester_folder["name"])

    # Внутри семестра — папки по дням
    day_folders = [i for i in list_folder(semester_folder["id"]) if is_folder(i)]

    # Сортируем папки по дням — пытаемся извлечь дату из названия
    def folder_date_key(f):
        m = re.search(r'(\d{2})[.\-_](\d{2})[.\-_](\d{4})', f["name"])
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass
        return date.min

    day_folders.sort(key=folder_date_key, reverse=True)

    # Собираем файлы из всех папок по дням
    all_files = []
    for day_folder in day_folders:
        items = list_folder(day_folder["id"])
        files = [i for i in items if is_spreadsheet(i)]
        if file_filter:
            filtered = [
                f for f in files
                if any(kw.lower() in f["name"].lower() for kw in file_filter)
            ]
            if filtered:
                files = filtered
        # Добавляем метаданные папки для извлечения даты
        for f in files:
            f["_day_folder"] = day_folder["name"]
        all_files.extend(files)

    logger.info("Корпус 1, семестр %d: найдено %d файлов", semester, len(all_files))
    return all_files


def get_files_for_corp(corp: dict, semester: int) -> list[dict]:
    """Возвращает список файлов для корпуса в зависимости от типа структуры."""
    if corp["structure"] == "flat":
        return get_flat_files(corp["folder_id"], corp.get("file_filter"))
    elif corp["structure"] == "nested":
        return get_nested_files(corp["folder_id"], semester, corp.get("file_filter"))
    else:
        logger.warning("Неизвестный тип структуры: %s", corp["structure"])
        return []
