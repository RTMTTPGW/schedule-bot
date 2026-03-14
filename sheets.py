"""
sheets.py — скачивание xlsx из публичной папки Google Drive и парсинг расписания.

Для получения списка файлов используется Google Drive API v3 с публичным API Key.
API Key (не Service Account!) — нужен только для чтения публичных файлов, бесплатно.

Структура xlsx-файла:
  Строка 1:  "Расписание на 16.03.2026 Понедельник верх"
  Строка 5+: название группы (объединённая ячейка на всю ширину)
  Строки после: A=номер пары, B:D=дисциплина, E:F=преподаватель, G=аудитория
"""

import os
import io
import re
import logging
import requests
import openpyxl

logger = logging.getLogger(__name__)

FOLDER_ID   = os.environ["FOLDER_ID"]
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]  # публичный API key из Google Cloud Console
REQUEST_TIMEOUT = 30


# ─── Drive API: список файлов ─────────────────────────────────────────────────

def get_drive_files() -> list[dict]:
    """
    Получает список xlsx-файлов из публичной папки через Drive API v3.
    Возвращает [{"id": "...", "name": "..."}, ...], новые файлы первыми.
    """
    url = "https://www.googleapis.com/drive/v3/files"
    params = {
        "q": f"'{FOLDER_ID}' in parents and trashed=false",
        "fields": "files(id,name,createdTime)",
        "orderBy": "createdTime desc",
        "key": GOOGLE_API_KEY,
        "pageSize": 50,
    }
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    files = resp.json().get("files", [])
    logger.info("Файлов в папке: %d", len(files))
    return files


def get_latest_file_id() -> str | None:
    """Возвращает id самого свежего файла в папке."""
    files = get_drive_files()
    return files[0]["id"] if files else None


# ─── Скачивание xlsx ──────────────────────────────────────────────────────────

def download_xlsx(file_id: str) -> bytes:
    """
    Скачивает Google Sheets файл экспортируя его как xlsx.
    Файлы в папке — нативные Google Sheets, их нельзя скачать напрямую,
    нужно использовать export URL.
    """
    # Экспортируем Google Sheets → xlsx через Drive export URL
    # Это работает для публичных файлов без авторизации
    export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export"
    params = {"format": "xlsx"}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    session = requests.Session()
    resp = session.get(export_url, params=params, headers=headers,
                       timeout=REQUEST_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()

    # Проверяем что получили xlsx (начинается с PK — zip-формат)
    if resp.content[:4] != b"PK\x03\x04":
        raise Exception(
            f"Ожидался xlsx, получено {len(resp.content)} байт. "
            f"Начало: {resp.content[:50]}"
        )

    logger.info("Экспортирован файл %s как xlsx, %d байт", file_id, len(resp.content))
    return resp.content



# ─── Парсинг xlsx ─────────────────────────────────────────────────────────────

def _cell_val(ws, row: int, col: int) -> str:
    """Читает значение ячейки с учётом объединённых ячеек."""
    cell = ws.cell(row=row, column=col)
    for rng in ws.merged_cells.ranges:
        if cell.coordinate in rng:
            val = ws.cell(rng.min_row, rng.min_col).value
            return _fmt(val)
    return _fmt(ws.cell(row=row, column=col).value)


def _fmt(val) -> str:
    """Форматирует значение ячейки: float без дробной части → int."""
    if val is None:
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    return str(val).strip()


def _is_group_header(ws, row: int) -> str | None:
    """
    Проверяет, является ли строка заголовком группы.
    Признак: ячейка A объединена минимум с 5 столбцами,
    значение не является системным заголовком.
    Возвращает название группы или None.
    """
    cell_a = ws.cell(row=row, column=1)
    for rng in ws.merged_cells.ranges:
        if rng.min_row == row and rng.min_col == 1 and rng.max_col >= 5:
            val = cell_a.value
            if not val:
                continue
            val_str = str(val).strip()
            skip = ("расписание", "учебная группа", "номер пары",
                    "дисциплина", "преподаватель", "аудитор")
            if any(s in val_str.lower() for s in skip):
                return None
            if val_str:
                return val_str
    return None


def parse_schedule(file_id: str, group_name: str) -> dict | None:
    """
    Скачивает xlsx и возвращает расписание для группы:
    {
        "date":  "16.03.2026",
        "day":   "Понедельник",
        "group": "2-24 ОРП-1",
        "pairs": [
            {"num": "1", "subject": "...", "teacher": "...", "room": "..."},
            ...
        ]
    }
    Возвращает None если группа не найдена.
    """
    data = download_xlsx(file_id)
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
    ws = wb.active
    max_row = ws.max_row

    # ── Дата и день из строки 1 ──────────────────────────────────────────────
    row1 = _cell_val(ws, 1, 1)
    date_str, day_str = "", ""
    m = re.search(r'(\d{2}\.\d{2}\.\d{4})', row1)
    if m:
        date_str = m.group(1)
    m = re.search(
        r'(понедельник|вторник|среда|четверг|пятница|суббота|воскресенье)',
        row1.lower()
    )
    if m:
        day_str = m.group(1).capitalize()

    # ── Находим блок нужной группы ───────────────────────────────────────────
    group_clean = group_name.strip().lower()
    start_row = end_row = None

    for r in range(1, max_row + 1):
        name = _is_group_header(ws, r)
        if name is None:
            continue
        if group_clean in name.lower():
            start_row = r
            for nr in range(r + 1, max_row + 2):
                if nr > max_row:
                    end_row = max_row
                    break
                if _is_group_header(ws, nr) is not None:
                    end_row = nr - 1
                    break
            break

    if start_row is None:
        logger.warning("Группа «%s» не найдена в файле %s", group_name, file_id)
        return None

    logger.info("Группа «%s» — строки %d:%d", group_name, start_row, end_row)

    # ── Читаем пары ──────────────────────────────────────────────────────────
    pairs = []
    for r in range(start_row + 1, end_row + 1):
        num     = _cell_val(ws, r, 1)   # A — номер пары
        if not num:
            continue
        # Числа читаются как float (2.0, 3.0) — приводим к int
        try:
            num = str(int(float(num)))
        except ValueError:
            pass
        subject = _cell_val(ws, r, 2)   # B (объединение B:D)
        teacher = _cell_val(ws, r, 5)   # E (объединение E:F)
        room    = _cell_val(ws, r, 7)   # G — аудитория

        if not subject and not teacher:
            continue

        pairs.append({
            "num":     num,
            "subject": subject or "—",
            "teacher": teacher,
            "room":    room,
        })

    return {
        "date":  date_str,
        "day":   day_str,
        "group": group_name,
        "pairs": pairs,
    }



# ─── Форматирование ───────────────────────────────────────────────────────────

CAL   = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
GROUP = '<tg-emoji emoji-id="5379773896352355687">🪙</tg-emoji>'
SUBJ  = '<tg-emoji emoji-id="5289733703742809329">🤓</tg-emoji>'
ROOM  = '<tg-emoji emoji-id="5416041192905265756">🏠</tg-emoji>'
SEP   = '<tg-emoji emoji-id="5332526915339173439">⚫️</tg-emoji>'


def format_schedule(data: dict) -> str:
    date  = data.get("date", "")
    day   = data.get("day", "")
    group = data.get("group", "")
    pairs = data.get("pairs", [])

    header = f"{CAL} <b>{date}"
    if day:
        header += f", {day}"
    header += f"</b>\n{GROUP} Группа: <b>{group}</b>\n"
    header += "━━━━━━━━━━━━━━━━━━"

    if not pairs:
        return header + "\n\n🎉 Занятий нет!"

    lines = [header]
    for p in pairs:
        block = f"\n{SEP} <b>{p['num']} пара</b>\n   {SUBJ} {p['subject']}"
        if p["teacher"]:
            block += f"\n   👩\u200d🏫 {p['teacher']}"
        if p["room"]:
            block += f"\n   {ROOM} {p['room']}"
        lines.append(block)

    return "\n".join(lines)
