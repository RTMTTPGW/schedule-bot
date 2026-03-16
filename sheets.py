"""
sheets.py — высокоуровневый интерфейс для получения расписания.
Использует drive.py (работа с Drive API) и parser.py (парсинг таблиц).
"""

import logging
from datetime import date, timedelta

from config import CORPS_BY_ID, get_current_semester
from drive import get_files_for_corp, export_as_xlsx
from parser import parse_file, get_file_date

logger = logging.getLogger(__name__)

# Премиум эмодзи для форматирования
CAL   = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
GROUP = '<tg-emoji emoji-id="5379773896352355687">🪙</tg-emoji>'
SUBJ  = '<tg-emoji emoji-id="5289733703742809329">🤓</tg-emoji>'
ROOM  = '<tg-emoji emoji-id="5416041192905265756">🏠</tg-emoji>'
SEP   = '<tg-emoji emoji-id="5397782960512444700">📌</tg-emoji>'


# ─── Поиск файлов ─────────────────────────────────────────────────────────────

def get_drive_files(corp_id: str | None = None) -> list[dict]:
    """
    Возвращает список файлов для корпуса.
    Если corp_id не задан — использует первый корпус (обратная совместимость).
    """
    corp_id = corp_id or "corp3"
    corp = CORPS_BY_ID.get(corp_id)
    if not corp:
        raise ValueError(f"Неизвестный корпус: {corp_id}")
    semester = get_current_semester()
    return get_files_for_corp(corp, semester)


def get_latest_file_id(corp_id: str = "corp3") -> str | None:
    """Последний файл в папке корпуса."""
    files = get_drive_files(corp_id)
    return files[0]["id"] if files else None


def get_today_file_id(corp_id: str = "corp3") -> str | None:
    """Последний файл с датой <= сегодня."""
    files = get_drive_files(corp_id)
    corp = CORPS_BY_ID[corp_id]
    today = date.today()
    for f in files:
        try:
            xlsx = export_as_xlsx(f["id"])
            file_date = get_file_date(xlsx, corp["table_format"])
            if file_date and file_date <= today:
                return f["id"]
        except Exception as e:
            logger.warning("Ошибка чтения даты файла %s: %s", f["id"], e)
    return files[0]["id"] if files else None


# ─── Парсинг расписания ───────────────────────────────────────────────────────

def parse_schedule(file_id: str, group_name: str, corp_id: str = "corp3") -> dict | None:
    """Скачивает файл и парсит расписание для группы."""
    corp = CORPS_BY_ID.get(corp_id, CORPS_BY_ID["corp3"])
    xlsx = export_as_xlsx(file_id)
    result = parse_file(xlsx, corp["table_format"], group_name)
    if result:
        result["corp_id"] = corp_id
        result["corp_name"] = corp["name"]
    return result


# ─── Форматирование ───────────────────────────────────────────────────────────

def format_schedule(data: dict) -> str:
    """Форматирует расписание в HTML для Telegram."""
    d     = data.get("date", "")
    day   = data.get("day", "")
    group = data.get("group", "")
    corp  = data.get("corp_name", "")
    pairs = data.get("pairs", [])

    header = f"{CAL} <b>{d}"
    if day:
        header += f", {day}"
    header += f"</b>\n{GROUP} Группа: <b>{group}</b>"
    if corp:
        header += f" · {corp}"

    if not pairs:
        return header + "\n\n🎉 Занятий нет!"

    lines = [header]
    for p in pairs:
        block = f"\n{SEP} <b>{p['num']} пара</b>\n   {SUBJ} {p['subject']}"
        if p.get("teacher"):
            block += f"\n   👩\u200d🏫 {p['teacher']}"
        if p.get("room"):
            block += f"\n   {ROOM} {p['room']}"
        lines.append(block)

    return "\n".join(lines)
