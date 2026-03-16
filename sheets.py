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

def parse_schedule(file_id: str, group_name: str, corp_id: str = "corp3",
                   target_date: "date | None" = None) -> dict | None:
    """
    Скачивает файл и парсит расписание для группы.
    Для корпуса 2 использует горизонтальный парсер + замены.
    """
    from datetime import date as _date
    corp = CORPS_BY_ID.get(corp_id, CORPS_BY_ID["corp3"])

    if corp_id == "corp2":
        return _parse_corp2(file_id, group_name, corp, target_date or _date.today())

    xlsx = export_as_xlsx(file_id)
    result = parse_file(xlsx, corp["table_format"], group_name)
    if result:
        result["corp_id"] = corp_id
        result["corp_name"] = corp["name"]
    return result


def _parse_corp2(replacement_file_id: str, group_name: str, corp: dict,
                 target_date) -> dict | None:
    """
    Парсит расписание корпуса 2:
    1. Находит основной файл расписания в папке
    2. Получает расписание на нужный день/неделю
    3. Накладывает замены из replacement_file_id
    """
    from parser_corp2 import parse_main_schedule, parse_replacements, merge_with_replacements

    # Находим основной файл
    main_xlsx = _get_corp2_main_file()
    if main_xlsx is None:
        logger.warning("Основной файл расписания корпуса 2 не найден")
        return None

    # Парсим основное расписание
    main_data = parse_main_schedule(main_xlsx, group_name, target_date)
    if main_data is None:
        return None

    # Парсим замены из текущего файла
    try:
        rep_xlsx = export_as_xlsx(replacement_file_id)
        replacements = parse_replacements(rep_xlsx, group_name)
    except Exception as e:
        logger.warning("Ошибка загрузки замен: %s", e)
        replacements = None

    # Объединяем
    result = merge_with_replacements(main_data, replacements)
    result["corp_id"]   = "corp2"
    result["corp_name"] = corp["name"]
    return result


_corp2_main_cache: tuple | None = None  # (file_id, bytes, timestamp)

def _get_corp2_main_file() -> bytes | None:
    """Находит и кэширует основной файл расписания корпуса 2."""
    import time
    global _corp2_main_cache

    # Кэш на 1 час
    if _corp2_main_cache and time.time() - _corp2_main_cache[2] < 3600:
        logger.info("Корпус 2: основной файл из кэша")
        return _corp2_main_cache[1]

    corp2 = CORPS_BY_ID["corp2"]
    keyword = corp2.get("main_schedule_keyword", "расписание")

    try:
        from drive import get_flat_files
        logger.info("Корпус 2: получаем список файлов из папки %s", corp2["folder_id"])
        files = get_flat_files(corp2["folder_id"])
        logger.info("Корпус 2: найдено %d файлов: %s", len(files), [f["name"] for f in files[:5]])

        main_file = None
        for f in files:
            if keyword.lower() in f["name"].lower():
                main_file = f
                break

        if not main_file:
            logger.warning("Основной файл корпуса 2 не найден (ключ: '%s'), файлы: %s",
                          keyword, [f["name"] for f in files[:5]])
            return None

        logger.info("Корпус 2: скачиваем основной файл '%s' (%s)", main_file["name"], main_file["id"])
        xlsx = export_as_xlsx(main_file["id"])
        logger.info("Корпус 2: основной файл загружен, %d байт", len(xlsx))
        _corp2_main_cache = (main_file["id"], xlsx, time.time())
        return xlsx
    except Exception as e:
        logger.error("Ошибка загрузки основного файла корпуса 2: %s", e)
        return None


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
