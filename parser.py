"""
parser.py — универсальный парсер таблиц расписания.

Поддерживает три формата:
  type_a — корпус 3: "Расписание на DD.MM.YYYY День"
           A=номер пары, B:D=дисциплина, E:F=преподаватель, G=аудитория
           Группа — объединённая ячейка на всю ширину

  type_b — корпуса 1, 4: "Лист замен на DD.MM.YYYY День"
           Та же структура что type_a, может быть столбец Территория (H)

  type_c — корпус 2: строки 1-3 заголовок с датой
           A=пара (текст "1пара"/"1 пара"), B=дисциплина, C=преподаватель, D=кабинет
           Группа — объединённая ячейка, название может быть слитным
"""

import io
import re
import logging
import openpyxl
from datetime import date

logger = logging.getLogger(__name__)

DAYS_RU = {
    "понедельник", "вторник", "среда", "четверг",
    "пятница", "суббота", "воскресенье"
}


# ─── Хелперы ──────────────────────────────────────────────────────────────────

def _fmt(val) -> str:
    """Форматирует значение ячейки. float без дробной части → int."""
    if val is None:
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    return str(val).strip()


def _cell(ws, row: int, col: int) -> str:
    """Читает ячейку с учётом объединений."""
    cell = ws.cell(row=row, column=col)
    for rng in ws.merged_cells.ranges:
        if cell.coordinate in rng:
            val = ws.cell(rng.min_row, rng.min_col).value
            return _fmt(val)
    return _fmt(cell.value)


def _extract_date(text: str) -> date | None:
    """Извлекает дату из строки вида DD.MM.YYYY."""
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    return None


def _extract_day(text: str) -> str:
    """Извлекает день недели из строки."""
    for day in DAYS_RU:
        if day in text.lower():
            return day.capitalize()
    return ""


def _is_group_header(ws, row: int, min_span: int = 4) -> str | None:
    """
    Проверяет является ли строка заголовком группы.
    Признак: ячейка A объединена с соседними (min_span столбцов),
    значение не системный заголовок.
    """
    cell_a = ws.cell(row=row, column=1)
    for rng in ws.merged_cells.ranges:
        if rng.min_row == row and rng.min_col == 1 and rng.max_col >= min_span:
            val = cell_a.value
            if not val:
                continue
            val_str = str(val).strip()
            skip = (
                "расписание", "учебная группа", "номер пары",
                "дисциплина", "преподаватель", "аудитор",
                "лист замен", "изменения", "корпус", "гб поу",
                "пара", "кабинет", "территория",
            )
            if any(s in val_str.lower() for s in skip):
                return None
            if val_str:
                return val_str
    return None


def _is_valid_group_query(query: str) -> bool:
    """
    Проверяет что запрос достаточно конкретный чтобы искать группу.
    Минимальные требования:
    - длина >= 4 символов
    - содержит хотя бы одну цифру
    - содержит хотя бы одну букву
    - не состоит только из цифр или только из букв
    """
    q = query.strip()
    q = q.strip()
    if len(q) < 4:
        return False
    has_digit = any(c.isdigit() for c in q)
    has_alpha = any(c.isalpha() for c in q)
    if not (has_digit and has_alpha):
        return False
    # Должен содержать дефис или начинаться с цифры (формат групп: "2-24 ОРП-1", "ОРП-1")
    starts_with_digit = q[0].isdigit()
    has_hyphen = '-' in q
    return starts_with_digit or has_hyphen


def _group_matches(group_header: str, group_query: str) -> bool:
    """
    Проверяет совпадение названия группы.

    Стратегии (применяются по порядку, первое совпадение = match):
    1. Точное совпадение после нормализации пробелов
    2. Запрос является началом заголовка (корпус 2: "1-25 ПКД-10" в "1-25 ПКД-10Поварское...")
    3. Запрос без пробелов входит в начало заголовка без пробелов (слитное написание)
    """
    # Сначала проверяем что запрос вообще похож на название группы
    if not _is_valid_group_query(group_query):
        return False

    def norm_spaces(s: str) -> str:
        return re.sub(r'\s+', ' ', s).strip().lower()

    def norm_nospaces(s: str) -> str:
        return re.sub(r'\s+', '', s).lower()

    h = norm_spaces(group_header)
    q = norm_spaces(group_query)

    # 1. Точное совпадение
    if h == q:
        return True

    # 2. Заголовок начинается с запроса
    if h.startswith(q):
        rest = h[len(q):]
        if not rest or not rest[0].isalnum():
            return True

    # 3. Без пробелов — запрос входит в начало заголовка
    hn = norm_nospaces(group_header)
    qn = norm_nospaces(group_query)
    if hn.startswith(qn):
        rest = hn[len(qn):]
        if not rest or not rest[0].isdigit():
            return True

    return False


# ─── Парсер type_a / type_b ───────────────────────────────────────────────────

def _parse_type_ab(ws, group_query: str) -> dict | None:
    """
    Парсит таблицы корпусов 1, 3, 4.
    Строка 1: "Расписание на DD.MM.YYYY День" или "Лист замен на DD.MM.YYYY День"
    Группы — объединённые ячейки на всю ширину.
    A=номер, B:D=предмет, E:F=преподаватель, G=аудитория
    """
    max_row = ws.max_row

    # Дата и день из строки 1
    row1 = _cell(ws, 1, 1)
    file_date = _extract_date(row1)
    day_str = _extract_day(row1)

    # Ищем группу
    start_row = end_row = None
    for r in range(1, max_row + 1):
        name = _is_group_header(ws, r, min_span=4)
        if name is None:
            continue
        if _group_matches(name, group_query):
            start_row = r
            for nr in range(r + 1, max_row + 2):
                if nr > max_row:
                    end_row = max_row
                    break
                if _is_group_header(ws, nr, min_span=4) is not None:
                    end_row = nr - 1
                    break
            break

    if start_row is None:
        return None

    # Читаем пары
    pairs = []
    for r in range(start_row + 1, end_row + 1):
        num = _cell(ws, r, 1)
        if not num:
            continue
        try:
            num = str(int(float(num)))
        except ValueError:
            # Диапазон типа "1-3"
            if not re.match(r'\d', num):
                continue

        subject = _cell(ws, r, 2)
        teacher = _cell(ws, r, 5)
        room    = _cell(ws, r, 7)

        if not subject and not teacher:
            continue

        pairs.append({
            "num":     num,
            "subject": subject or "—",
            "teacher": teacher,
            "room":    room,
        })

    return {
        "date":  file_date.strftime("%d.%m.%Y") if file_date else "",
        "day":   day_str,
        "group": group_query,
        "pairs": pairs,
    }


# ─── Парсер type_c ────────────────────────────────────────────────────────────

def _parse_type_c(ws, group_query: str) -> dict | None:
    """
    Парсит таблицы корпуса 2.
    Строки 1-3: заголовок с датой ("Изменения в расписании на DD.MM.YYYY")
    A=пара ("1пара"/"1 пара"), B=предмет, C=преподаватель, D=кабинет
    Группа — объединённая ячейка (название может быть слитным: "1-25ПКД-10Поварское...")
    """
    max_row = ws.max_row

    # Дата из первых трёх строк
    file_date = None
    day_str = ""
    for r in range(1, 4):
        val = _cell(ws, r, 1)
        if not val:
            # Попробуем другие столбцы
            for c in range(1, 5):
                val = _cell(ws, r, c)
                if val:
                    break
        if val:
            if not file_date:
                file_date = _extract_date(val)
            if not day_str:
                day_str = _extract_day(val)

    # Ищем группу — объединённая ячейка, может быть слитной
    start_row = end_row = None
    for r in range(1, max_row + 1):
        name = _is_group_header(ws, r, min_span=2)
        if name is None:
            continue
        if _group_matches(name, group_query):
            start_row = r
            for nr in range(r + 1, max_row + 2):
                if nr > max_row:
                    end_row = max_row
                    break
                if _is_group_header(ws, nr, min_span=2) is not None:
                    end_row = nr - 1
                    break
            break

    if start_row is None:
        return None

    # Читаем пары
    pairs = []
    for r in range(start_row + 1, end_row + 1):
        raw_num = _cell(ws, r, 1)
        if not raw_num:
            continue

        # Нормализуем "1пара" / "1 пара" / "1" → "1"
        m = re.match(r'^(\d+)', raw_num.strip())
        if not m:
            continue
        num = m.group(1)

        subject = _cell(ws, r, 2)
        teacher = _cell(ws, r, 3)
        room    = _cell(ws, r, 4)

        if not subject and not teacher:
            continue

        pairs.append({
            "num":     num,
            "subject": subject or "—",
            "teacher": teacher,
            "room":    room,
        })

    return {
        "date":  file_date.strftime("%d.%m.%Y") if file_date else "",
        "day":   day_str,
        "group": group_query,
        "pairs": pairs,
    }


# ─── Публичный интерфейс ──────────────────────────────────────────────────────

def parse_file(xlsx_bytes: bytes, table_format: str, group_query: str) -> dict | None:
    """
    Парсит xlsx-файл и возвращает расписание для группы.
    table_format: "type_a", "type_b", "type_c"
    Возвращает None если группа не найдена.
    """
    try:
        wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)
        ws = wb.active
    except Exception as e:
        logger.error("Ошибка открытия файла: %s", e)
        return None

    if table_format in ("type_a", "type_b"):
        return _parse_type_ab(ws, group_query)
    elif table_format == "type_c":
        return _parse_type_c(ws, group_query)
    else:
        logger.warning("Неизвестный формат таблицы: %s", table_format)
        return None


def get_file_date(xlsx_bytes: bytes, table_format: str) -> date | None:
    """Извлекает дату из файла без полного парсинга."""
    try:
        wb = openpyxl.load_workbook(
            io.BytesIO(xlsx_bytes), data_only=True, read_only=True
        )
        ws = wb.active
        # Для всех форматов дата в первых 3 строках
        for r in range(1, 4):
            for row in ws.iter_rows(min_row=r, max_row=r, values_only=True):
                for cell in row:
                    if cell:
                        d = _extract_date(str(cell))
                        if d:
                            wb.close()
                            return d
        wb.close()
    except Exception as e:
        logger.warning("Ошибка чтения даты: %s", e)
    return None
