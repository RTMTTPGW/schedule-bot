"""
scheduler.py — фоновая проверка Google Drive на новые файлы расписания.

Логика:
  Каждые N минут смотрим папку Drive.
  Если появился новый файл (не помечен как просмотренный) —
  читаем дату из первой строки файла ("Расписание на 16.03.2026 ...").
  Если дата в файле >= завтра (т.е. расписание на будущий день) — рассылаем.
  Если дата сегодняшняя или прошедшая — помечаем как просмотренный, но не рассылаем.
"""

import logging
import os
import io
import re
from datetime import datetime, date, timedelta

import openpyxl
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import is_file_seen, mark_file_seen
from sheets import get_drive_files, download_xlsx

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "10"))


def _extract_date_from_file(file_id: str) -> date | None:
    """
    Скачивает файл и читает дату из строки 1.
    Пример строки: "Расписание на 16.03.2026 Понедельник верх"
    Возвращает объект date или None если не удалось распознать.
    """
    try:
        data = download_xlsx(file_id)
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True, read_only=True)
        ws = wb.active

        # Читаем первую строку — там дата
        row1_val = ""
        for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
            for cell in row:
                if cell:
                    row1_val = str(cell).strip()
                    break
            break

        wb.close()

        m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', row1_val)
        if m:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))

    except Exception as e:
        logger.warning("Не удалось прочитать дату из файла %s: %s", file_id, e)

    return None


async def _check_for_new_files(application, broadcast_callback):
    """
    Проверяет Drive на новые файлы.
    Рассылает только те у которых дата >= завтра.
    """
    try:
        files = get_drive_files()
    except Exception as e:
        logger.exception("Ошибка получения списка файлов Drive: %s", e)
        return

    today = date.today()
    tomorrow = today + timedelta(days=1)

    for file in files:
        file_id = file["id"]

        if is_file_seen(file_id):
            continue

        # Читаем дату из файла
        file_date = _extract_date_from_file(file_id)

        if file_date is None:
            # Не смогли прочитать дату — помечаем и пропускаем
            logger.warning("Файл %s: дата не распознана, пропускаем", file_id)
            mark_file_seen(file_id)
            continue

        # Помечаем файл как просмотренный в любом случае
        mark_file_seen(file_id)

        if file_date < tomorrow:
            # Расписание на сегодня или прошедший день — не рассылаем
            logger.info(
                "Файл %s: дата %s — сегодня или прошлое, рассылка не нужна",
                file_id, file_date,
            )
            continue

        # Расписание на будущий день — рассылаем!
        logger.info(
            "Файл %s: дата %s — будущий день, запускаем рассылку",
            file_id, file_date,
        )
        await broadcast_callback(application, file_id)
        # Один файл за раз чтобы не спамить
        break


def start_scheduler(application, broadcast_callback):
    """Запускает APScheduler в asyncio-режиме."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _check_for_new_files,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        args=[application, broadcast_callback],
        id="drive_check",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Планировщик запущен: проверка Drive каждые %d мин.",
        CHECK_INTERVAL_MINUTES,
    )
