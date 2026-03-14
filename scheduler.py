"""
scheduler.py — фоновая проверка Google Drive на новые файлы расписания.
Каждые N минут смотрит папку. Если появился новый файл — рассылает всем подписчикам.
"""

import asyncio
import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import is_file_seen, mark_file_seen
from sheets import get_all_files

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "10"))


async def _check_for_new_files(application, broadcast_callback):
    """Проверяет Drive. Если есть новый файл — вызывает broadcast_callback."""
    try:
        files = get_all_files()
    except Exception as e:
        logger.exception("Ошибка при получении списка файлов с Drive: %s", e)
        return

    for file in files:
        file_id = file["id"]
        file_name = file.get("name", "")

        if not is_file_seen(file_id):
            logger.info("Новый файл расписания: %s (%s)", file_name, file_id)
            mark_file_seen(file_id)
            await broadcast_callback(application, file_id)
            # Обрабатываем только один новый файл за раз, чтобы не спамить
            break


def start_scheduler(application, broadcast_callback):
    """
    Запускает APScheduler в asyncio-режиме.
    broadcast_callback(application, file_id) — async-функция рассылки.
    """
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
