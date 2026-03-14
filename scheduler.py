"""
scheduler.py — фоновая проверка Drive на новые/изменённые файлы.

Если несколько проверок подряд падают с ошибкой — шлёт алерт в группу.
"""

import hashlib
import logging
import os
import io
import re
from datetime import date, timedelta

import openpyxl
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import is_file_seen, mark_file_seen, get_file_hash
from sheets import get_drive_files, download_xlsx, parse_schedule, format_schedule

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "10"))
GROUP_NAME = os.environ.get("GROUP_NAME", "")

# Сколько ошибок подряд перед алертом
ERROR_THRESHOLD = int(os.environ.get("DRIVE_ERROR_THRESHOLD", "3"))
_consecutive_errors = 0
_alert_sent = False  # чтобы не спамить алертами

_last_schedule: dict[str, dict] = {}


# ─── Хэш расписания ───────────────────────────────────────────────────────────

def _schedule_hash(data: dict) -> str:
    pairs = data.get("pairs", [])
    content = "|".join(
        f"{p['num']}:{p['subject']}:{p['teacher']}:{p['room']}"
        for p in sorted(pairs, key=lambda x: str(x["num"]))
    )
    return hashlib.md5(content.encode()).hexdigest()


# ─── Дата из файла ────────────────────────────────────────────────────────────

def _extract_date(file_id: str) -> date | None:
    try:
        data = download_xlsx(file_id)
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True, read_only=True)
        ws = wb.active
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


# ─── Diff расписаний ──────────────────────────────────────────────────────────

def _diff_schedule(old_data: dict, new_data: dict) -> str:
    old_pairs = {str(p["num"]): p for p in old_data.get("pairs", [])}
    new_pairs = {str(p["num"]): p for p in new_data.get("pairs", [])}
    lines = []
    for num in sorted(set(new_pairs) - set(old_pairs)):
        p = new_pairs[num]
        lines.append(f"➕ <b>{num} пара добавлена</b>\n   📖 {p['subject']}")
    for num in sorted(set(old_pairs) - set(new_pairs)):
        p = old_pairs[num]
        lines.append(f"➖ <b>{num} пара убрана</b>\n   📖 {p['subject']}")
    for num in sorted(set(old_pairs) & set(new_pairs)):
        o, n = old_pairs[num], new_pairs[num]
        changes = []
        if o["subject"] != n["subject"]:
            changes.append(f"   📖 {o['subject']} → {n['subject']}")
        if o["teacher"] != n["teacher"]:
            changes.append(f"   👩‍🏫 {o['teacher']} → {n['teacher']}")
        if o["room"] != n["room"]:
            changes.append(f"   🏫 {o['room']} → {n['room']}")
        if changes:
            lines.append(f"✏️ <b>{num} пара изменена</b>\n" + "\n".join(changes))
    return "\n\n".join(lines)


# ─── Основная проверка ────────────────────────────────────────────────────────

async def _check_for_new_files(application, broadcast_new, broadcast_changed, alert_error):
    global _consecutive_errors, _alert_sent

    try:
        files = get_drive_files()
        # Успех — сбрасываем счётчик ошибок и флаг алерта
        _consecutive_errors = 0
        _alert_sent = False
    except Exception as e:
        _consecutive_errors += 1
        logger.exception("Ошибка получения файлов Drive (попытка %d)", _consecutive_errors)
        if _consecutive_errors >= ERROR_THRESHOLD and not _alert_sent:
            _alert_sent = True
            await alert_error(application, str(e))
        return

    tomorrow = date.today() + timedelta(days=1)

    for file in files:
        file_id = file["id"]
        try:
            sched_data = parse_schedule(file_id, GROUP_NAME)
        except Exception as e:
            logger.warning("Ошибка парсинга файла %s: %s", file_id, e)
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        if not sched_data:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        file_date = _extract_date(file_id)
        if file_date is None or file_date < tomorrow:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        new_hash = _schedule_hash(sched_data)

        if not is_file_seen(file_id):
            logger.info("Новый файл %s (дата %s) — рассылка", file_id, file_date)
            mark_file_seen(file_id, new_hash)
            _last_schedule[file_id] = sched_data
            await broadcast_new(application, sched_data)
            break
        else:
            old_hash = get_file_hash(file_id)
            if old_hash and old_hash == new_hash:
                continue
            old_sched = _last_schedule.get(file_id)
            diff_text = _diff_schedule(old_sched, sched_data) if old_sched else ""
            logger.info("Файл %s изменился — уведомление", file_id)
            mark_file_seen(file_id, new_hash)
            _last_schedule[file_id] = sched_data
            await broadcast_changed(application, sched_data, diff_text)


def start_scheduler(application, broadcast_new, broadcast_changed, alert_error):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _check_for_new_files,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        args=[application, broadcast_new, broadcast_changed, alert_error],
        id="drive_check",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Планировщик запущен: проверка каждые %d мин.", CHECK_INTERVAL_MINUTES)
