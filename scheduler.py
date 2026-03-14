"""
scheduler.py — фоновая проверка Drive на новые/изменённые файлы.
При новом файле рассылает каждому подписчику расписание его группы.
"""

import hashlib
import logging
import os
import io
import re
from datetime import date, timedelta

import openpyxl
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db import is_file_seen, mark_file_seen, get_file_hash, get_all_subscribers
from sheets import get_drive_files, download_xlsx, parse_schedule

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "10"))
DEFAULT_GROUP = os.environ.get("GROUP_NAME", "")
ERROR_THRESHOLD = int(os.environ.get("DRIVE_ERROR_THRESHOLD", "3"))

_consecutive_errors = 0
_alert_sent = False
# file_id -> {group_name -> parsed_data}
_last_schedules: dict[str, dict[str, dict]] = {}


# ─── Хэш ──────────────────────────────────────────────────────────────────────

def _schedule_hash(data: dict) -> str:
    pairs = data.get("pairs", [])
    content = "|".join(
        f"{p['num']}:{p['subject']}:{p['teacher']}:{p['room']}"
        for p in sorted(pairs, key=lambda x: str(x["num"]))
    )
    return hashlib.md5(content.encode()).hexdigest()


# ─── Дата ─────────────────────────────────────────────────────────────────────

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
        logger.warning("Не удалось прочитать дату из %s: %s", file_id, e)
    return None


# ─── Diff ─────────────────────────────────────────────────────────────────────

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
            changes.append(f"   🏠 {o['room']} → {n['room']}")
        if changes:
            lines.append(f"✏️ <b>{num} пара изменена</b>\n" + "\n".join(changes))
    return "\n\n".join(lines)


# ─── Уникальные группы подписчиков ────────────────────────────────────────────

def _get_subscribed_groups() -> set[str]:
    """Собирает все уникальные группы среди подписчиков."""
    groups = set()
    for sub in get_all_subscribers():
        g = sub["group_name"] or DEFAULT_GROUP
        if g:
            groups.add(g)
    return groups


# ─── Основная проверка ────────────────────────────────────────────────────────

async def _check_for_new_files(application, broadcast_new, broadcast_changed, alert_error):
    global _consecutive_errors, _alert_sent

    try:
        files = get_drive_files()
        _consecutive_errors = 0
        _alert_sent = False
    except Exception as e:
        _consecutive_errors += 1
        logger.exception("Ошибка Drive (попытка %d)", _consecutive_errors)
        if _consecutive_errors >= ERROR_THRESHOLD and not _alert_sent:
            _alert_sent = True
            await alert_error(application, str(e))
        return

    tomorrow = date.today() + timedelta(days=1)
    groups = _get_subscribed_groups()
    if not groups:
        return

    for file in files:
        file_id = file["id"]

        file_date = _extract_date(file_id)
        if file_date is None or file_date < tomorrow:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        # Парсим расписание для всех групп подписчиков
        new_scheds: dict[str, dict] = {}
        for group in groups:
            try:
                data = parse_schedule(file_id, group)
                if data:
                    new_scheds[group] = data
            except Exception as e:
                logger.warning("Ошибка парсинга группы %s: %s", group, e)

        if not new_scheds:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        # Считаем общий хэш всех групп
        combined_hash = hashlib.md5(
            "|".join(
                f"{g}:{_schedule_hash(d)}"
                for g, d in sorted(new_scheds.items())
            ).encode()
        ).hexdigest()

        if not is_file_seen(file_id):
            logger.info("Новый файл %s (дата %s) — рассылка", file_id, file_date)
            mark_file_seen(file_id, combined_hash)
            _last_schedules[file_id] = new_scheds
            await broadcast_new(application, file_id)
            break

        else:
            old_hash = get_file_hash(file_id)
            if old_hash == combined_hash:
                continue

            # Считаем diff для каждой группы
            old_scheds = _last_schedules.get(file_id, {})
            diffs: dict[str, str] = {}
            for group, new_data in new_scheds.items():
                old_data = old_scheds.get(group)
                diffs[group] = _diff_schedule(old_data, new_data) if old_data else ""

            logger.info("Файл %s изменился — уведомление", file_id)
            mark_file_seen(file_id, combined_hash)
            _last_schedules[file_id] = new_scheds
            await broadcast_changed(application, file_id, diffs)


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
    logger.info("Планировщик запущен: каждые %d мин.", CHECK_INTERVAL_MINUTES)
