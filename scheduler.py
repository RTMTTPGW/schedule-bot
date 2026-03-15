"""
scheduler.py — проверяет корпуса каждые N минут.
Улучшения:
- Пропускает корпуса без подписчиков
- Кэширует даты файлов в SQLite
- Умный алерт: per-corp счётчик ошибок, сброс только при успехе корпуса
- Сводка после рассылки
"""

import hashlib
import logging
import os
import io
import re
from datetime import date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import CORPS, CORPS_BY_ID, get_current_semester
from db import (
    is_file_seen, mark_file_seen, get_file_hash, get_file_date_cached,
    get_all_subscribers, get_subscribed_corp_ids,
    update_scheduler_stats, get_corp_error_count,
)
from drive import get_files_for_corp, export_as_xlsx
from parser import parse_file, get_file_date

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = int(os.environ.get("CHECK_INTERVAL_MINUTES", "10"))
DEFAULT_GROUP = os.environ.get("GROUP_NAME", "")
DEFAULT_CORP  = os.environ.get("CORP_ID", "corp3")
ERROR_THRESHOLD = int(os.environ.get("DRIVE_ERROR_THRESHOLD", "3"))

_last_schedules: dict[str, dict] = {}


def _schedule_hash(data: dict) -> str:
    pairs = data.get("pairs", [])
    content = "|".join(
        f"{p['num']}:{p['subject']}:{p['teacher']}:{p['room']}"
        for p in sorted(pairs, key=lambda x: str(x["num"]))
    )
    return hashlib.md5(content.encode()).hexdigest()


def _diff_schedule(old_data: dict, new_data: dict) -> str:
    old_p = {str(p["num"]): p for p in old_data.get("pairs", [])}
    new_p = {str(p["num"]): p for p in new_data.get("pairs", [])}
    lines = []
    for num in sorted(set(new_p) - set(old_p)):
        p = new_p[num]
        lines.append(f"➕ <b>{num} пара добавлена</b>\n   📖 {p['subject']}")
    for num in sorted(set(old_p) - set(new_p)):
        p = old_p[num]
        lines.append(f"➖ <b>{num} пара убрана</b>\n   📖 {p['subject']}")
    for num in sorted(set(old_p) & set(new_p)):
        o, n = old_p[num], new_p[num]
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


def _get_groups_for_corp(corp_id: str) -> set[str]:
    groups = set()
    for sub in get_all_subscribers():
        if (sub.get("corp_id") or DEFAULT_CORP) == corp_id:
            g = sub["group_name"] or DEFAULT_GROUP
            if g:
                groups.add(g)
    return groups


def _get_file_date_with_cache(file_id: str, corp: dict) -> date | None:
    """Возвращает дату файла — сначала из кэша, потом из самого файла."""
    cached = get_file_date_cached(file_id)
    if cached:
        try:
            m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', cached)
            if m:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except Exception:
            pass

    # Кэша нет — читаем из файла
    try:
        xlsx = export_as_xlsx(file_id)
        file_date = get_file_date(xlsx, corp["table_format"])
        if file_date:
            # Сохраняем в кэш
            mark_file_seen(file_id, file_date=file_date.strftime("%d.%m.%Y"))
        return file_date
    except Exception as e:
        logger.warning("Ошибка чтения даты %s: %s", file_id, e)
        return None


async def _check_corp(corp: dict, application, broadcast_new, broadcast_changed):
    corp_id  = corp["id"]
    semester = get_current_semester()
    tomorrow = date.today() + timedelta(days=1)

    try:
        files = get_files_for_corp(corp, semester)
    except Exception as e:
        raise RuntimeError(f"[{corp['name']}] Ошибка Drive: {e}") from e

    groups = _get_groups_for_corp(corp_id)
    if not groups:
        return 0  # нет подписчиков — пропускаем

    sent_count = 0

    for file in files:
        file_id = file["id"]

        file_date = _get_file_date_with_cache(file_id, corp)
        if file_date is None or file_date < tomorrow:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        try:
            xlsx = export_as_xlsx(file_id)
        except Exception as e:
            logger.warning("[%s] Ошибка скачивания %s: %s", corp["name"], file_id, e)
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        new_scheds: dict[str, dict] = {}
        for group in groups:
            data = parse_file(xlsx, corp["table_format"], group)
            if data:
                new_scheds[group] = data

        if not new_scheds:
            if not is_file_seen(file_id):
                mark_file_seen(file_id)
            continue

        combined_hash = hashlib.md5(
            "|".join(
                f"{g}:{_schedule_hash(d)}"
                for g, d in sorted(new_scheds.items())
            ).encode()
        ).hexdigest()

        if not is_file_seen(file_id):
            logger.info("[%s] Новый файл %s → рассылка", corp["name"], file_id)
            mark_file_seen(file_id, combined_hash, file_date.strftime("%d.%m.%Y"))
            for g, d in new_scheds.items():
                _last_schedules[f"{file_id}:{corp_id}:{g}"] = d
            sent_count = await broadcast_new(application, file_id, corp_id)
            break
        else:
            old_hash = get_file_hash(file_id)
            if old_hash == combined_hash:
                continue
            diffs: dict[str, str] = {}
            for group, new_data in new_scheds.items():
                key = f"{file_id}:{corp_id}:{group}"
                old_data = _last_schedules.get(key)
                diffs[f"{corp_id}:{group}"] = _diff_schedule(old_data, new_data) if old_data else ""
                _last_schedules[key] = new_data
            logger.info("[%s] Файл %s изменился → уведомление", corp["name"], file_id)
            mark_file_seen(file_id, combined_hash, file_date.strftime("%d.%m.%Y"))
            sent_count = await broadcast_changed(application, file_id, corp_id, diffs)

    return sent_count


async def _check_all_corps(application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done):
    # Получаем корпуса у которых есть подписчики
    active_corps = get_subscribed_corp_ids()
    # Добавляем дефолтный корпус
    active_corps.add(DEFAULT_CORP)

    for corp in CORPS:
        corp_id = corp["id"]
        if corp_id not in active_corps:
            logger.debug("Корпус %s пропущен (нет подписчиков)", corp_id)
            continue

        error_count, alert_sent = get_corp_error_count(corp_id)

        try:
            sent = await _check_corp(corp, application, broadcast_new, broadcast_changed)
            update_scheduler_stats(corp_id, success=True)

            if sent and sent > 0:
                await on_broadcast_done(application, corp["name"], sent)

        except Exception as e:
            error_count += 1
            logger.exception("Ошибка проверки корпуса %s (попытка %d)", corp_id, error_count)

            send_alert = error_count >= ERROR_THRESHOLD and not alert_sent
            update_scheduler_stats(corp_id, success=False,
                                   error_count=error_count, alert_sent=send_alert)

            if send_alert:
                await alert_error(application, f"[{corp['name']}] {e}")


def start_scheduler(application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _check_all_corps,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        args=[application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done],
        id="drive_check",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Планировщик запущен: каждые %d мин., корпусов: %d", CHECK_INTERVAL_MINUTES, len(CORPS))
