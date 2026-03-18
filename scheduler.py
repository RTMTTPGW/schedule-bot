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


NEW_MARK = '<tg-emoji emoji-id="5382357040008021292">🆕</tg-emoji>'


def _diff_schedule(old_data: dict, new_data: dict) -> str:
    old_p = {str(p["num"]): p for p in old_data.get("pairs", [])}
    new_p = {str(p["num"]): p for p in new_data.get("pairs", [])}
    nm = NEW_MARK
    lines = []
    for num in sorted(set(new_p) - set(old_p)):
        p = new_p[num]
        subj = p['subject']
        lines.append(f"\u2795 <b>{num} пара добавлена</b>\n   \U0001f4d6 {subj}")
    for num in sorted(set(old_p) - set(new_p)):
        p = old_p[num]
        subj = p['subject']
        lines.append(f"\u2796 <b>{num} пара убрана</b>\n   \U0001f4d6 {subj}")
    for num in sorted(set(old_p) & set(new_p)):
        o, n = old_p[num], new_p[num]
        changes = []
        if o["subject"] != n["subject"]:
            changes.append(
                "   \U0001f4d6 " + nm + " " + n["subject"] + " " + nm +
                "\n      (было: " + o["subject"] + ")"
            )
        if o["teacher"] != n["teacher"]:
            changes.append(
                "   \U0001f469\u200d\U0001f3eb " + nm + " " + n["teacher"] + " " + nm +
                "\n      (было: " + o["teacher"] + ")"
            )
        if o["room"] != n["room"]:
            changes.append(
                "   \U0001f3e0 " + nm + " " + n["room"] + " " + nm +
                "\n      (было: " + o["room"] + ")"
            )
        if changes:
            lines.append(
                "\u270f\ufe0f <b>" + num + " пара изменена</b>\n" +
                "\n".join(changes)
            )
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
        logger.info("[%s] Нет подписчиков — пропускаем", corp["name"])
        return 0

    logger.info("[%s] Проверяем %d файлов, групп подписчиков: %d",
                corp["name"], len(files), len(groups))
    sent_count = 0

    for file in files:
        file_id = file["id"]

        file_date = _get_file_date_with_cache(file_id, corp)
        if file_date is None or file_date < tomorrow:
            # Помечаем только если дата точно в прошлом (не None)
            # None означает что не смогли прочитать — попробуем снова в следующий раз
            if file_date is not None and not is_file_seen(file_id):
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
    # Если есть подписчики без corp_id — добавляем дефолтный корпус
    # (подписались до того как выбрали корпус)
    all_subs = get_all_subscribers()
    has_unconfigured = any(not s.get("corp_id") for s in all_subs)
    if has_unconfigured or all_subs:
        active_corps.add(DEFAULT_CORP)
    # Если вообще есть подписчики — проверяем все корпуса где они есть
    for s in all_subs:
        if s.get("corp_id"):
            active_corps.add(s["corp_id"])

    logger.info("Проверка Drive: %d корпусов активны %s, подписчиков: %d",
                len(active_corps), list(active_corps), len(all_subs))

    for corp in CORPS:
        corp_id = corp["id"]
        if corp_id not in active_corps:
            logger.info("Корпус %s пропущен (нет подписчиков)", corp_id)
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


def _get_interval_minutes() -> int:
    """
    Умный интервал проверки:
    - Воскресенье: раз в час (расписание уже появилось в пт/сб)
    - Остальные дни: CHECK_INTERVAL_MINUTES
    """
    from datetime import datetime
    if datetime.now().weekday() == 6:  # 6 = воскресенье
        return 60
    return CHECK_INTERVAL_MINUTES


async def _smart_check(application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done):
    """Обёртка с умным интервалом — перепланирует себя в зависимости от дня недели."""
    await _check_all_corps(application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done)

    # Обновляем интервал следующего запуска
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    scheduler = application.bot_data.get("scheduler")
    if scheduler:
        interval = _get_interval_minutes()
        scheduler.reschedule_job("drive_check", trigger="interval", minutes=interval)
        logger.debug("Следующая проверка через %d мин.", interval)


def start_scheduler(application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done):
    scheduler = AsyncIOScheduler()
    application.bot_data["scheduler"] = scheduler

    interval = _get_interval_minutes()
    scheduler.add_job(
        _smart_check,
        trigger="interval",
        minutes=interval,
        args=[application, broadcast_new, broadcast_changed, alert_error, on_broadcast_done],
        id="drive_check",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Планировщик запущен: каждые %d мин. (сейчас), корпусов: %d", interval, len(CORPS))
