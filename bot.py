import os
import time
import logging
from datetime import date

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from db import (
    init_db,
    add_subscriber, remove_subscriber, get_all_subscribers,
    get_gif_file_id, save_gif_file_id,
)
from sheets import get_latest_file_id, get_today_file_id, parse_schedule, format_schedule
from scheduler import start_scheduler

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN      = os.environ["BOT_TOKEN"]
GROUP_NAME = os.environ.get("GROUP_NAME", "")
# ID группы куда слать алерты об ошибках Drive (задаётся в Railway)
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "")
GIF_PATH   = os.path.join(os.path.dirname(__file__), "emoji.mp4")

# ─── Премиум эмодзи ───────────────────────────────────────────────────────────
WAVE  = '<tg-emoji emoji-id="5319016550248751722">👋</tg-emoji>'
CAL   = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
BELL  = '<tg-emoji emoji-id="5458603043203327669">🔔</tg-emoji>'
WARN  = '<tg-emoji emoji-id="5447644880824181073">⚠️</tg-emoji>'
NEW   = '<tg-emoji emoji-id="5382357040008021292">🆕</tg-emoji>'
CHECK = '<tg-emoji emoji-id="5206607081334906820">✔️</tg-emoji>'
CROSS = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'
CLOCK = '<tg-emoji emoji-id="5386367538735104399">⌛</tg-emoji>'

# ─── Cooldown ─────────────────────────────────────────────────────────────────
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "30"))
# chat_id -> timestamp последнего успешного запроса
_last_request: dict[int, float] = {}


def _check_cooldown(chat_id: int) -> int | None:
    """Возвращает сколько секунд осталось ждать, или None если можно отвечать."""
    last = _last_request.get(chat_id)
    if last is None:
        return None
    elapsed = time.time() - last
    remaining = int(COOLDOWN_SECONDS - elapsed)
    return remaining if remaining > 0 else None


def _set_cooldown(chat_id: int):
    _last_request[chat_id] = time.time()


# ─── GIF file_id (хранится в SQLite) ─────────────────────────────────────────
_gif_file_id: str = ""


def _get_gif_id() -> str:
    global _gif_file_id
    if not _gif_file_id:
        _gif_file_id = get_gif_file_id() or os.environ.get("GIF_FILE_ID", "")
    return _gif_file_id


# ─── Отправка гифки с текстом одним сообщением ───────────────────────────────

async def _send_with_gif(bot, chat_id: int, text: str):
    """Отправляет гифку + текст одним сообщением (caption)."""
    global _gif_file_id
    gif_id = _get_gif_id()
    try:
        if gif_id:
            await bot.send_animation(
                chat_id=chat_id,
                animation=gif_id,
                caption=text,
                parse_mode="HTML",
            )
        else:
            with open(GIF_PATH, "rb") as f:
                msg = await bot.send_animation(
                    chat_id=chat_id,
                    animation=f,
                    caption=text,
                    parse_mode="HTML",
                )
            _gif_file_id = msg.animation.file_id
            save_gif_file_id(_gif_file_id)
            logger.info("Гифка сохранена, file_id: %s", _gif_file_id)
    except Exception as e:
        logger.warning("Ошибка гифки, шлю только текст: %s", e)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


# ─── Хелпер получения и отправки расписания ──────────────────────────────────

async def _fetch_and_reply(msg, bot, chat_id: int, file_id: str, prefix: str = ""):
    """Парсит файл и отправляет расписание. msg — сообщение-заглушка для удаления."""
    data = parse_schedule(file_id, GROUP_NAME)
    if not data:
        await msg.edit_text(
            f'{CROSS} Группа <b>{GROUP_NAME}</b> не найдена в файле.',
            parse_mode="HTML",
        )
        return
    await msg.delete()
    text = (prefix + format_schedule(data)) if prefix else format_schedule(data)
    await _send_with_gif(bot, chat_id, text)


# ─── Команды ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"{WAVE} Привет! Я бот расписания.\n\n"
        f"{CAL} /today — расписание на сегодня\n"
        f"{NEW} /new — последнее новое расписание\n"
        f"{BELL} /subscribe — подписаться на авторассылку\n"
        f"{CROSS} /unsubscribe — отписаться",
        parse_mode="HTML",
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Расписание из последнего файла с датой <= сегодня."""
    chat_id = update.effective_chat.id
    wait = _check_cooldown(chat_id)
    if wait:
        await update.message.reply_text(
            f"{CLOCK} Подожди ещё <b>{wait} сек.</b> перед следующим запросом.",
            parse_mode="HTML",
        )
        return

    msg = await update.message.reply_text(f"{CLOCK} Загружаю расписание...", parse_mode="HTML")
    try:
        file_id = get_today_file_id()
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов в папке Drive не найдено.", parse_mode="HTML")
            return
        _set_cooldown(chat_id)
        await _fetch_and_reply(msg, context.bot, chat_id, file_id)
    except Exception as e:
        logger.exception("Ошибка /today")
        await msg.edit_text(f"{WARN} Ошибка: {e}", parse_mode="HTML")


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Расписание из последнего загруженного файла (может быть на будущий день)."""
    chat_id = update.effective_chat.id
    wait = _check_cooldown(chat_id)
    if wait:
        await update.message.reply_text(
            f"{CLOCK} Подожди ещё <b>{wait} сек.</b> перед следующим запросом.",
            parse_mode="HTML",
        )
        return

    msg = await update.message.reply_text(f"{CLOCK} Загружаю расписание...", parse_mode="HTML")
    try:
        file_id = get_latest_file_id()
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов в папке Drive не найдено.", parse_mode="HTML")
            return
        _set_cooldown(chat_id)
        await _fetch_and_reply(msg, context.bot, chat_id, file_id)
    except Exception as e:
        logger.exception("Ошибка /new")
        await msg.edit_text(f"{WARN} Ошибка: {e}", parse_mode="HTML")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        f"{CHECK} Подписка оформлена!\n"
        "Когда появится новый файл расписания на будущий день — пришлю автоматически.",
        parse_mode="HTML",
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remove_subscriber(update.effective_chat.id)
    await update.message.reply_text(f"{CROSS} Вы отписаны.", parse_mode="HTML")


# ─── Авторассылка ─────────────────────────────────────────────────────────────

async def broadcast(application: Application, sched_data: dict):
    text = f"{NEW} <b>Новое расписание!</b>\n\n" + format_schedule(sched_data)
    for chat_id in get_all_subscribers():
        try:
            await _send_with_gif(application.bot, chat_id, text)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def broadcast_changed(application: Application, sched_data: dict, diff_text: str):
    d = sched_data.get("date", "")
    day = sched_data.get("day", "")
    day_str = f", {day}" if day else ""
    if diff_text:
        text = (
            f"{WARN} <b>Расписание на {d}{day_str} изменилось!</b>\n\n"
            + diff_text
            + "\n\n📋 Актуальное расписание:\n\n"
            + format_schedule(sched_data)
        )
    else:
        text = (
            f"{WARN} <b>Расписание на {d}{day_str} обновлено!</b>\n\n"
            + format_schedule(sched_data)
        )
    for chat_id in get_all_subscribers():
        try:
            await _send_with_gif(application.bot, chat_id, text)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def alert_drive_error(application: Application, error_msg: str):
    """Шлёт алерт об ошибке Drive в группу где бот."""
    if not ALERT_CHAT_ID:
        return
    try:
        await application.bot.send_message(
            chat_id=int(ALERT_CHAT_ID),
            text=f"{WARN} <b>Ошибка доступа к Drive!</b>\n\n<code>{error_msg}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Не удалось отправить алерт: %s", e)


# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("today",       cmd_today))
    app.add_handler(CommandHandler("new",         cmd_new))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    start_scheduler(app, broadcast, broadcast_changed, alert_drive_error)

    logger.info("Бот запущен, группа: %s", GROUP_NAME)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
