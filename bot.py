import os
import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from db import init_db, add_subscriber, remove_subscriber, get_all_subscribers
from sheets import get_latest_file_id, parse_schedule, format_schedule
from scheduler import start_scheduler

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN      = os.environ["BOT_TOKEN"]
GROUP_NAME = os.environ.get("GROUP_NAME", "")
# После первой отправки гифки Railway сохранит сюда file_id автоматически.
# Можно также задать вручную если уже знаешь file_id.
GIF_FILE_ID = os.environ.get("GIF_FILE_ID", "")
GIF_PATH    = os.path.join(os.path.dirname(__file__), "emoji.mp4")

# ─── Премиум эмодзи ───────────────────────────────────────────────────────────
WAVE  = '<tg-emoji emoji-id="5319016550248751722">👋</tg-emoji>'
CAL   = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
BELL  = '<tg-emoji emoji-id="5458603043203327669">🔔</tg-emoji>'
WARN  = '<tg-emoji emoji-id="5447644880824181073">⚠️</tg-emoji>'
NEW   = '<tg-emoji emoji-id="5382357040008021292">🆕</tg-emoji>'
CHECK = '<tg-emoji emoji-id="5206607081334906820">✔️</tg-emoji>'
CROSS = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'
CLOCK = '<tg-emoji emoji-id="5386367538735104399">⌛</tg-emoji>'


# ─── Хелпер отправки гифки ────────────────────────────────────────────────────

# Кэш file_id чтобы не перечитывать файл каждый раз
_gif_file_id: str = GIF_FILE_ID


async def _send_gif(bot, chat_id: int):
    """Отправляет гифку. При первой отправке загружает файл и кэширует file_id."""
    global _gif_file_id
    try:
        if _gif_file_id:
            await bot.send_animation(chat_id=chat_id, animation=_gif_file_id)
        else:
            with open(GIF_PATH, "rb") as f:
                msg = await bot.send_animation(chat_id=chat_id, animation=f)
            _gif_file_id = msg.animation.file_id
            logger.info("Гифка загружена, file_id: %s", _gif_file_id)
            logger.info("Добавь в Railway переменную: GIF_FILE_ID=%s", _gif_file_id)
    except Exception as e:
        logger.warning("Не удалось отправить гифку: %s", e)


# ─── Команды ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"{WAVE} Привет! Я бот расписания.\n\n"
        f"{CAL} /today — расписание из последнего файла\n"
        f"{BELL} /subscribe — подписаться на авторассылку\n"
        f"{CROSS} /unsubscribe — отписаться",
        parse_mode="HTML",
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text(
        f"{CLOCK} Загружаю расписание...",
        parse_mode="HTML",
    )
    try:
        file_id = get_latest_file_id()
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов в папке Drive не найдено.", parse_mode="HTML")
            return
        data = parse_schedule(file_id, GROUP_NAME)
        if not data:
            await msg.edit_text(
                f'{CROSS} Группа <b>{GROUP_NAME}</b> не найдена в файле.\n'
                'Проверь переменную GROUP_NAME.',
                parse_mode="HTML",
            )
            return
        await msg.edit_text(format_schedule(data), parse_mode="HTML")
        await _send_gif(context.bot, update.effective_chat.id)
    except Exception as e:
        logger.exception("Ошибка /today")
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
    await update.message.reply_text(
        f"{CROSS} Вы отписаны.",
        parse_mode="HTML",
    )


# ─── Авторассылка ─────────────────────────────────────────────────────────────

async def broadcast(application: Application, sched_data: dict):
    """Рассылает новое расписание всем подписчикам."""
    text = f"{NEW} <b>Новое расписание!</b>\n\n" + format_schedule(sched_data)
    for chat_id in get_all_subscribers():
        try:
            await application.bot.send_message(chat_id, text, parse_mode="HTML")
            await _send_gif(application.bot, chat_id)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def broadcast_changed(application: Application, sched_data: dict, diff_text: str):
    """Рассылает уведомление об изменении расписания."""
    date = sched_data.get("date", "")
    day  = sched_data.get("day", "")
    day_str = f", {day}" if day else ""

    if diff_text:
        text = (
            f"{WARN} <b>Расписание на {date}{day_str} изменилось!</b>\n\n"
            + diff_text
            + "\n\n📋 Актуальное расписание:\n\n"
            + format_schedule(sched_data)
        )
    else:
        text = (
            f"{WARN} <b>Расписание на {date}{day_str} обновлено!</b>\n\n"
            + format_schedule(sched_data)
        )

    for chat_id in get_all_subscribers():
        try:
            await application.bot.send_message(chat_id, text, parse_mode="HTML")
            await _send_gif(application.bot, chat_id)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("today",       cmd_today))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    start_scheduler(app, broadcast, broadcast_changed)

    logger.info("Бот запущен, группа: %s", GROUP_NAME)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
