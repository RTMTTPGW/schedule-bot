import os
import logging

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
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


# ─── Клавиатура ───────────────────────────────────────────────────────────────

KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📅 Расписание")],
        [KeyboardButton("🔔 Подписаться"), KeyboardButton("🔕 Отписаться")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)


# ─── Логика получения расписания ──────────────────────────────────────────────

async def _fetch_and_send(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        file_id = get_latest_file_id()
        if not file_id:
            await context.bot.send_message(chat_id, "❌ Файлов в папке Drive не найдено.")
            return

        data = parse_schedule(file_id, GROUP_NAME)
        if not data:
            await context.bot.send_message(
                chat_id,
                f"❌ Группа <b>{GROUP_NAME}</b> не найдена в файле.\n"
                "Проверь переменную GROUP_NAME.",
                parse_mode="HTML",
            )
            return

        await context.bot.send_message(
            chat_id,
            format_schedule(data),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Ошибка при получении расписания")
        await context.bot.send_message(chat_id, f"⚠️ Ошибка: {e}")


# ─── Команды ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я бот расписания.\n\n"
        "📅 /today — расписание из последнего файла\n"
        "🔔 /subscribe — подписаться на авторассылку\n"
        "🔕 /unsubscribe — отписаться",
        reply_markup=KEYBOARD,
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Загружаю расписание...")
    try:
        file_id = get_latest_file_id()
        if not file_id:
            await msg.edit_text("❌ Файлов в папке Drive не найдено.")
            return
        data = parse_schedule(file_id, GROUP_NAME)
        if not data:
            await msg.edit_text(
                f"❌ Группа <b>{GROUP_NAME}</b> не найдена в файле.\n"
                "Проверь переменную GROUP_NAME.",
                parse_mode="HTML",
            )
            return
        await msg.edit_text(format_schedule(data), parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /today")
        await msg.edit_text(f"⚠️ Ошибка: {e}")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_subscriber(update.effective_chat.id)
    await update.message.reply_text(
        "✅ Подписка оформлена!\n"
        "Когда появится новый файл расписания на будущий день — пришлю автоматически.",
        reply_markup=KEYBOARD,
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remove_subscriber(update.effective_chat.id)
    await update.message.reply_text("🔕 Вы отписаны.", reply_markup=KEYBOARD)


# ─── Кнопки ───────────────────────────────────────────────────────────────────

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "📅 Расписание":
        await cmd_today(update, context)
    elif text == "🔔 Подписаться":
        await cmd_subscribe(update, context)
    elif text == "🔕 Отписаться":
        await cmd_unsubscribe(update, context)


# ─── Авторассылка (вызывается из scheduler) ───────────────────────────────────

async def broadcast(application: Application, sched_data: dict):
    """Рассылает новое расписание всем подписчикам."""
    text = "🆕 <b>Новое расписание!</b>\n\n" + format_schedule(sched_data)
    for chat_id in get_all_subscribers():
        try:
            await application.bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def broadcast_changed(application: Application, sched_data: dict, diff_text: str):
    """Рассылает уведомление об изменении расписания."""
    date  = sched_data.get("date", "")
    day   = sched_data.get("day", "")

    if diff_text:
        text = (
            f"⚠️ <b>Расписание на {date}"
            + (f", {day}" if day else "")
            + " изменилось!</b>\n\n"
            + diff_text
            + "\n\n📋 Актуальное расписание:\n\n"
            + format_schedule(sched_data)
        )
    else:
        text = (
            f"⚠️ <b>Расписание на {date}"
            + (f", {day}" if day else "")
            + " обновлено!</b>\n\n"
            + format_schedule(sched_data)
        )

    for chat_id in get_all_subscribers():
        try:
            await application.bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("today",       cmd_today))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons)
    )

    start_scheduler(app, broadcast, broadcast_changed)

    logger.info("Бот запущен, группа: %s", GROUP_NAME)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
