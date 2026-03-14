import os
import time
import logging

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
    set_chat_group, get_chat_group,
)
from sheets import get_latest_file_id, get_today_file_id, parse_schedule, format_schedule
from scheduler import start_scheduler

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN         = os.environ["BOT_TOKEN"]
# Группа по умолчанию — используется если пользователь не задал свою
DEFAULT_GROUP = os.environ.get("GROUP_NAME", "")
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "")
GIF_PATH      = os.path.join(os.path.dirname(__file__), "emoji.mp4")

# ─── Премиум эмодзи ───────────────────────────────────────────────────────────
WAVE  = '<tg-emoji emoji-id="5319016550248751722">👋</tg-emoji>'
CAL   = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
BELL  = '<tg-emoji emoji-id="5458603043203327669">🔔</tg-emoji>'
WARN  = '<tg-emoji emoji-id="5447644880824181073">⚠️</tg-emoji>'
NEW   = '<tg-emoji emoji-id="5382357040008021292">🆕</tg-emoji>'
CHECK = '<tg-emoji emoji-id="5206607081334906820">✔️</tg-emoji>'
CROSS = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'
CLOCK = '<tg-emoji emoji-id="5386367538735104399">⌛</tg-emoji>'
PIN   = '<tg-emoji emoji-id="5397782960512444700">📌</tg-emoji>'

# ─── Cooldown (per chat + command) ────────────────────────────────────────────
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "30"))
_last_request: dict[tuple, float] = {}


def _check_cooldown(chat_id: int, command: str) -> int | None:
    key = (chat_id, command)
    last = _last_request.get(key)
    if last is None:
        return None
    remaining = int(COOLDOWN_SECONDS - (time.time() - last))
    return remaining if remaining > 0 else None


def _set_cooldown(chat_id: int, command: str):
    _last_request[(chat_id, command)] = time.time()


# ─── GIF ──────────────────────────────────────────────────────────────────────
_gif_file_id: str = ""


def _get_gif_id() -> str:
    global _gif_file_id
    if not _gif_file_id:
        _gif_file_id = get_gif_file_id() or os.environ.get("GIF_FILE_ID", "")
    return _gif_file_id


async def _send_with_gif(bot, chat_id: int, text: str):
    global _gif_file_id
    gif_id = _get_gif_id()
    try:
        if gif_id:
            await bot.send_animation(
                chat_id=chat_id, animation=gif_id,
                caption=text, parse_mode="HTML",
            )
        else:
            with open(GIF_PATH, "rb") as f:
                msg = await bot.send_animation(
                    chat_id=chat_id, animation=f,
                    caption=text, parse_mode="HTML",
                )
            _gif_file_id = msg.animation.file_id
            save_gif_file_id(_gif_file_id)
            logger.info("Гифка сохранена, file_id: %s", _gif_file_id)
    except Exception as e:
        logger.warning("Ошибка гифки, шлю только текст: %s", e)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")


# ─── Хелперы ──────────────────────────────────────────────────────────────────

def _resolve_group(chat_id: int) -> str | None:
    """Возвращает группу для чата: своя → дефолтная → None."""
    return get_chat_group(chat_id) or DEFAULT_GROUP or None


async def _fetch_and_reply(msg, bot, chat_id: int, file_id: str, group: str, prefix: str = ""):
    data = parse_schedule(file_id, group)
    if not data:
        await msg.edit_text(
            f'{CROSS} Группа <b>{group}</b> не найдена в файле.\n'
            f'Проверь название командой /setgroup',
            parse_mode="HTML",
        )
        return
    await msg.delete()
    text = (prefix + format_schedule(data)) if prefix else format_schedule(data)
    await _send_with_gif(bot, chat_id, text)


# ─── Команды ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group = _resolve_group(chat_id)

    if not group:
        await update.message.reply_text(
            f"{WAVE} Привет! Я бот расписания.\n\n"
            f"{PIN} <b>Для начала выбери свою группу:</b>\n"
            f"/setgroup &lt;название&gt;\n\n"
            f"Название группы пишется точно так же как в таблице расписания — "
            f"с дефисами, пробелами и цифрами. Например:\n"
            f"<code>/setgroup 2-24 ОРП-1</code>\n\n"
            f"❗ Регистр и пробелы важны — скопируй название прямо из таблицы "
            f"если не уверен.",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        f"{WAVE} Привет! Я бот расписания.\n\n"
        f"Текущая группа: <b>{group}</b>\n\n"
        f"{CAL} /today — расписание на сегодня\n"
        f"{NEW} /new — последнее новое расписание\n"
        f"🔧 /setgroup &lt;название&gt; — сменить группу\n"
        f"{BELL} /subscribe — подписаться на авторассылку\n"
        f"{CROSS} /unsubscribe — отписаться",
        parse_mode="HTML",
    )


async def cmd_setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group = " ".join(context.args).strip() if context.args else ""

    if not group:
        current = _resolve_group(chat_id)
        current_str = f"Сейчас: <b>{current}</b>" if current else "Группа не задана"
        await update.message.reply_text(
            f"Использование: /setgroup &lt;название группы&gt;\n"
            f"Например: /setgroup 2-24 ОРП-1\n\n"
            f"{current_str}",
            parse_mode="HTML",
        )
        return

    set_chat_group(chat_id, group)

    # Проверяем что группа реально есть в последнем файле
    try:
        from sheets import get_latest_file_id, parse_schedule
        file_id = get_latest_file_id()
        if file_id:
            test = parse_schedule(file_id, group)
            if not test:
                await update.message.reply_text(
                    f"{WARN} Группа <b>{group}</b> не найдена в последнем файле расписания.\n\n"
                    f"Возможные причины:\n"
                    f"— опечатка в названии\n"
                    f"— группа пишется иначе чем в таблице\n\n"
                    f"Проверь название прямо в таблице и попробуй снова.",
                    parse_mode="HTML",
                )
                return
    except Exception:
        pass  # Если проверка упала — не блокируем, просто сохраняем

    await update.message.reply_text(
        f"{CHECK} Группа установлена: <b>{group}</b>\n"
        f"Теперь /today и /new будут показывать расписание этой группы.",
        parse_mode="HTML",
    )


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group = _resolve_group(chat_id)
    if not group:
        await update.message.reply_text(
            f"{CROSS} Группа не задана. Используй /setgroup &lt;название&gt;",
            parse_mode="HTML",
        )
        return

    wait = _check_cooldown(chat_id, "today")
    if wait:
        await update.message.reply_text(
            f"{CLOCK} Подожди ещё <b>{wait} сек.</b>", parse_mode="HTML"
        )
        return

    msg = await update.message.reply_text(f"{CLOCK} Загружаю расписание...", parse_mode="HTML")
    try:
        file_id = get_today_file_id()
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов в папке Drive не найдено.", parse_mode="HTML")
            return
        _set_cooldown(chat_id, "today")
        await _fetch_and_reply(msg, context.bot, chat_id, file_id, group)
    except Exception as e:
        logger.exception("Ошибка /today")
        await msg.edit_text(f"{WARN} Ошибка: {e}", parse_mode="HTML")


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group = _resolve_group(chat_id)
    if not group:
        await update.message.reply_text(
            f"{CROSS} Группа не задана. Используй /setgroup &lt;название&gt;",
            parse_mode="HTML",
        )
        return

    wait = _check_cooldown(chat_id, "new")
    if wait:
        await update.message.reply_text(
            f"{CLOCK} Подожди ещё <b>{wait} сек.</b>", parse_mode="HTML"
        )
        return

    msg = await update.message.reply_text(f"{CLOCK} Загружаю расписание...", parse_mode="HTML")
    try:
        file_id = get_latest_file_id()
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов в папке Drive не найдено.", parse_mode="HTML")
            return
        _set_cooldown(chat_id, "new")
        await _fetch_and_reply(msg, context.bot, chat_id, file_id, group)
    except Exception as e:
        logger.exception("Ошибка /new")
        await msg.edit_text(f"{WARN} Ошибка: {e}", parse_mode="HTML")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group = _resolve_group(chat_id)
    if not group:
        await update.message.reply_text(
            f"{CROSS} Сначала задай группу: /setgroup &lt;название&gt;",
            parse_mode="HTML",
        )
        return
    add_subscriber(chat_id)
    await update.message.reply_text(
        f"{CHECK} Подписка оформлена!\n"
        f"Группа: <b>{group}</b>\n"
        f"Когда появится новое расписание — пришлю автоматически.",
        parse_mode="HTML",
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remove_subscriber(update.effective_chat.id)
    await update.message.reply_text(f"{CROSS} Вы отписаны.", parse_mode="HTML")


# ─── Авторассылка ─────────────────────────────────────────────────────────────

async def broadcast(application: Application, file_id: str):
    """Рассылает новое расписание каждому подписчику его группы."""
    subscribers = get_all_subscribers()
    # Кэш: group_name -> parsed data чтобы не парсить один файл 100 раз
    cache: dict[str, dict | None] = {}

    for sub in subscribers:
        chat_id   = sub["chat_id"]
        group     = sub["group_name"] or DEFAULT_GROUP
        if not group:
            continue

        if group not in cache:
            try:
                cache[group] = parse_schedule(file_id, group)
            except Exception as e:
                logger.warning("Ошибка парсинга группы %s: %s", group, e)
                cache[group] = None

        data = cache[group]
        if not data:
            continue

        text = f"{NEW} <b>Новое расписание!</b>\n\n" + format_schedule(data)
        try:
            await _send_with_gif(application.bot, chat_id, text)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def broadcast_changed(application: Application, file_id: str, diffs: dict[str, str]):
    """
    Рассылает изменения расписания.
    diffs: {group_name: diff_text}
    """
    subscribers = get_all_subscribers()
    cache: dict[str, dict | None] = {}

    for sub in subscribers:
        chat_id = sub["chat_id"]
        group   = sub["group_name"] or DEFAULT_GROUP
        if not group:
            continue

        if group not in cache:
            try:
                cache[group] = parse_schedule(file_id, group)
            except Exception:
                cache[group] = None

        data = cache[group]
        if not data:
            continue

        diff_text = diffs.get(group, "")
        d       = data.get("date", "")
        day     = data.get("day", "")
        day_str = f", {day}" if day else ""

        if diff_text:
            text = (
                f"{WARN} <b>Расписание на {d}{day_str} изменилось!</b>\n\n"
                + diff_text
                + "\n\n📋 Актуальное расписание:\n\n"
                + format_schedule(data)
            )
        else:
            text = (
                f"{WARN} <b>Расписание на {d}{day_str} обновлено!</b>\n\n"
                + format_schedule(data)
            )

        try:
            await _send_with_gif(application.bot, chat_id, text)
        except Exception as e:
            logger.warning("Не удалось отправить chat_id=%s: %s", chat_id, e)


async def alert_drive_error(application: Application, error_msg: str):
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
    app.add_handler(CommandHandler("setgroup",    cmd_setgroup))
    app.add_handler(CommandHandler("today",       cmd_today))
    app.add_handler(CommandHandler("new",         cmd_new))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))

    start_scheduler(app, broadcast, broadcast_changed, alert_drive_error)

    logger.info("Бот запущен, дефолтная группа: %s", DEFAULT_GROUP)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
