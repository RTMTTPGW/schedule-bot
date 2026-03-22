import os
import time
import logging
import asyncio
import httpx

_http_client: httpx.AsyncClient | None = None

def _get_http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=10)
    return _http_client


from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, ReplyKeyboardMarkup, KeyboardButton, MenuButtonWebApp
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler,
)

from db import (
    init_db,
    add_subscriber, remove_subscriber, get_all_subscribers, is_subscriber,
    get_gif_file_id, save_gif_file_id,
    set_chat_group, get_chat_group,
    set_chat_corp, get_chat_corp,
    set_group_mode, is_group_mode,
    get_scheduler_stats,
    kv_get, kv_set,
)
from sheets import get_latest_file_id, get_today_file_id, parse_schedule, format_schedule
from config import CORPS, CORPS_BY_ID
from scheduler import start_scheduler

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

from html import escape as _esc
TOKEN         = os.environ["BOT_TOKEN"]
BASE_URL      = f"https://api.telegram.org/bot{TOKEN}"

def _mask_token(text: str) -> str:
    """Маскирует токен бота в строках для безопасного логирования."""
    return text.replace(TOKEN, "***") if TOKEN in text else text
ADMIN_ID      = int(os.environ.get("ADMIN_ID", "0"))
DEFAULT_GROUP = os.environ.get("GROUP_NAME", "")
DEFAULT_CORP  = os.environ.get("CORP_ID", "corp3")
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "")
GIF_PATH      = os.path.join(os.path.dirname(__file__), "emoji.mp4")

# ConversationHandler states
WAITING_GROUP  = 1
SELECT_COURSE  = 2
SELECT_GROUP   = 3

# ─── Премиум эмодзи ───────────────────────────────────────────────────────────
WAVE   = '<tg-emoji emoji-id="5319016550248751722">👋</tg-emoji>'
CAL    = '<tg-emoji emoji-id="5274055917766202507">🗓</tg-emoji>'
BELL   = '<tg-emoji emoji-id="5458603043203327669">🔔</tg-emoji>'
WARN   = '<tg-emoji emoji-id="5447644880824181073">⚠️</tg-emoji>'
NEW    = '<tg-emoji emoji-id="5382357040008021292">🆕</tg-emoji>'
CHECK  = '<tg-emoji emoji-id="5206607081334906820">✔️</tg-emoji>'
CROSS  = '<tg-emoji emoji-id="5210952531676504517">❌</tg-emoji>'
CLOCK  = '<tg-emoji emoji-id="5386367538735104399">⌛</tg-emoji>'
PIN    = '<tg-emoji emoji-id="5397782960512444700">📌</tg-emoji>'
WRENCH = '<tg-emoji emoji-id="5339081812821957844">⚙️</tg-emoji>'
BACK   = '◀️'

# ─── Cooldown (SQLite-backed, survives restarts) ──────────────────────────────
COOLDOWN_SECONDS = int(os.environ.get("COOLDOWN_SECONDS", "30"))

def _check_cooldown(chat_id: int, command: str) -> int | None:
    val = kv_get(f"cd:{chat_id}:{command}")
    if val is None:
        return None
    try:
        last = float(val)
    except ValueError:
        return None
    remaining = int(COOLDOWN_SECONDS - (time.time() - last))
    return remaining if remaining > 0 else None

def _set_cooldown(chat_id: int, command: str):
    kv_set(f"cd:{chat_id}:{command}", str(time.time()))

# ─── GIF ──────────────────────────────────────────────────────────────────────
_gif_file_id: str = ""

def _get_gif_id() -> str:
    global _gif_file_id
    if not _gif_file_id:
        _gif_file_id = get_gif_file_id() or os.environ.get("GIF_FILE_ID", "")
    return _gif_file_id

async def _send_with_gif(bot, chat_id: int, text: str, reply_markup=None):
    global _gif_file_id
    gif_id = _get_gif_id()
    try:
        if gif_id:
            await bot.send_animation(
                chat_id=chat_id, animation=gif_id,
                caption=text, parse_mode="HTML",
                reply_markup=reply_markup,
            )
        else:
            with open(GIF_PATH, "rb") as f:
                msg = await bot.send_animation(
                    chat_id=chat_id, animation=f,
                    caption=text, parse_mode="HTML",
                    reply_markup=reply_markup,
                )
            _gif_file_id = msg.animation.file_id
            save_gif_file_id(_gif_file_id)
    except Exception as e:
        logger.warning("Ошибка гифки: %s", e.args[0] if e.args else type(e).__name__)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML",
                               reply_markup=reply_markup)

# ─── Хелперы ──────────────────────────────────────────────────────────────────

def _resolve_corp(chat_id: int) -> str:
    return get_chat_corp(chat_id) or DEFAULT_CORP

def _resolve_group(chat_id: int) -> str | None:
    """Возвращает группу пользователя. DEFAULT_GROUP не используется — каждый выбирает сам."""
    return get_chat_group(chat_id) or None

BACK_KB       = InlineKeyboardMarkup([[InlineKeyboardButton(f"{BACK} Закрыть", callback_data="del:msg")]])
DELETE_KB     = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Удалить", callback_data="del:msg")]])

# ─── ReplyKeyboard — постоянные кнопки в чатбаре ─────────────────────────────
BTN_TODAY  = "📅 На сегодня"
BTN_NEW    = "🆕 Последнее новое"
BTN_CORP   = "🏢 Сменить корпус"
BTN_GROUP  = "👥 Сменить группу"
BTN_SUB    = "🔔 Подписаться"
BTN_UNSUB  = "🔕 Отписаться"

def _reply_kb(subscribed: bool) -> ReplyKeyboardMarkup:
    sub_btn = BTN_UNSUB if subscribed else BTN_SUB
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BTN_TODAY), KeyboardButton(BTN_NEW)],
         [KeyboardButton(BTN_CORP),  KeyboardButton(BTN_GROUP)],
         [KeyboardButton(sub_btn)]],
        resize_keyboard=True,
        is_persistent=True,
    )

async def _fetch_and_send(bot, chat_id: int, file_id: str, group: str, corp_id: str):
    kb = DELETE_KB if is_group_mode(chat_id) else BACK_KB
    data = parse_schedule(file_id, group, corp_id)
    if not data:
        await bot.send_message(
            chat_id=chat_id,
            text=f'{CROSS} Группа <b>{_esc(group)}</b> не найдена в файле.',
            parse_mode="HTML",
            reply_markup=kb,
        )
        return
    await _send_with_gif(bot, chat_id, format_schedule(data), reply_markup=kb)

# ─── Главное меню ─────────────────────────────────────────────────────────────

def _menu_text(chat_id: int) -> str:
    corp_id    = _resolve_corp(chat_id)
    group      = _resolve_group(chat_id)
    corp       = CORPS_BY_ID.get(corp_id, {})
    subscribed = is_subscriber(chat_id)
    sub_status = f"{CHECK} Авторассылка включена" if subscribed else f"{CROSS} Авторассылка отключена"

    if not group:
        return (
            f"{WAVE} <b>Бот расписания ВПТ</b>\n\n"
            f"{PIN} Группа не выбрана\n\n"
            f"Выбери корпус и укажи группу чтобы начать."
        )
    return (
        f"{WAVE} <b>Бот расписания ВПТ</b>\n\n"
        f"🏢 Корпус: <b>{corp.get('name', corp_id)}</b>\n"
        f"👥 Группа: <b>{_esc(group)}</b>\n"
        f"{sub_status}"
    )

def _menu_keyboard_raw(chat_id: int) -> dict:
    subscribed = is_subscriber(chat_id)
    kb = [
        [{"text": "🌐 Открыть сайт расписания", "web_app": {"url": "https://rtmttpgw.github.io/vpt-schedule/"}}],
        [{"text": "📅 Расписание на сегодня", "callback_data": "m:today"}],
        [{"text": "🆕 Последнее новое", "callback_data": "m:new"}],
        [{"text": "🏢 Сменить корпус", "callback_data": "m:setcorp"}],
        [{"text": "👥 Сменить группу", "callback_data": "m:setgroup"}],
    ]
    if subscribed:
        kb.append([{"text": "🔕 Отписаться", "callback_data": "m:unsubscribe", "style": "danger"}])
    else:
        kb.append([{"text": "🔔 Подписаться", "callback_data": "m:subscribe", "style": "success"}])
    return {"inline_keyboard": kb}

def _menu_keyboard_ptb(chat_id: int) -> InlineKeyboardMarkup:
    subscribed = is_subscriber(chat_id)
    kb = [
        [InlineKeyboardButton("🌐 Открыть сайт расписания", web_app=WebAppInfo(url="https://rtmttpgw.github.io/vpt-schedule/"))],
        [InlineKeyboardButton("📅 Расписание на сегодня", callback_data="m:today")],
        [InlineKeyboardButton("🆕 Последнее новое", callback_data="m:new")],
        [InlineKeyboardButton("🏢 Сменить корпус", callback_data="m:setcorp")],
        [InlineKeyboardButton("👥 Сменить группу", callback_data="m:setgroup")],
    ]
    if subscribed:
        kb.append([InlineKeyboardButton("🔕 Отписаться", callback_data="m:unsubscribe")])
    else:
        kb.append([InlineKeyboardButton("🔔 Подписаться", callback_data="m:subscribe")])
    return InlineKeyboardMarkup(kb)

async def _send_msg_with_color_keyboard(bot, chat_id: int, text: str, subscribed: bool):
    """Отправляет сообщение с ReplyKeyboard."""
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=_reply_kb(subscribed),
    )


async def _send_menu(bot, chat_id: int):
    """Отправляет статус + ReplyKeyboard. В групповом режиме ничего не делает."""
    if is_group_mode(chat_id):
        return
    text = _menu_text(chat_id)
    subscribed = is_subscriber(chat_id)
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        reply_markup=_reply_kb(subscribed),
    )
async def _replace_with_menu(query, chat_id: int):
    """Удаляет текущее сообщение и отправляет меню заново."""
    try:
        await query.message.delete()
    except Exception:
        pass
    await _send_menu(query.message.get_bot(), chat_id)

# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id

    if is_group_mode(chat_id):
        await update.message.reply_text(
            f"{CHECK} <b>Бот активен в групповом режиме.</b>\n"
            "Авторассылка работает автоматически.\n\n"
            "Доступные команды:\n"
            f"{WRENCH} /setcorp — выбрать корпус\n"
            f"{WRENCH} /setgroup — указать группу\n"
            f"{BELL} /subscribe — подписаться\n"
            f"{CROSS} /unsubscribe — отписаться\n"
            f"{CAL} /today — расписание на сегодня\n"
            f"{NEW} /new — последнее новое расписание\n"
            f"{WRENCH} /groupmode — выключить групповой режим",
            parse_mode="HTML",
        )
        return

    corp_id = get_chat_corp(chat_id)
    group   = get_chat_group(chat_id)
    corp    = CORPS_BY_ID.get(corp_id, {}) if corp_id else {}
    subscribed = is_subscriber(chat_id)

    # Приветствие — всегда
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"{WAVE} <b>Привет! Я бот расписания ВПТ.</b>\n\n"
            f"{PIN} Показываю расписание для любой группы техникума. "
            "Работает для всех 4 корпусов.\n\n"
            "<b>Как начать:</b>\n"
            "1. Нажми \"🏢 Сменить корпус\" и выбери свой\n"
            "2. Нажми \"👥 Сменить группу\" — выбери курс и группу\n"
            "3. Нажми \"📅 Расписание на сегодня\" — готово!\n\n"
            f"<b>Авторассылка:</b>\n"
            f"{BELL} Подпишись и бот сам пришлёт расписание когда появится новый файл.\n\n"
            "<b>Для группового чата:</b>\n"
            "Добавь бота в чат, настрой под свою группу и напиши /groupmode — "
            "бот будет без кнопок и не будет мешать, но автоматически пришлёт расписание когда оно появится.\n"
            "Команда доступна только администраторам чата.\n\n"
            "⚠️ <b>2 корпус не поддерживается</b> — расписание там ведётся в нестандартном формате."
        ),
        parse_mode="HTML",
    )

    # В групповых чатах — только текстовое сообщение без кнопок
    from telegram import Chat
    is_private = update.effective_chat.type == Chat.PRIVATE

    if not is_private:
        if corp_id and group:
            sub_status = f"{CHECK} Авторассылка включена" if subscribed else f"{CROSS} Авторассылка отключена"
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🏢 Корпус: <b>{_esc(corp.get('name', corp_id))}</b>\n"
                    f"👥 Группа: <b>{_esc(group)}</b>\n"
                    f"{sub_status}\n\n"
                    f"Используй /groupmode чтобы включить авторассылку без кнопок."
                ),
                parse_mode="HTML",
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"{PIN} Для настройки бота в группе:\n"
                    f"1. /setcorp — выбери корпус\n"
                    f"2. /setgroup — укажи группу\n"
                    f"3. /subscribe — подпишись на авторассылку\n"
                    f"4. /groupmode — включи тихий режим (только для админов)"
                ),
                parse_mode="HTML",
            )
        return

    # Устанавливаем Menu Button (открывает сайт)
    try:
        await context.bot.set_chat_menu_button(
            chat_id=chat_id,
            menu_button=MenuButtonWebApp(
                text="🌐 Расписание",
                web_app=WebAppInfo(url="https://rtmttpgw.github.io/vpt-schedule/"),
            ),
        )
    except Exception:
        pass

    # В личке — статус + ReplyKeyboard
    if corp_id and group:
        sub_status = f"{CHECK} Авторассылка включена" if subscribed else f"{CROSS} Авторассылка отключена"
        menu_text = (
            f"🏢 Корпус: <b>{_esc(corp.get('name', corp_id))}</b>\n"
            f"👥 Группа: <b>{_esc(group)}</b>\n"
            f"{sub_status}"
        )
    else:
        menu_text = "⬇️ Выбери корпус и группу чтобы начать:"

    await _send_msg_with_color_keyboard(context.bot, chat_id, menu_text, subscribed)

# ─── Обработчик кнопок главного меню ─────────────────────────────────────────

async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    action  = query.data.split(":")[1]

    # В групповом режиме кнопки меню не работают
    if is_group_mode(chat_id) and action not in ("back",):
        return

    if action == "today":
        group   = _resolve_group(chat_id)
        corp_id = _resolve_corp(chat_id)
        if not corp_id or not group:
            await query.answer()
            # Открываем выбор корпуса если не выбран
            try:
                await query.message.delete()
            except Exception:
                pass
            kb = [[InlineKeyboardButton(c["name"], callback_data=f"corp:{c['id']}")]
                  for c in CORPS if not c.get("unsupported")]
            kb.append([InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"{PIN} Сначала выбери корпус и группу:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            return
        wait = _check_cooldown(chat_id, "today")
        if wait:
            await query.answer(f"Подожди ещё {wait} сек.", show_alert=True)
            return
        try:
            file_id = get_today_file_id(corp_id)
            if not file_id:
                await query.answer("Файлов не найдено.", show_alert=True)
                return
            _set_cooldown(chat_id, "today")
            await query.message.delete()
            await _fetch_and_send(context.bot, chat_id, file_id, group, corp_id)
        except Exception:
            logger.exception("Ошибка today")
            await query.answer("Произошла ошибка. Попробуй позже.", show_alert=True)

    elif action == "new":
        group   = _resolve_group(chat_id)
        corp_id = _resolve_corp(chat_id)
        if not corp_id or not group:
            await query.answer()
            try:
                await query.message.delete()
            except Exception:
                pass
            kb = [[InlineKeyboardButton(c["name"], callback_data=f"corp:{c['id']}")]
                  for c in CORPS if not c.get("unsupported")]
            kb.append([InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")])
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"{PIN} Сначала выбери корпус и группу:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            return
        wait = _check_cooldown(chat_id, "new")
        if wait:
            await query.answer(f"Подожди ещё {wait} сек.", show_alert=True)
            return
        try:
            file_id = get_latest_file_id(corp_id)
            if not file_id:
                await query.answer("Файлов не найдено.", show_alert=True)
                return
            _set_cooldown(chat_id, "new")
            await query.message.delete()
            await _fetch_and_send(context.bot, chat_id, file_id, group, corp_id)
        except Exception:
            logger.exception("Ошибка new")
            await query.answer("Произошла ошибка. Попробуй позже.", show_alert=True)

    elif action == "setcorp":
        # Удаляем меню, показываем экран выбора корпуса
        try:
            await query.message.delete()
        except Exception:
            pass
        kb = [[InlineKeyboardButton(c["name"], callback_data=f"corp:{c['id']}")]
              for c in CORPS if not c.get("unsupported")]
        kb.append([InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")])
        await context.bot.send_message(
            chat_id=chat_id,
            text="🏢 Выбери корпус:",
            reply_markup=InlineKeyboardMarkup(kb),
        )

    elif action == "setgroup":
        # Удаляем меню, показываем выбор курса
        try:
            await query.message.delete()
        except Exception:
            pass
        kb = [
            [InlineKeyboardButton("1 курс", callback_data="course:1"),
             InlineKeyboardButton("2 курс", callback_data="course:2")],
            [InlineKeyboardButton("3 курс", callback_data="course:3"),
             InlineKeyboardButton("4 курс", callback_data="course:4")],
            [InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")],
        ]
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"{WRENCH} <b>Шаг 1:</b> Выбери свой курс",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        context.user_data["course_select_msg_id"] = msg.message_id
        return SELECT_COURSE

    elif action == "subscribe":
        group = _resolve_group(chat_id)
        if not group:
            await query.answer("Сначала выбери группу!", show_alert=True)
            return
        add_subscriber(chat_id)
        await query.answer("Подписка оформлена!")
        try:
            await query.message.delete()
        except Exception:
            pass
        await _send_menu(context.bot, chat_id)

    elif action == "unsubscribe":
        remove_subscriber(chat_id)
        await query.answer("Отписка оформлена.")
        try:
            await query.message.delete()
        except Exception:
            pass
        await _send_menu(context.bot, chat_id)

    elif action == "back":
        try:
            await query.message.delete()
        except Exception:
            pass
        if not is_group_mode(chat_id):
            await _send_menu(context.bot, chat_id)

    return ConversationHandler.END

# ─── Выбор корпуса ────────────────────────────────────────────────────────────

async def cb_corp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    corp_id = query.data.split(":")[1]
    corp    = CORPS_BY_ID.get(corp_id)
    if not corp:
        await query.answer("Неизвестный корпус.", show_alert=True)
        return
    set_chat_corp(chat_id, corp_id)
    try:
        await query.message.delete()
    except Exception:
        pass
    # Уведомление о смене корпуса
    notice = await context.bot.send_message(
        chat_id=chat_id,
        text=f"{CHECK} Корпус установлен: <b>{corp['name']}</b>",
        parse_mode="HTML",
    )
    import asyncio as _ai
    await _ai.sleep(1.5)
    try:
        await notice.delete()
    except Exception:
        pass
    await _send_menu(context.bot, chat_id)

# ─── Выбор курса и группы из списка ──────────────────────────────────────────

async def cb_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал курс — загружаем группы из Drive и показываем список."""
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    course  = int(query.data.split(":")[1])
    corp_id = _resolve_corp(chat_id)

    context.user_data["selected_course"]      = course
    context.user_data["course_select_msg_id"] = query.message.message_id

    # Анимация загрузки — три точки
    async def animate_loading():
        dots = [".", "..", "..."]
        for i in range(9):  # 3 цикла × 3 варианта = ~9 секунд
            try:
                await query.edit_message_text(
                    f"{CLOCK} Загружаю список групп {course} курса{dots[i % 3]}",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1)
            except Exception:
                break

    # Запускаем анимацию и загрузку параллельно
    async def load_groups():
        try:
            from api import _extract_groups_from_file
            from drive import export_as_xlsx
            file_id = get_latest_file_id(corp_id)
            if not file_id:
                raise Exception("Файлов не найдено")
            corp_cfg = CORPS_BY_ID.get(corp_id, {})
            xlsx = export_as_xlsx(file_id)
            return _extract_groups_from_file(xlsx, corp_cfg.get("table_format", "type_a"))
        except Exception as e:
            logger.warning("Ошибка загрузки групп: %s", e)
            return []

    anim_task = asyncio.create_task(animate_loading())
    try:
        all_groups = await asyncio.wait_for(load_groups(), timeout=15)
        anim_task.cancel()
    except asyncio.TimeoutError:
        anim_task.cancel()
        kb = [
            [InlineKeyboardButton(f"{BACK} Назад", callback_data="m:setgroup")],
        ]
        try:
            await query.edit_message_text(
                f"{WARN} Не удалось загрузить список групп.\n\n"
                "Сервер не ответил за 15 секунд. Попробуй ещё раз.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(kb),
            )
        except Exception:
            pass
        return SELECT_COURSE
    except Exception as e:
        anim_task.cancel()
        logger.warning("Ошибка загрузки групп: %s", e)
        all_groups = []

    # Фильтруем по курсу — группы текущего года имеют паттерн N-YY
    # Курс определяется по году поступления: 1 курс = 25, 2 = 24, 3 = 23, 4 = 22
    from datetime import date
    current_year = date.today().year % 100
    # Корректируем если после сентября — новый учебный год
    if date.today().month >= 9:
        entry_year = current_year - course + 1
    else:
        entry_year = current_year - course

    filtered = []
    for g in all_groups:
        # Ищем паттерн X-YY в начале названия группы
        import re
        m = re.match(r'^\d+-(\d{2})', g.strip())
        if m and int(m.group(1)) == entry_year % 100:
            filtered.append(g)

    if not filtered:
        # Если фильтр не дал результатов — показываем все группы
        filtered = all_groups

    if not filtered:
        kb = [[InlineKeyboardButton(f"{BACK} Назад", callback_data="m:setgroup")]]
        await query.edit_message_text(
            f"{WARN} Группы не найдены. Попробуй ввести вручную.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return SELECT_COURSE

    # Показываем кнопки с группами
    kb = []
    for g in filtered:
        # Берём только короткое название (до первого пробела после кода)
        short = re.match(r'^([\d\-\s]+[А-ЯЁA-Z]+[\-\d]+)', g.strip())
        label = short.group(1).strip() if short else g.strip()[:20]
        kb.append([InlineKeyboardButton(label, callback_data=f"grp:{g[:50]}")])

    kb.append([InlineKeyboardButton(f"{BACK} Назад", callback_data="m:setgroup")])

    await query.edit_message_text(
        f"{WRENCH} <b>Шаг 2:</b> Выбери свою группу ({course} курс)",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return SELECT_GROUP


async def cb_group_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал группу из списка."""
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    group   = query.data[4:]  # убираем "grp:"
    corp_id = _resolve_corp(chat_id)

    # Показываем поиск
    await query.edit_message_text(
        f"{CLOCK} Устанавливаю группу <b>{_esc(group)}</b>...",
        parse_mode="HTML",
    )

    set_chat_group(chat_id, group)

    # Удаляем экран выбора
    try:
        await query.message.delete()
    except Exception:
        pass

    # Уведомление об успехе
    notice = await context.bot.send_message(
        chat_id=chat_id,
        text=f"{CHECK} Группа установлена: <b>{_esc(group)}</b>",
        parse_mode="HTML",
    )
    import asyncio as _asyncio
    await _asyncio.sleep(1.5)
    try:
        await notice.delete()
    except Exception:
        pass
    await _send_menu(context.bot, chat_id)
    return ConversationHandler.END


# ─── Ввод группы (ConversationHandler) ───────────────────────────────────────

async def receive_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group   = update.message.text.strip()
    corp_id = _resolve_corp(chat_id)

    # Валидация длины
    if len(group) > 50:
        try:
            await update.message.delete()
        except Exception:
            pass
        msg_id = context.user_data.get("waiting_group_msg_id")
        if msg_id:
            kb = [[InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")]]
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=msg_id,
                text=f"{CROSS} Слишком длинное название (макс. 50 символов).",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb),
            )
        return WAITING_GROUP

    # Удаляем сообщение пользователя
    try:
        await update.message.delete()
    except Exception:
        pass

    # Показываем статус поиска в сообщении с инструкцией
    msg_id = context.user_data.get("waiting_group_msg_id")
    if msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"{CLOCK} Ищу группу <b>{_esc(group)}</b>...",
                parse_mode="HTML",
            )
        except Exception:
            pass

    # Проверяем группу
    valid = False
    try:
        file_id = get_latest_file_id(corp_id)
        if file_id:
            test = parse_schedule(file_id, group, corp_id)
            valid = test is not None
    except Exception:
        valid = True  # не блокируем при ошибке

    if not valid:
        # Редактируем сообщение меню с ошибкой и кнопкой попробовать снова
        msg_id = context.user_data.get("waiting_group_msg_id")
        if msg_id:
            kb = [
                [InlineKeyboardButton("🔄 Попробовать снова", callback_data="m:setgroup")],
                [InlineKeyboardButton(f"{BACK} Назад", callback_data="m:back")],
            ]
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=f"{WARN} Группа <b>{_esc(group)}</b> не найдена.\n\n"
                         f"Проверь название в таблице расписания и попробуй снова.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(kb),
                )
            except Exception:
                pass
        return WAITING_GROUP

    set_chat_group(chat_id, group)

    # Редактируем сообщение меню — показываем обновлённый статус
    # Удаляем экран ввода группы
    msg_id = context.user_data.get("waiting_group_msg_id")
    if msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass

    # Показываем уведомление об успехе, потом меню
    notice = await context.bot.send_message(
        chat_id=chat_id,
        text=f"{CHECK} Группа установлена: <b>{_esc(group)}</b>",
        parse_mode="HTML",
    )
    import asyncio
    await asyncio.sleep(1.5)
    try:
        await notice.delete()
    except Exception:
        pass
    await _send_menu(context.bot, chat_id)
    return ConversationHandler.END

async def cancel_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена ввода группы по /start или /cancel."""
    await _send_menu(context.bot, update.effective_chat.id)
    return ConversationHandler.END

# ─── Команды для группового режима ───────────────────────────────────────────

async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group   = _resolve_group(chat_id)
    if not group:
        await update.message.reply_text(
            f"{CROSS} Сначала задай группу: /setgroup название",
            parse_mode="HTML",
        )
        return
    add_subscriber(chat_id)
    corp = CORPS_BY_ID.get(_resolve_corp(chat_id), {})
    await update.message.reply_text(
        f"{CHECK} Подписка оформлена! Корпус: <b>{_esc(corp.get('name', ''))}</b> · Группа: <b>{_esc(group)}</b>",
        parse_mode="HTML",
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    remove_subscriber(update.effective_chat.id)
    await update.message.reply_text(f"{CROSS} Вы отписаны.", parse_mode="HTML")


async def cmd_setcorp_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = [[InlineKeyboardButton(c["name"], callback_data=f"corp:{c['id']}")]
          for c in CORPS if not c.get("unsupported")]
    await update.message.reply_text(
        "🏢 Выбери корпус:",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def cmd_setgroup_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    group   = " ".join(context.args).strip() if context.args else ""

    if not group:
        await update.message.reply_text(
            "Использование: /setgroup название\n"
            "Например: <code>/setgroup 2-24 ОРП-1</code>",
            parse_mode="HTML",
        )
        return

    # Показываем статус поиска
    msg_id = context.user_data.get("waiting_group_msg_id")
    if msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"{CLOCK} Ищу группу <b>{_esc(group)}</b>...",
                parse_mode="HTML",
            )
        except Exception:
            pass

    set_chat_group(chat_id, group)

    try:
        corp_id = _resolve_corp(chat_id)
        file_id = get_latest_file_id(corp_id)
        if file_id:
            test = parse_schedule(file_id, group, corp_id)
            if not test:
                await update.message.reply_text(
                    f"{WARN} Группа <b>{group}</b> не найдена в последнем файле. Проверь название.",
                    parse_mode="HTML",
                )
                return
    except Exception:
        pass

    await update.message.reply_text(
        f"{CHECK} Группа установлена: <b>{_esc(group)}</b>",
        parse_mode="HTML",
    )

    if not is_group_mode(chat_id):
        await _send_menu(context.bot, chat_id)


# ─── /setup — мастер настройки группового чата ──────────────────────────────

SETUP_CORP   = 10
SETUP_COURSE = 11
SETUP_GROUP  = 12

async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пошаговый мастер настройки бота для группового чата."""
    if not update.message:
        return
    chat_id = update.effective_chat.id

    if not await _is_admin(update, context):
        await update.message.reply_text(
            f"{CROSS} Только администраторы могут запускать настройку.",
            parse_mode="HTML",
        )
        return

    kb = [[InlineKeyboardButton(c["name"], callback_data=f"setup_corp:{c['id']}")]
          for c in CORPS if not c.get("unsupported")]
    await update.message.reply_text(
        f"{WRENCH} <b>Настройка бота — Шаг 1 из 3</b>\n\n"
        "Выбери корпус для этого чата:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return SETUP_CORP


async def setup_cb_corp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    corp_id = query.data.split(":")[1]
    chat_id = query.message.chat.id

    set_chat_corp(chat_id, corp_id)
    context.user_data["setup_corp_id"] = corp_id

    kb = [
        [InlineKeyboardButton("1 курс", callback_data="setup_course:1"),
         InlineKeyboardButton("2 курс", callback_data="setup_course:2")],
        [InlineKeyboardButton("3 курс", callback_data="setup_course:3"),
         InlineKeyboardButton("4 курс", callback_data="setup_course:4")],
    ]
    await query.edit_message_text(
        f"{CHECK} Корпус: <b>{CORPS_BY_ID[corp_id]['name']}</b>\n\n"
        f"{WRENCH} <b>Шаг 2 из 3</b>\n\n"
        "Выбери курс:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return SETUP_COURSE


async def setup_cb_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    course  = int(query.data.split(":")[1])
    chat_id = query.message.chat.id
    corp_id = context.user_data.get("setup_corp_id") or _resolve_corp(chat_id)

    context.user_data["setup_course"] = course

    # Анимация загрузки
    async def animate():
        dots = [".", "..", "..."]
        for i in range(9):
            try:
                await query.edit_message_text(
                    f"{CLOCK} Загружаю список групп {course} курса{dots[i % 3]}",
                    parse_mode="HTML",
                )
                await asyncio.sleep(1)
            except Exception:
                break

    async def load():
        try:
            from api import _extract_groups_from_file
            from drive import export_as_xlsx
            file_id = get_latest_file_id(corp_id)
            if not file_id:
                raise Exception("Файлов не найдено")
            cfg = CORPS_BY_ID.get(corp_id, {})
            xlsx = export_as_xlsx(file_id)
            return _extract_groups_from_file(xlsx, cfg.get("table_format", "type_a"))
        except Exception as e:
            logger.warning("setup: ошибка групп: %s", e)
            return []

    anim = asyncio.create_task(animate())
    try:
        all_groups = await asyncio.wait_for(load(), timeout=15)
        anim.cancel()
    except asyncio.TimeoutError:
        anim.cancel()
        await query.edit_message_text(
            f"{WARN} Не удалось загрузить группы (таймаут 15 сек). Попробуй /setup снова.",
            parse_mode="HTML",
        )
        return ConversationHandler.END

    from datetime import date as _dt
    import re as _re
    cur_yr = _dt.today().year % 100
    entry_yr = (cur_yr - course + 1) if _dt.today().month >= 9 else (cur_yr - course)
    filtered = [g for g in all_groups
                if (m := _re.match(r'^\d+-(\d{2})', g.strip())) and int(m.group(1)) == entry_yr % 100]
    groups = filtered or all_groups

    if not groups:
        await query.edit_message_text(
            f"{WARN} Группы не найдены. Укажи вручную: /setgroup название",
            parse_mode="HTML",
        )
        return ConversationHandler.END

    import re as _re2
    kb = []
    for g in groups:
        s = _re2.match(r'^([\d\-\s]+[А-ЯЁA-Z]+[\-\d]+)', g.strip())
        label = s.group(1).strip() if s else g.strip()[:20]
        kb.append([InlineKeyboardButton(label, callback_data=f"setup_grp:{g[:50]}")])

    await query.edit_message_text(
        f"{WRENCH} <b>Шаг 3 из 3</b>\n\nВыбери группу ({course} курс):",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return SETUP_GROUP


async def setup_cb_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    group   = query.data[10:]  # убираем "setup_grp:"
    chat_id = query.message.chat.id
    corp_id = context.user_data.get("setup_corp_id") or _resolve_corp(chat_id)
    corp    = CORPS_BY_ID.get(corp_id, {})

    set_chat_group(chat_id, group)
    add_subscriber(chat_id)

    await query.edit_message_text(
        f"{CHECK} <b>Настройка завершена!</b>\n\n"
        f"🏢 Корпус: <b>{corp.get('name', corp_id)}</b>\n"
        f"👥 Группа: <b>{_esc(group)}</b>\n"
        f"{BELL} Авторассылка: <b>включена</b>\n\n"
        f"Теперь включи тихий режим командой /groupmode — "
        "бот будет автоматически присылать расписание без лишних кнопок.",
        parse_mode="HTML",
    )
    return ConversationHandler.END


# ─── /groupmode ──────────────────────────────────────────────────────────────

async def _is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверяет является ли пользователь admin в групповом чате."""
    chat = update.effective_chat
    # В личке все команды разрешены
    if chat.type == "private":
        return True
    user_id = update.effective_user.id
    try:
        member = await context.bot.get_chat_member(chat.id, user_id)
        return member.status in ("creator", "administrator")
    except Exception:
        return False


async def cmd_groupmode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Переключает групповой режим.
    В групповом режиме бот не показывает меню и кнопки —
    только авторассылка расписания. Идеально для группового чата.
    Только admin может включить в группах.
    """
    chat_id = update.effective_chat.id

    if not await _is_admin(update, context):
        await update.message.reply_text(
            f"{CROSS} Только администраторы чата могут менять режим работы бота.",
            parse_mode="HTML",
        )
        return

    currently = is_group_mode(chat_id)
    new_state = not currently
    set_group_mode(chat_id, new_state)

    if new_state:
        await update.message.reply_text(
            f"{CHECK} <b>Групповой режим включён.</b>\n\n"
            "В этом режиме бот молча рассылает расписание без меню и кнопок.\n\n"
            "<b>Доступные команды:</b>\n"
            f"{WRENCH} /setcorp — выбрать корпус\n"
            f"{WRENCH} /setgroup &lt;название&gt; — указать группу\n"
            f"   Пример: <code>/setgroup 2-24 ОРП-1</code>\n"
            f"{BELL} /subscribe — подписаться на авторассылку\n"
            f"{CROSS} /unsubscribe — отписаться\n"
            f"{CAL} /today — расписание на сегодня\n"
            f"{NEW} /new — последнее новое расписание\n\n"
            "Чтобы вернуться в обычный режим — /groupmode",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            f"{CROSS} <b>Групповой режим выключен.</b>\n\n"
            "Бот снова работает в обычном режиме. Напиши /start.",
            parse_mode="HTML",
        )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбрасывает настройки пользователя. Только для ADMIN_ID."""
    if not update.message:
        return
    user_id = update.effective_user.id

    # В личке — сбрасываем свои настройки
    # С аргументом chat_id — сбрасываем чужие (только ADMIN_ID)
    if context.args and ADMIN_ID and user_id == ADMIN_ID:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(f"{CROSS} Неверный chat_id.", parse_mode="HTML")
            return
    else:
        target_id = update.effective_chat.id

    # Только ADMIN_ID может сбрасывать чужие настройки
    if target_id != update.effective_chat.id and (not ADMIN_ID or user_id != ADMIN_ID):
        return

    from db import _conn
    with _conn() as con:
        con.execute("DELETE FROM chat_settings WHERE chat_id = ?", (target_id,))
        con.execute("DELETE FROM subscribers WHERE chat_id = ?", (target_id,))

    await update.message.reply_text(
        f"{CHECK} Настройки сброшены для chat_id <code>{target_id}</code>.\n"
        "Группа, корпус и подписка удалены.",
        parse_mode="HTML",
    )


# ─── /status (только для ADMIN_ID) ──────────────────────────────────────────

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статус бота — только для администратора."""
    if ADMIN_ID == 0:
        await update.message.reply_text(
            f"{WARN} Добавь переменную ADMIN_ID в Railway (твой Telegram ID).",
            parse_mode="HTML",
        )
        return
    if update.effective_user.id != ADMIN_ID:
        return

    subs  = get_all_subscribers()
    stats = get_scheduler_stats()

    corp_counts: dict[str, int] = {}
    for sub in subs:
        cid = sub.get("corp_id") or DEFAULT_CORP
        corp_counts[cid] = corp_counts.get(cid, 0) + 1

    corps_lines = []
    for corp_id, count in corp_counts.items():
        name = CORPS_BY_ID.get(corp_id, {}).get("name", corp_id)
        corps_lines.append(f"  {name}: {count} подп.")
    corps_text = "\n".join(corps_lines)

    stats_lines = []
    for s in stats:
        name = CORPS_BY_ID.get(s["corp_id"], {}).get("name", s["corp_id"])
        err  = s["error_count"]
        last = s["last_success"] or "никогда"
        line = f"  {name}: успех {last}"
        if err:
            line += f", ошибок: {err}"
        stats_lines.append(line)
    stats_text = "\n".join(stats_lines) or "  Нет данных"

    interval = os.environ.get("CHECK_INTERVAL_MINUTES", "10")
    text = (
        "<b>Статус бота ВПТ</b>\n\n"
        f"\U0001f465 Всего подписчиков: <b>{len(subs)}</b>\n"
        f"{corps_text}\n\n"
        "\U0001f504 Drive проверки:\n"
        f"{stats_text}\n\n"
        f"\u23f1 Интервал: {interval} мин."
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def on_broadcast_done(application, corp_name: str, sent_count: int):
    """Шлёт сводку админу после рассылки."""
    if not ADMIN_ID:
        return
    try:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"📬 Разослано расписание [{corp_name}] — {sent_count} получателей",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Ошибка сводки: %s", e)


# ─── Авторассылка ─────────────────────────────────────────────────────────────

async def broadcast(application, file_id: str, corp_id: str) -> int:
    """Рассылает новое расписание. Возвращает количество отправленных."""
    subscribers = get_all_subscribers()
    cache: dict[str, dict | None] = {}
    sent = 0
    for sub in subscribers:
        if (sub.get("corp_id") or DEFAULT_CORP) != corp_id:
            continue
        chat_id = sub["chat_id"]
        group   = sub["group_name"] or DEFAULT_GROUP
        if not group:
            continue
        key = f"{corp_id}:{group}"
        if key not in cache:
            try:
                cache[key] = parse_schedule(file_id, group, corp_id)
            except Exception as e:
                logger.warning("Ошибка парсинга %s: %s", key, e)
                cache[key] = None
        data = cache[key]
        if not data:
            continue
        text = f"{NEW} <b>Новое расписание!</b>\n\n" + format_schedule(data)
        try:
            await _send_with_gif(application.bot, chat_id, text)
            sent += 1
            if sent % 25 == 0:
                await asyncio.sleep(1)
        except Exception as e:
            logger.warning("Ошибка отправки %s: %s", chat_id, e)
    return sent

async def broadcast_changed(application, file_id: str, corp_id: str, diffs: dict) -> int:
    subscribers = get_all_subscribers()
    cache: dict[str, dict | None] = {}
    sent = 0
    for sub in subscribers:
        if (sub.get("corp_id") or DEFAULT_CORP) != corp_id:
            continue
        chat_id = sub["chat_id"]
        group   = sub["group_name"] or DEFAULT_GROUP
        if not group:
            continue
        key = f"{corp_id}:{group}"
        if key not in cache:
            try:
                cache[key] = parse_schedule(file_id, group, corp_id)
            except Exception:
                cache[key] = None
        data = cache[key]
        if not data:
            continue
        diff_text = diffs.get(key, "")
        d       = data.get("date", "")
        day     = data.get("day", "")
        day_str = f", {day}" if day else ""
        text = (
            f"{WARN} <b>Расписание на {d}{day_str} изменилось!</b>\n\n"
            + diff_text + "\n\n📋 Актуальное расписание:\n\n" + format_schedule(data)
            if diff_text else
            f"{WARN} <b>Расписание на {d}{day_str} обновлено!</b>\n\n" + format_schedule(data)
        )
        try:
            await _send_with_gif(application.bot, chat_id, text)
            sent += 1
            if sent % 25 == 0:
                await asyncio.sleep(1)
        except Exception as e:
            logger.warning("Ошибка отправки %s: %s", chat_id, e)
    return sent

async def alert_drive_error(application, error_msg: str):
    if not ALERT_CHAT_ID:
        return
    try:
        await application.bot.send_message(
            chat_id=int(ALERT_CHAT_ID),
            text=f"{WARN} <b>Ошибка Drive!</b>\n\n<code>{error_msg}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Ошибка алерта: %s", e)

# ─── Запуск ───────────────────────────────────────────────────────────────────

async def cb_delete_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет сообщение с расписанием по нажатию кнопки 🗑 Удалить."""
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


# ─── Обработчики ReplyKeyboard кнопок ────────────────────────────────────────

async def handle_reply_btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия постоянных кнопок в чатбаре."""
    if not update.message or not update.message.text:
        return
    text    = update.message.text.strip()
    chat_id = update.effective_chat.id

    # Удаляем сообщение пользователя чтобы не засорять чат
    try:
        await update.message.delete()
    except Exception:
        pass

    if text == BTN_TODAY:
        await _cmd_direct(update, context, "today")
    elif text == BTN_NEW:
        await _cmd_direct(update, context, "new")
    elif text == BTN_CORP:
        kb = [[InlineKeyboardButton(c["name"], callback_data=f"corp:{c['id']}")]
              for c in CORPS if not c.get("unsupported")]
        await context.bot.send_message(
            chat_id=chat_id,
            text="🏢 Выбери корпус:",
            reply_markup=InlineKeyboardMarkup(kb),
        )
    elif text == BTN_GROUP:
        kb = [
            [InlineKeyboardButton("1 курс", callback_data="course:1"),
             InlineKeyboardButton("2 курс", callback_data="course:2")],
            [InlineKeyboardButton("3 курс", callback_data="course:3"),
             InlineKeyboardButton("4 курс", callback_data="course:4")],
        ]
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"{WRENCH} <b>Шаг 1:</b> Выбери свой курс",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        context.user_data["course_select_msg_id"] = msg.message_id
    elif text in (BTN_SUB, BTN_UNSUB):
        group = _resolve_group(chat_id)
        if not group:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"{CROSS} Сначала выбери группу.",
                parse_mode="HTML",
            )
            return
        if text == BTN_SUB:
            add_subscriber(chat_id)
            corp = CORPS_BY_ID.get(_resolve_corp(chat_id), {})
            notice = await context.bot.send_message(
                chat_id=chat_id,
                text=f"{CHECK} Подписка оформлена! {corp.get('name','')} · {_esc(group)}",
                parse_mode="HTML",
                reply_markup=_reply_kb(True),
            )
        else:
            remove_subscriber(chat_id)
            notice = await context.bot.send_message(
                chat_id=chat_id,
                text=f"{CROSS} Отписка оформлена.",
                parse_mode="HTML",
                reply_markup=_reply_kb(False),
            )


async def _cmd_direct(update: Update, context: ContextTypes.DEFAULT_TYPE, cmd: str):
    """Прямые команды /today и /new."""
    chat_id = update.effective_chat.id
    group   = _resolve_group(chat_id)
    corp_id = _resolve_corp(chat_id)
    if not group:
        # Автоматически открываем флоу выбора группы
        kb = [
            [InlineKeyboardButton("1 курс", callback_data="course:1"),
             InlineKeyboardButton("2 курс", callback_data="course:2")],
            [InlineKeyboardButton("3 курс", callback_data="course:3"),
             InlineKeyboardButton("4 курс", callback_data="course:4")],
        ]
        msg = await update.message.reply_text(
            f"{PIN} Сначала выбери курс — и я покажу доступные группы.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        context.user_data["course_select_msg_id"] = msg.message_id
        return SELECT_COURSE
    wait = _check_cooldown(chat_id, cmd)
    if wait:
        await update.message.reply_text(
            f"{CLOCK} Подожди ещё <b>{wait} сек.</b>", parse_mode="HTML"
        )
        return
    msg = await update.message.reply_text(f"{CLOCK} Загружаю...", parse_mode="HTML")
    try:
        file_id = get_today_file_id(corp_id) if cmd == "today" else get_latest_file_id(corp_id)
        if not file_id:
            await msg.edit_text(f"{CROSS} Файлов не найдено.", parse_mode="HTML")
            return
        _set_cooldown(chat_id, cmd)
        await msg.delete()
        await _fetch_and_send(context.bot, chat_id, file_id, group, corp_id)
    except Exception:
        logger.exception("Ошибка /%s", cmd)
        try:
            await msg.edit_text(f"{WARN} Произошла ошибка. Попробуй позже.", parse_mode="HTML")
        except Exception:
            pass


# ─── Standalone хэндлеры курса/группы (работают вне ConversationHandler) ────

async def cb_course_standalone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Хэндлер выбора курса — работает вне ConversationHandler (из ReplyKeyboard)."""
    query = update.callback_query
    # Если мы внутри ConversationHandler — делегируем туда
    # Иначе обрабатываем самостоятельно
    await cb_course(update, context)


async def cb_group_select_standalone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Хэндлер выбора группы — работает вне ConversationHandler (из ReplyKeyboard)."""
    await cb_group_select(update, context)


def _build_ptb_app():
    """Собирает PTB Application с хэндлерами."""
    ptb = Application.builder().token(TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup)],
        states={
            SETUP_CORP:   [CallbackQueryHandler(setup_cb_corp,   pattern=r"^setup_corp:")],
            SETUP_COURSE: [CallbackQueryHandler(setup_cb_course, pattern=r"^setup_course:")],
            SETUP_GROUP:  [CallbackQueryHandler(setup_cb_group,  pattern=r"^setup_grp:")],
        },
        fallbacks=[CommandHandler("cancel", cancel_group)],
        per_message=False,
    )
    ptb.add_handler(setup_conv)

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_menu, pattern=r"^m:")],
        states={
            WAITING_GROUP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_group),
                CallbackQueryHandler(cb_menu, pattern=r"^m:"),
            ],
            SELECT_COURSE: [
                CallbackQueryHandler(cb_course, pattern=r"^course:"),
                CallbackQueryHandler(cb_menu,   pattern=r"^m:"),
            ],
            SELECT_GROUP: [
                CallbackQueryHandler(cb_group_select, pattern=r"^grp:"),
                CallbackQueryHandler(cb_menu,         pattern=r"^m:"),
                CallbackQueryHandler(cb_course,       pattern=r"^course:"),
            ],
        },
        fallbacks=[
            CommandHandler("start",  cancel_group),
            CommandHandler("cancel", cancel_group),
            CallbackQueryHandler(cb_menu,  pattern=r"^m:"),
            CallbackQueryHandler(cb_corp,  pattern=r"^corp:"),
        ],
        per_message=False,
        conversation_timeout=300,
    )

    ptb.add_handler(CommandHandler("start",       cmd_start))
    ptb.add_handler(CommandHandler("status",      cmd_status))
    ptb.add_handler(CommandHandler("reset",       cmd_reset))
    ptb.add_handler(CommandHandler("setup",       cmd_setup))

    # ReplyKeyboard кнопки
    reply_btns = [BTN_TODAY, BTN_NEW, BTN_CORP, BTN_GROUP, BTN_SUB, BTN_UNSUB]
    ptb.add_handler(MessageHandler(
        filters.TEXT & filters.Regex("^(" + "|".join(reply_btns) + ")$"),
        handle_reply_btn,
    ))
    ptb.add_handler(CommandHandler("groupmode",   cmd_groupmode))
    ptb.add_handler(CommandHandler("today",       lambda u, c: _cmd_direct(u, c, "today")))
    ptb.add_handler(CommandHandler("new",         lambda u, c: _cmd_direct(u, c, "new")))
    ptb.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    ptb.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    ptb.add_handler(CommandHandler("setcorp",     cmd_setcorp_text))
    ptb.add_handler(CommandHandler("setgroup",    cmd_setgroup_text))
    ptb.add_handler(conv)
    ptb.add_handler(CallbackQueryHandler(cb_corp,       pattern=r"^corp:"))
    ptb.add_handler(CallbackQueryHandler(cb_delete_msg, pattern=r"^del:"))
    # Standalone хэндлеры для выбора курса/группы — работают вне ConversationHandler
    ptb.add_handler(CallbackQueryHandler(cb_course_standalone,      pattern=r"^course:"))
    ptb.add_handler(CallbackQueryHandler(cb_group_select_standalone, pattern=r"^grp:"))

    return ptb


def main():
    import uvicorn
    from api import app as fastapi_app

    init_db()

    # Сбрасываем кэш gif_file_id если файл изменился
    if os.path.exists(GIF_PATH):
        mtime = str(int(os.path.getmtime(GIF_PATH)))
        from db import kv_get, kv_set
        if kv_get('gif_mtime') != mtime:
            kv_set('gif_mtime', mtime)
            save_gif_file_id('')
            logger.info("GIF обновлён — сброс кэша")

    ptb = _build_ptb_app()
    port = int(os.environ.get("PORT", "8000"))

    async def run_all():
        # Запускаем FastAPI
        api_config = uvicorn.Config(
            fastapi_app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
        api_server = uvicorn.Server(api_config)

        # Запускаем PTB
        async with ptb:
            await ptb.initialize()
            await ptb.start()
            # Запускаем планировщик ПОСЛЕ initialize — bot_data уже доступен
            start_scheduler(ptb, broadcast, broadcast_changed, alert_drive_error, on_broadcast_done)
            # Error handler
            async def _error_handler(update, context):
                from telegram.error import Conflict, TimedOut, NetworkError
                err = context.error
                if isinstance(err, Conflict):
                    logger.warning("Конфликт двух экземпляров — ждём завершения старого")
                    return
                if isinstance(err, (TimedOut, NetworkError)):
                    logger.warning("Сетевая ошибка: %s", type(err).__name__)
                    return
                logger.exception("Необработанная ошибка PTB", exc_info=err)
            ptb.add_error_handler(_error_handler)

            # Ждём завершения предыдущего экземпляра
            await asyncio.sleep(5)
            await ptb.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
            logger.info("Бот запущен, API на порту %d", port)
            await api_server.serve()
            await ptb.updater.stop()
            await ptb.stop()

    asyncio.run(run_all())


if __name__ == "__main__":
    main()
