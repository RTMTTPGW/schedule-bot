import os
import io
import re
import requests
from datetime import datetime, timedelta
from uuid import uuid4

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from openpyxl import load_workbook
from bs4 import BeautifulSoup

# ================= НАСТРОЙКИ =================

TOKEN = os.getenv("BOT_TOKEN")
FOLDER_ID = "1fxehYVWNrEC5EoHnrzgaxoSyCDXCDTur"
GROUP_NAME = "2-24 ОРП-1"

if not TOKEN:
    raise ValueError("BOT_TOKEN не найден")

# ================= GOOGLE DRIVE (PUBLIC PARSING) =================

def find_file_id():
    folder_url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
    response = requests.get(folder_url)
    html = response.text

    today = datetime.now()
    dates = [
        today + timedelta(days=1),
        today,
        today - timedelta(days=1),
    ]

    for date in dates:
        date_str = date.strftime("%d.%m.%Y")

        # Ищем блок где есть дата
        pattern = rf'(["\'])([a-zA-Z0-9_-]{{25,}})\1.*?Расписание\s+{re.escape(date_str)}'
        match = re.search(pattern, html)

        if match:
            return match.group(2)

    return None


def download_file(file_id):
    session = requests.Session()

    url = "https://drive.google.com/uc?export=download"
    response = session.get(url, params={"id": file_id}, stream=True)

    # Проверяем наличие confirm token
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            response = session.get(
                url,
                params={"id": file_id, "confirm": value},
                stream=True,
            )
            break

    return io.BytesIO(response.content)

# ================= PARSE XLSX =================

def parse_schedule(file_bytes):
    wb = load_workbook(file_bytes)
    sheet = wb.active

    schedule = []
    found = False

    for row in sheet.iter_rows(values_only=True):
        row_values = [str(cell) if cell else "" for cell in row]

        if GROUP_NAME in row_values:
            found = True
            continue

        if found:
            if isinstance(row[0], int):
                subject = row[1] or ""
                teacher = row[2] or ""
                cabinet = row[3] or ""

                schedule.append(
                    f"{row[0]}. {subject}\n"
                    f"Преп: {teacher}\n"
                    f"Каб: {cabinet}\n"
                )
            else:
                break

    if not schedule:
        return "Расписание не найдено."

    return "\n".join(schedule)


# ================= TELEGRAM =================

bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.inline_query()
async def inline_handler(inline_query: types.InlineQuery):
    try:
        file_id = find_file_id()

        if not file_id:
            text = "Файл расписания не найден."
        else:
            file_bytes = download_file(file_id)
            text = parse_schedule(file_bytes)

    except Exception as e:
        text = f"Ошибка: {str(e)}"

    result = InlineQueryResultArticle(
        id=str(uuid4()),
        title="Расписание 2-24 ОРП-1",
        input_message_content=InputTextMessageContent(message_text=text)
    )

    await inline_query.answer([result], cache_time=1)


# ================= WEBHOOK =================

async def on_startup(app):
    webhook_url = os.getenv("RENDER_EXTERNAL_URL")
    if not webhook_url:
        raise ValueError("RENDER_EXTERNAL_URL не найден")

    await bot.set_webhook(webhook_url)


app = web.Application()
app.on_startup.append(on_startup)

SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/")
setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
