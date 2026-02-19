import os
import io
import requests
from datetime import datetime, timedelta
from uuid import uuid4

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from openpyxl import load_workbook

TOKEN = os.getenv("BOT_TOKEN")
DRIVE_API_KEY = os.getenv("DRIVE_API_KEY")

FOLDER_ID = "1fxehYVWNrEC5EoHnrzgaxoSyCDXCDTur"
GROUP_NAME = "2-24 ОРП-1"

if not TOKEN:
    raise ValueError("BOT_TOKEN не найден")

if not DRIVE_API_KEY:
    raise ValueError("DRIVE_API_KEY не найден")


# ================= GOOGLE DRIVE API =================

def find_file_id():
    today = datetime.now()
    dates = [
        today + timedelta(days=1),
        today,
        today - timedelta(days=1),
    ]

    url = "https://www.googleapis.com/drive/v3/files"

    params = {
        "q": f"'{FOLDER_ID}' in parents",
        "fields": "files(id, name)",
        "key": DRIVE_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()
    files = data.get("files", [])

    for date in dates:
        date_str = date.strftime("%d.%m.%Y")

        for file in files:
            if date_str in file["name"]:
                return file["id"], date_str

    return None, None


def download_file(file_id):
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export"

    params = {
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "key": DRIVE_API_KEY
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"Ошибка загрузки файла: {response.text}")

    return io.BytesIO(response.content)


# ================= PARSE XLSX =================

def parse_schedule(file_bytes, date_str):
    wb = load_workbook(file_bytes)
    sheet = wb.active

    schedule = []
    found = False
    target_group = GROUP_NAME.strip().lower()

    for row in sheet.iter_rows(values_only=True):
        row_values = [str(cell).strip() if cell else "" for cell in row]

        # Ищем строку с группой
        if not found:
            if any(target_group in cell.lower() for cell in row_values):
                found = True
            continue

        # Номер пары в колонке A
        pair_number = row_values[0]

        if pair_number.isdigit():
            subject = row_values[1] if len(row_values) > 1 else ""
            teacher = row_values[4] if len(row_values) > 4 else ""
            cabinet = row_values[6] if len(row_values) > 6 else ""

            schedule.append({
                "number": pair_number,
                "subject": subject,
                "teacher": teacher,
                "cabinet": cabinet
            })

    if not schedule:
        return "Расписание не найдено."

    # Красивое форматирование
  result = f"📅 Расписание на {date_str}\n\n"

    for lesson in schedule:
        result += (
            f"🔹 {lesson['number']} пара\n"
            f"   📖 {lesson['subject']}\n"
            f"   👩‍🏫 {lesson['teacher']}\n"
            f"   🏫 Кабинет: {lesson['cabinet']}\n\n"
        )
    return result

# ================= TELEGRAM =================

bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.inline_query()
async def inline_handler(inline_query: types.InlineQuery):
    try:
        file_id, date_str = find_file_id()

if not file_id:
    text = "Файл расписания не найден."
else:
    file_bytes = download_file(file_id)
    text = parse_schedule(file_bytes, date_str)


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
    await bot.set_webhook(webhook_url)


app = web.Application()
app.on_startup.append(on_startup)

SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/")
setup_application(app, dp, bot=bot)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
