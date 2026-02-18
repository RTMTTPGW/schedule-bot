import os
import io
from datetime import datetime, timedelta
from uuid import uuid4

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineQueryResultArticle, InputTextMessageContent
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from google.oauth2 import service_account
from googleapiclient.discovery import build
from openpyxl import load_workbook

TOKEN = os.getenv("8512190832:AAGM5Aj_IzyWX77mnxozabEjOwTx7MdAwF0")
FOLDER_ID = "1fxehYVWNrEC5EoHnrzgaxoSyCDXCDTur"
GROUP_NAME = "2-24 ОРП-1"

# ================= GOOGLE DRIVE =================

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

credentials = service_account.Credentials.from_service_account_file(
    "credentials.json",
    scopes=SCOPES
)

drive_service = build("drive", "v3", credentials=credentials)

def find_file():
    today = datetime.now()
    dates = [
        today + timedelta(days=1),
        today,
        today - timedelta(days=1),
    ]

    for date in dates:
        date_str = date.strftime("%d.%m.%Y")

        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and name contains '{date_str}'",
            fields="files(id, name)"
        ).execute()

        files = results.get("files", [])
        if files:
            return files[0]["id"]

    return None

def download_file(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    return io.BytesIO(request.execute())

def parse_schedule(file_bytes):
    wb = load_workbook(file_bytes)
    sheet = wb.active

    schedule = []
    found = False

    for row in sheet.iter_rows(values_only=True):
        if GROUP_NAME in [str(cell) for cell in row if cell]:
            found = True
            continue

        if found:
            if isinstance(row[0], int):
                schedule.append(
                    f"{row[0]}. {row[1]}\nПреп: {row[2]}\nКаб: {row[3]}\n"
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
    file_id = find_file()

    if not file_id:
        text = "Файл расписания не найден."
    else:
        file_bytes = download_file(file_id)
        text = parse_schedule(file_bytes)

    result = InlineQueryResultArticle(
        id=str(uuid4()),
        title="Расписание 2-24 ОРП-1",
        input_message_content=InputTextMessageContent(message_text=text)
    )

    await inline_query.answer([result], cache_time=1)

# ================= WEBHOOK =================

async def on_startup(bot: Bot):
    await bot.set_webhook(os.getenv("RENDER_EXTERNAL_URL"))

app = web.Application()
SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/")
setup_application(app, dp, bot=bot)
web.run_app(app, host="0.0.0.0", port=10000)
