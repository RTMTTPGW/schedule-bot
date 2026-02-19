import os
import re
from datetime import datetime
import telebot
from google.oauth2 import service_account
from googleapiclient.discovery import build

TOKEN = os.getenv("BOT_TOKEN")
FOLDER_ID = os.getenv("FOLDER_ID")

bot = telebot.TeleBot(TOKEN)

# === Google API ===
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
credentials = service_account.Credentials.from_service_account_file(
    'credentials.json',
    scopes=SCOPES
)

drive_service = build('drive', 'v3', credentials=credentials)
docs_service = build('docs', 'v1', credentials=credentials)


# === Поиск файла по дате ===
def find_schedule_file(date_string):
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)"
    ).execute()

    files = results.get('files', [])

    for file in files:
        if date_string in file['name']:
            return file['id']

    return None


# === Получение текста из Google Docs ===
def get_doc_text(doc_id):
    document = docs_service.documents().get(documentId=doc_id).execute()
    content = document.get('body').get('content')

    text = ""

    for element in content:
        if 'paragraph' in element:
            for run in element['paragraph']['elements']:
                if 'textRun' in run:
                    text += run['textRun']['content']

    return text


# === Парсинг расписания ===
def parse_schedule(text):
    pairs = {}
    current_pair = None

    lines = text.split('\n')

    for line in lines:
        line = line.strip()

        # Ищем номер пары
        match = re.match(r"(\d)\s*пара", line.lower())
        if match:
            current_pair = int(match.group(1))
            pairs[current_pair] = {
                "subject": "",
                "teacher": "",
                "room": ""
            }
            continue

        if current_pair:
            if not pairs[current_pair]["subject"]:
                pairs[current_pair]["subject"] = line
            elif not pairs[current_pair]["teacher"]:
                pairs[current_pair]["teacher"] = line
            elif not pairs[current_pair]["room"]:
                pairs[current_pair]["room"] = line

    return pairs


# === Красивый вывод ===
def format_schedule(date_string, pairs):
    if not pairs:
        return "Расписание не найдено."

    text = f"📅 Расписание на {date_string}\n"
    text += "━━━━━━━━━━━━━━━━━━\n\n"

    for number in sorted(pairs.keys()):
        pair = pairs[number]

        text += f"🔹 {number} пара\n"
        text += f"   📖 {pair['subject']}\n"
        text += f"   👩‍🏫 {pair['teacher']}\n"
        text += f"   🏫 {pair['room']}\n\n"

    return text


# === Команда /today ===
@bot.message_handler(commands=['today'])
def today(message):
    today_date = datetime.now().strftime("%d.%m.%Y")

    file_id = find_schedule_file(today_date)

    if not file_id:
        bot.send_message(message.chat.id, "Расписание не найдено.")
        return

    text = get_doc_text(file_id)
    pairs = parse_schedule(text)
    formatted = format_schedule(today_date, pairs)

    bot.send_message(message.chat.id, formatted)


# === Запуск ===
bot.infinity_polling()
