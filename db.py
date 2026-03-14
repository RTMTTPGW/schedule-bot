import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "bot.db")


def _conn():
    return sqlite3.connect(DB_PATH)


def init_db():
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                subscribed_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                file_id TEXT PRIMARY KEY,
                seen_at  TEXT DEFAULT (datetime('now'))
            )
        """)
    logger.info("БД инициализирована: %s", DB_PATH)


# ─── Подписчики ───────────────────────────────────────────────────────────────

def add_subscriber(chat_id: int):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)",
            (chat_id,),
        )


def remove_subscriber(chat_id: int):
    with _conn() as con:
        con.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))


def get_all_subscribers() -> list[int]:
    with _conn() as con:
        rows = con.execute("SELECT chat_id FROM subscribers").fetchall()
    return [r[0] for r in rows]


# ─── Отслеживание новых файлов ────────────────────────────────────────────────

def is_file_seen(file_id: str) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
    return row is not None


def mark_file_seen(file_id: str):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO seen_files (file_id) VALUES (?)",
            (file_id,),
        )
