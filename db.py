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
                chat_id    INTEGER PRIMARY KEY,
                group_name TEXT DEFAULT NULL,
                subscribed_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                file_id    TEXT PRIMARY KEY,
                seen_at    TEXT DEFAULT (datetime('now')),
                sched_hash TEXT DEFAULT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # group_name на чат (для групп Telegram и ЛС)
        con.execute("""
            CREATE TABLE IF NOT EXISTS chat_group (
                chat_id    INTEGER PRIMARY KEY,
                group_name TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
    logger.info("БД инициализирована: %s", DB_PATH)


# ─── Группа чата ──────────────────────────────────────────────────────────────

def set_chat_group(chat_id: int, group_name: str):
    """Сохраняет выбранную группу для чата или пользователя."""
    with _conn() as con:
        con.execute(
            """
            INSERT INTO chat_group (chat_id, group_name)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                group_name = excluded.group_name,
                updated_at = datetime('now')
            """,
            (chat_id, group_name),
        )


def get_chat_group(chat_id: int) -> str | None:
    """Возвращает группу для чата, или None если не задана."""
    with _conn() as con:
        row = con.execute(
            "SELECT group_name FROM chat_group WHERE chat_id = ?", (chat_id,)
        ).fetchone()
    return row[0] if row else None


# ─── Подписчики ───────────────────────────────────────────────────────────────

def add_subscriber(chat_id: int):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,)
        )


def remove_subscriber(chat_id: int):
    with _conn() as con:
        con.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))


def get_all_subscribers() -> list[dict]:
    """Возвращает список {'chat_id': ..., 'group_name': ...} для всех подписчиков."""
    with _conn() as con:
        rows = con.execute("""
            SELECT s.chat_id, cg.group_name
            FROM subscribers s
            LEFT JOIN chat_group cg ON cg.chat_id = s.chat_id
        """).fetchall()
    return [{"chat_id": r[0], "group_name": r[1]} for r in rows]


# ─── Файлы расписания ─────────────────────────────────────────────────────────

def is_file_seen(file_id: str) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
    return row is not None


def get_file_hash(file_id: str) -> str | None:
    with _conn() as con:
        row = con.execute(
            "SELECT sched_hash FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
    return row[0] if row else None


def mark_file_seen(file_id: str, sched_hash: str | None = None):
    with _conn() as con:
        con.execute(
            """
            INSERT INTO seen_files (file_id, sched_hash)
            VALUES (?, ?)
            ON CONFLICT(file_id) DO UPDATE SET sched_hash = excluded.sched_hash
            """,
            (file_id, sched_hash),
        )


# ─── KV-хранилище ─────────────────────────────────────────────────────────────

def get_gif_file_id() -> str | None:
    with _conn() as con:
        row = con.execute(
            "SELECT value FROM kv WHERE key = 'gif_file_id'"
        ).fetchone()
    return row[0] if row else None


def save_gif_file_id(file_id: str):
    with _conn() as con:
        con.execute(
            "INSERT INTO kv (key, value) VALUES ('gif_file_id', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (file_id,),
        )
