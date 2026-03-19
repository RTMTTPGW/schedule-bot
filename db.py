import sqlite3
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "bot.db")

import threading
_local = threading.local()

def _conn() -> sqlite3.Connection:
    if not getattr(_local, "conn", None):
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-8000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn

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
                seen_at TEXT DEFAULT (datetime('now')),
                sched_hash TEXT DEFAULT NULL,
                file_date TEXT DEFAULT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                group_name TEXT DEFAULT NULL,
                corp_id TEXT DEFAULT NULL,
                group_mode INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS scheduler_stats (
                corp_id TEXT PRIMARY KEY,
                last_check TEXT DEFAULT NULL,
                last_success TEXT DEFAULT NULL,
                error_count INTEGER DEFAULT 0,
                alert_sent INTEGER DEFAULT 0
            )
        """)
    logger.info("БД инициализирована: %s", DB_PATH)

# ─── Настройки чата ───────────────────────────────────────────────────────────

def set_chat_group(chat_id: int, group_name: str):
    with _conn() as con:
        con.execute("""
            INSERT INTO chat_settings (chat_id, group_name)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
            group_name = excluded.group_name,
            updated_at = datetime('now')
        """, (chat_id, group_name))

def get_chat_group(chat_id: int) -> str | None:
    with _conn() as con:
        row = con.execute(
            "SELECT group_name FROM chat_settings WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return row[0] if row else None

def set_chat_corp(chat_id: int, corp_id: str):
    with _conn() as con:
        con.execute("""
            INSERT INTO chat_settings (chat_id, corp_id)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
            corp_id = excluded.corp_id,
            updated_at = datetime('now')
        """, (chat_id, corp_id))

def get_chat_corp(chat_id: int) -> str | None:
    with _conn() as con:
        row = con.execute(
            "SELECT corp_id FROM chat_settings WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return row[0] if row else None

# ─── Подписчики ───────────────────────────────────────────────────────────────

def is_subscriber(chat_id: int) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM subscribers WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return row is not None

def add_subscriber(chat_id: int):
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,)
        )

def remove_subscriber(chat_id: int):
    with _conn() as con:
        con.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))

def get_all_subscribers() -> list[dict]:
    with _conn() as con:
        rows = con.execute("""
            SELECT s.chat_id, cs.group_name, cs.corp_id
            FROM subscribers s
            LEFT JOIN chat_settings cs ON cs.chat_id = s.chat_id
        """).fetchall()
        return [{"chat_id": r[0], "group_name": r[1], "corp_id": r[2]} for r in rows]

def get_subscribers_for_corp(corp_id: str) -> list[dict]:
    """Возвращает подписчиков конкретного корпуса."""
    with _conn() as con:
        rows = con.execute("""
            SELECT s.chat_id, cs.group_name, cs.corp_id
            FROM subscribers s
            LEFT JOIN chat_settings cs ON cs.chat_id = s.chat_id
            WHERE cs.corp_id = ? OR cs.corp_id IS NULL
        """, (corp_id,)).fetchall()
        return [{"chat_id": r[0], "group_name": r[1], "corp_id": r[2]} for r in rows]

def get_subscribed_corp_ids() -> set[str]:
    """Возвращает множество corp_id у которых есть подписчики."""
    with _conn() as con:
        rows = con.execute("""
            SELECT DISTINCT cs.corp_id
            FROM subscribers s
            LEFT JOIN chat_settings cs ON cs.chat_id = s.chat_id
            WHERE cs.corp_id IS NOT NULL
        """).fetchall()
        return {r[0] for r in rows if r[0]}

# ─── Файлы расписания + кэш дат ───────────────────────────────────────────────

def is_file_seen(file_id: str) -> bool:
    """
    Файл считается «обработанным» только если у него есть sched_hash.
    Наличие только file_date (кэш даты) НЕ считается признаком обработки.
    """
    with _conn() as con:
        row = con.execute(
            "SELECT sched_hash FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
        return row is not None and row[0] is not None

def get_file_hash(file_id: str) -> str | None:
    with _conn() as con:
        row = con.execute(
            "SELECT sched_hash FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
        return row[0] if row else None

def get_file_date_cached(file_id: str) -> str | None:
    """Возвращает закэшированную дату файла (строка DD.MM.YYYY) или None."""
    with _conn() as con:
        row = con.execute(
            "SELECT file_date FROM seen_files WHERE file_id = ?", (file_id,)
        ).fetchone()
        return row[0] if row and row[0] else None

def cache_file_date_only(file_id: str, file_date: str):
    """
    Кэширует ТОЛЬКО дату файла, не помечая его как обработанный.
    sched_hash остаётся NULL → is_file_seen вернёт False до реальной рассылки.
    """
    with _conn() as con:
        con.execute("""
            INSERT INTO seen_files (file_id, file_date)
            VALUES (?, ?)
            ON CONFLICT(file_id) DO UPDATE SET
            file_date = COALESCE(excluded.file_date, seen_files.file_date)
        """, (file_id, file_date))

def mark_file_seen(file_id: str, sched_hash: str | None = None, file_date: str | None = None):
    """
    Помечает файл как обработанный (рассылка была).
    Если sched_hash передан — файл будет считаться seen.
    Если передан только file_date без hash — используй cache_file_date_only вместо этого.
    """
    with _conn() as con:
        con.execute("""
            INSERT INTO seen_files (file_id, sched_hash, file_date)
            VALUES (?, ?, ?)
            ON CONFLICT(file_id) DO UPDATE SET
            sched_hash = COALESCE(excluded.sched_hash, seen_files.sched_hash),
            file_date = COALESCE(excluded.file_date, seen_files.file_date)
        """, (file_id, sched_hash, file_date))

# ─── Режим чата ───────────────────────────────────────────────────────────────

def set_group_mode(chat_id: int, enabled: bool):
    with _conn() as con:
        con.execute("""
            INSERT INTO chat_settings (chat_id, group_mode)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
            group_mode = excluded.group_mode,
            updated_at = datetime('now')
        """, (chat_id, 1 if enabled else 0))

def is_group_mode(chat_id: int) -> bool:
    with _conn() as con:
        row = con.execute(
            "SELECT group_mode FROM chat_settings WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return bool(row[0]) if row and row[0] is not None else False

# ─── Статистика планировщика ──────────────────────────────────────────────────

def update_scheduler_stats(corp_id: str, success: bool, error_count: int = 0, alert_sent: bool = False):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with _conn() as con:
        if success:
            con.execute("""
                INSERT INTO scheduler_stats (corp_id, last_check, last_success, error_count, alert_sent)
                VALUES (?, ?, ?, 0, 0)
                ON CONFLICT(corp_id) DO UPDATE SET
                last_check = excluded.last_check,
                last_success = excluded.last_success,
                error_count = 0,
                alert_sent = 0
            """, (corp_id, now, now))
        else:
            con.execute("""
                INSERT INTO scheduler_stats (corp_id, last_check, error_count, alert_sent)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(corp_id) DO UPDATE SET
                last_check = excluded.last_check,
                error_count = excluded.error_count,
                alert_sent = excluded.alert_sent
            """, (corp_id, now, error_count, 1 if alert_sent else 0))

def get_scheduler_stats() -> list[dict]:
    with _conn() as con:
        rows = con.execute("""
            SELECT corp_id, last_check, last_success, error_count, alert_sent
            FROM scheduler_stats
        """).fetchall()
        return [
            {
                "corp_id": r[0], "last_check": r[1],
                "last_success": r[2], "error_count": r[3], "alert_sent": r[4]
            }
            for r in rows
        ]

def get_corp_error_count(corp_id: str) -> tuple[int, bool]:
    """Возвращает (error_count, alert_sent) для корпуса."""
    with _conn() as con:
        row = con.execute(
            "SELECT error_count, alert_sent FROM scheduler_stats WHERE corp_id = ?",
            (corp_id,)
        ).fetchone()
        return (row[0], bool(row[1])) if row else (0, False)

# ─── KV ───────────────────────────────────────────────────────────────────────

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

def kv_get(key: str) -> str | None:
    with _conn() as con:
        row = con.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

def kv_set(key: str, value: str):
    with _conn() as con:
        con.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
