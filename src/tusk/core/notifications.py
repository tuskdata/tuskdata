"""Notification system for Tusk.

Provides multi-channel notifications (email, Slack, Discord, Telegram, webhook)
with rate limiting, retry, and in-app notifications. Any plugin can register
events and send notifications through configured channels.

Usage:
    from tusk.core.notifications import get_notification_service

    svc = get_notification_service()
    svc.register_event("ci.pipeline.failed", "tusk-ci", "Pipeline Failed", "A pipeline run has failed")
    svc.send("ci.pipeline.failed", "Pipeline 'deploy-prod' failed at stage 'test'", context={...})
"""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

import msgspec

from tusk.core.logging import get_logger

log = get_logger("notifications")

# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────


class NotificationChannel(msgspec.Struct):
    id: str
    name: str
    channel_type: str  # email, slack, discord, telegram, webhook
    config: dict[str, Any]
    enabled: bool = True
    created_at: float = 0.0


class NotificationEvent(msgspec.Struct):
    id: str
    event_key: str  # e.g., "ci.pipeline.failed"
    plugin_id: str  # e.g., "tusk-ci" or "core"
    label: str
    description: str = ""


class NotificationSubscription(msgspec.Struct):
    event_key: str
    channel_id: str
    enabled: bool = True


class NotificationHistory(msgspec.Struct):
    id: str
    event_key: str
    channel_id: str
    channel_name: str = ""
    status: str = "pending"  # pending, sent, failed
    message: str = ""
    context: dict[str, Any] | None = None
    error: str | None = None
    attempt: int = 1
    created_at: float = 0.0


class InAppNotification(msgspec.Struct):
    id: str
    user_id: str = ""  # empty = broadcast to all
    event_key: str = ""
    title: str = ""
    message: str = ""
    icon: str = "bell"
    variant: str = "info"  # info, success, warning, error
    link: str = ""
    read: bool = False
    created_at: float = 0.0


# ─────────────────────────────────────────────────────────────
# Channel senders
# ─────────────────────────────────────────────────────────────


def _send_email(config: dict, subject: str, body: str) -> None:
    """Send notification via SMTP."""
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(body)
    msg["Subject"] = f"[Tusk] {subject}"
    msg["From"] = config.get("from_addr", "tusk@localhost")
    msg["To"] = config.get("to_addr", config.get("from_addr", ""))

    host = config.get("host", "localhost")
    port = int(config.get("port", 587))
    use_tls = config.get("tls", True)

    with smtplib.SMTP(host, port, timeout=10) as smtp:
        if use_tls:
            smtp.starttls()
        username = config.get("username")
        password = config.get("password")
        if username and password:
            smtp.login(username, password)
        smtp.send_message(msg)


def _send_slack(config: dict, subject: str, body: str) -> None:
    """Send notification via Slack incoming webhook."""
    import httpx

    url = config.get("webhook_url", "")
    if not url:
        raise ValueError("Slack webhook_url not configured")

    payload = {
        "text": f"*{subject}*\n{body}",
    }
    resp = httpx.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _send_discord(config: dict, subject: str, body: str) -> None:
    """Send notification via Discord webhook."""
    import httpx

    url = config.get("webhook_url", "")
    if not url:
        raise ValueError("Discord webhook_url not configured")

    payload = {
        "embeds": [{
            "title": subject,
            "description": body,
            "color": 0x5865F2,  # Discord blurple
        }],
    }
    resp = httpx.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _send_telegram(config: dict, subject: str, body: str) -> None:
    """Send notification via Telegram Bot API."""
    import httpx

    bot_token = config.get("bot_token", "")
    chat_id = config.get("chat_id", "")
    if not bot_token or not chat_id:
        raise ValueError("Telegram bot_token and chat_id required")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": f"<b>{subject}</b>\n{body}",
        "parse_mode": "HTML",
    }
    resp = httpx.post(url, json=payload, timeout=10)
    resp.raise_for_status()


def _send_webhook(config: dict, subject: str, body: str) -> None:
    """Send notification via custom webhook."""
    import httpx

    url = config.get("url", "")
    if not url:
        raise ValueError("Webhook url not configured")

    headers = config.get("headers", {})
    payload = {
        "event": subject,
        "message": body,
        "timestamp": time.time(),
    }
    resp = httpx.post(url, json=payload, headers=headers, timeout=10)
    resp.raise_for_status()


_SENDERS = {
    "email": _send_email,
    "slack": _send_slack,
    "discord": _send_discord,
    "telegram": _send_telegram,
    "webhook": _send_webhook,
}


# ─────────────────────────────────────────────────────────────
# Notification service
# ─────────────────────────────────────────────────────────────


class NotificationService:
    """Singleton notification service with SQLite persistence."""

    _instance: "NotificationService | None" = None

    def __init__(self, db_path: Path | None = None):
        if db_path is None:
            db_path = Path.home() / ".tusk" / "notifications.db"
        self._db_path = db_path
        self._rate_limit: dict[str, float] = {}  # event_key -> last_sent_ts
        self._rate_limit_seconds = 60  # min seconds between same event
        self._max_retries = 3
        self._init_db()

    @classmethod
    def get_instance(cls, db_path: Path | None = None) -> "NotificationService":
        if cls._instance is None:
            cls._instance = cls(db_path)
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (for tests)."""
        cls._instance = None

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS notification_channels (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    channel_type TEXT NOT NULL,
                    config TEXT NOT NULL DEFAULT '{}',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS notification_events (
                    id TEXT PRIMARY KEY,
                    event_key TEXT NOT NULL UNIQUE,
                    plugin_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS notification_subscriptions (
                    event_key TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (event_key, channel_id),
                    FOREIGN KEY (channel_id) REFERENCES notification_channels(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS notification_history (
                    id TEXT PRIMARY KEY,
                    event_key TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    channel_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    message TEXT NOT NULL DEFAULT '',
                    context TEXT,
                    error TEXT,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS in_app_notifications (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL DEFAULT '',
                    event_key TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    icon TEXT NOT NULL DEFAULT 'bell',
                    variant TEXT NOT NULL DEFAULT 'info',
                    link TEXT NOT NULL DEFAULT '',
                    read INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_history_created
                    ON notification_history(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_inapp_user_read
                    ON in_app_notifications(user_id, read, created_at DESC);
            """)
            conn.commit()
        finally:
            conn.close()

    # ── Channel CRUD ──────────────────────────────────────────

    def create_channel(self, name: str, channel_type: str, config: dict, enabled: bool = True) -> NotificationChannel:
        if channel_type not in _SENDERS:
            raise ValueError(f"Unsupported channel type: {channel_type}. Must be one of: {', '.join(_SENDERS)}")
        ch = NotificationChannel(
            id=uuid.uuid4().hex[:12],
            name=name,
            channel_type=channel_type,
            config=config,
            enabled=enabled,
            created_at=time.time(),
        )
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT INTO notification_channels (id, name, channel_type, config, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (ch.id, ch.name, ch.channel_type, json.dumps(ch.config), 1 if ch.enabled else 0, ch.created_at),
            )
            conn.commit()
        finally:
            conn.close()
        return ch

    def get_channel(self, channel_id: str) -> NotificationChannel | None:
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT * FROM notification_channels WHERE id = ?", (channel_id,)).fetchone()
            if not row:
                return None
            return NotificationChannel(
                id=row["id"], name=row["name"], channel_type=row["channel_type"],
                config=json.loads(row["config"]), enabled=bool(row["enabled"]),
                created_at=row["created_at"],
            )
        finally:
            conn.close()

    def list_channels(self) -> list[NotificationChannel]:
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT * FROM notification_channels ORDER BY created_at DESC").fetchall()
            return [
                NotificationChannel(
                    id=r["id"], name=r["name"], channel_type=r["channel_type"],
                    config=json.loads(r["config"]), enabled=bool(r["enabled"]),
                    created_at=r["created_at"],
                )
                for r in rows
            ]
        finally:
            conn.close()

    def update_channel(self, channel_id: str, *, name: str | None = None, config: dict | None = None, enabled: bool | None = None) -> bool:
        parts, params = [], []
        if name is not None:
            parts.append("name = ?")
            params.append(name)
        if config is not None:
            parts.append("config = ?")
            params.append(json.dumps(config))
        if enabled is not None:
            parts.append("enabled = ?")
            params.append(1 if enabled else 0)
        if not parts:
            return False
        params.append(channel_id)
        conn = self._get_conn()
        try:
            cur = conn.execute(f"UPDATE notification_channels SET {', '.join(parts)} WHERE id = ?", params)
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def delete_channel(self, channel_id: str) -> bool:
        conn = self._get_conn()
        try:
            cur = conn.execute("DELETE FROM notification_channels WHERE id = ?", (channel_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def test_channel(self, channel_id: str) -> tuple[bool, str]:
        """Send a test message through a channel. Returns (success, message)."""
        ch = self.get_channel(channel_id)
        if not ch:
            return False, "Channel not found"
        sender = _SENDERS.get(ch.channel_type)
        if not sender:
            return False, f"No sender for type: {ch.channel_type}"
        try:
            sender(ch.config, "Tusk Test Notification", "This is a test notification from Tusk Studio.")
            return True, "Test sent successfully"
        except Exception as e:
            return False, str(e)

    # ── Event registration ────────────────────────────────────

    def register_event(self, event_key: str, plugin_id: str, label: str, description: str = "") -> None:
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO notification_events (id, event_key, plugin_id, label, description)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(event_key) DO UPDATE SET plugin_id=excluded.plugin_id, label=excluded.label, description=excluded.description""",
                (uuid.uuid4().hex[:12], event_key, plugin_id, label, description),
            )
            conn.commit()
        finally:
            conn.close()

    def list_events(self) -> list[NotificationEvent]:
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT * FROM notification_events ORDER BY plugin_id, event_key").fetchall()
            return [
                NotificationEvent(id=r["id"], event_key=r["event_key"], plugin_id=r["plugin_id"], label=r["label"], description=r["description"])
                for r in rows
            ]
        finally:
            conn.close()

    # ── Subscriptions ─────────────────────────────────────────

    def subscribe(self, event_key: str, channel_id: str, enabled: bool = True) -> None:
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO notification_subscriptions (event_key, channel_id, enabled)
                   VALUES (?, ?, ?)
                   ON CONFLICT(event_key, channel_id) DO UPDATE SET enabled=excluded.enabled""",
                (event_key, channel_id, 1 if enabled else 0),
            )
            conn.commit()
        finally:
            conn.close()

    def unsubscribe(self, event_key: str, channel_id: str) -> None:
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM notification_subscriptions WHERE event_key = ? AND channel_id = ?", (event_key, channel_id))
            conn.commit()
        finally:
            conn.close()

    def get_subscriptions(self, event_key: str | None = None, channel_id: str | None = None) -> list[NotificationSubscription]:
        conn = self._get_conn()
        try:
            sql = "SELECT * FROM notification_subscriptions WHERE 1=1"
            params: list[Any] = []
            if event_key:
                sql += " AND event_key = ?"
                params.append(event_key)
            if channel_id:
                sql += " AND channel_id = ?"
                params.append(channel_id)
            rows = conn.execute(sql, params).fetchall()
            return [
                NotificationSubscription(event_key=r["event_key"], channel_id=r["channel_id"], enabled=bool(r["enabled"]))
                for r in rows
            ]
        finally:
            conn.close()

    def bulk_update_subscriptions(self, subscriptions: list[dict]) -> None:
        """Update subscriptions in bulk. Each dict: {event_key, channel_id, enabled}."""
        conn = self._get_conn()
        try:
            for sub in subscriptions:
                conn.execute(
                    """INSERT INTO notification_subscriptions (event_key, channel_id, enabled)
                       VALUES (?, ?, ?)
                       ON CONFLICT(event_key, channel_id) DO UPDATE SET enabled=excluded.enabled""",
                    (sub["event_key"], sub["channel_id"], 1 if sub.get("enabled", True) else 0),
                )
            conn.commit()
        finally:
            conn.close()

    # ── Send notifications ────────────────────────────────────

    def send(self, event_key: str, message: str, *, context: dict | None = None, title: str | None = None, icon: str = "bell", variant: str = "info", link: str = "") -> int:
        """Send notification for an event. Returns count of channels notified."""
        # Rate limiting
        now = time.time()
        last = self._rate_limit.get(event_key, 0)
        if now - last < self._rate_limit_seconds:
            log.debug("Rate limited", event_key=event_key, seconds_since_last=now - last)
            return 0
        self._rate_limit[event_key] = now

        # Create in-app notification
        self._create_in_app(event_key, title or event_key, message, icon, variant, link)

        # Find subscribed channels
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """SELECT s.channel_id, c.name, c.channel_type, c.config
                   FROM notification_subscriptions s
                   JOIN notification_channels c ON c.id = s.channel_id
                   WHERE s.event_key = ? AND s.enabled = 1 AND c.enabled = 1""",
                (event_key,),
            ).fetchall()
        finally:
            conn.close()

        sent = 0
        for row in rows:
            ch_id = row["channel_id"]
            ch_name = row["name"]
            ch_type = row["channel_type"]
            ch_config = json.loads(row["config"])

            sender = _SENDERS.get(ch_type)
            if not sender:
                continue

            subject = title or event_key
            history_id = uuid.uuid4().hex[:12]

            try:
                sender(ch_config, subject, message)
                self._save_history(history_id, event_key, ch_id, ch_name, "sent", message, context)
                sent += 1
                log.info("Notification sent", event_key=event_key, channel=ch_name, type=ch_type)
            except Exception as e:
                self._save_history(history_id, event_key, ch_id, ch_name, "failed", message, context, error=str(e))
                log.warning("Notification failed", event_key=event_key, channel=ch_name, error=str(e))

        return sent

    def retry_failed(self) -> int:
        """Retry failed notifications (up to max_retries). Returns count retried."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """SELECT h.id, h.event_key, h.channel_id, h.message, h.context, h.attempt,
                          c.name, c.channel_type, c.config
                   FROM notification_history h
                   JOIN notification_channels c ON c.id = h.channel_id
                   WHERE h.status = 'failed' AND h.attempt < ? AND c.enabled = 1
                   ORDER BY h.created_at ASC LIMIT 50""",
                (self._max_retries,),
            ).fetchall()
        finally:
            conn.close()

        retried = 0
        for row in rows:
            sender = _SENDERS.get(row["channel_type"])
            if not sender:
                continue

            ctx = json.loads(row["context"]) if row["context"] else None
            try:
                sender(json.loads(row["config"]), row["event_key"], row["message"])
                self._update_history(row["id"], "sent", attempt=row["attempt"] + 1)
                retried += 1
            except Exception as e:
                self._update_history(row["id"], "failed", error=str(e), attempt=row["attempt"] + 1)

        return retried

    # ── History ───────────────────────────────────────────────

    def get_history(self, limit: int = 50, event_key: str | None = None, status: str | None = None) -> list[NotificationHistory]:
        conn = self._get_conn()
        try:
            sql = "SELECT * FROM notification_history WHERE 1=1"
            params: list[Any] = []
            if event_key:
                sql += " AND event_key = ?"
                params.append(event_key)
            if status:
                sql += " AND status = ?"
                params.append(status)
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            return [
                NotificationHistory(
                    id=r["id"], event_key=r["event_key"], channel_id=r["channel_id"],
                    channel_name=r["channel_name"], status=r["status"], message=r["message"],
                    context=json.loads(r["context"]) if r["context"] else None,
                    error=r["error"], attempt=r["attempt"], created_at=r["created_at"],
                )
                for r in rows
            ]
        finally:
            conn.close()

    def clear_history(self, older_than_days: int = 30) -> int:
        cutoff = time.time() - (older_than_days * 86400)
        conn = self._get_conn()
        try:
            cur = conn.execute("DELETE FROM notification_history WHERE created_at < ?", (cutoff,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def _save_history(self, history_id: str, event_key: str, channel_id: str, channel_name: str, status: str, message: str, context: dict | None, error: str | None = None) -> None:
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO notification_history (id, event_key, channel_id, channel_name, status, message, context, error, attempt, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                (history_id, event_key, channel_id, channel_name, status, message, json.dumps(context) if context else None, error, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def _update_history(self, history_id: str, status: str, error: str | None = None, attempt: int = 1) -> None:
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE notification_history SET status = ?, error = ?, attempt = ? WHERE id = ?",
                (status, error, attempt, history_id),
            )
            conn.commit()
        finally:
            conn.close()

    # ── In-app notifications ──────────────────────────────────

    def _create_in_app(self, event_key: str, title: str, message: str, icon: str = "bell", variant: str = "info", link: str = "", user_id: str = "") -> None:
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT INTO in_app_notifications (id, user_id, event_key, title, message, icon, variant, link, read, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                (uuid.uuid4().hex[:12], user_id, event_key, title, message, icon, variant, link, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def create_in_app(self, title: str, message: str, *, event_key: str = "", icon: str = "bell", variant: str = "info", link: str = "", user_id: str = "") -> None:
        """Public method to create an in-app notification directly."""
        self._create_in_app(event_key, title, message, icon, variant, link, user_id)

    def get_in_app(self, user_id: str = "", unread_only: bool = False, limit: int = 20) -> list[InAppNotification]:
        conn = self._get_conn()
        try:
            sql = "SELECT * FROM in_app_notifications WHERE (user_id = '' OR user_id = ?)"
            params: list[Any] = [user_id]
            if unread_only:
                sql += " AND read = 0"
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            return [
                InAppNotification(
                    id=r["id"], user_id=r["user_id"], event_key=r["event_key"],
                    title=r["title"], message=r["message"], icon=r["icon"],
                    variant=r["variant"], link=r["link"], read=bool(r["read"]),
                    created_at=r["created_at"],
                )
                for r in rows
            ]
        finally:
            conn.close()

    def get_unread_count(self, user_id: str = "") -> int:
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM in_app_notifications WHERE (user_id = '' OR user_id = ?) AND read = 0",
                (user_id,),
            ).fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    def mark_read(self, notification_id: str) -> bool:
        conn = self._get_conn()
        try:
            cur = conn.execute("UPDATE in_app_notifications SET read = 1 WHERE id = ?", (notification_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def mark_all_read(self, user_id: str = "") -> int:
        conn = self._get_conn()
        try:
            cur = conn.execute(
                "UPDATE in_app_notifications SET read = 1 WHERE (user_id = '' OR user_id = ?) AND read = 0",
                (user_id,),
            )
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def delete_in_app(self, notification_id: str) -> bool:
        conn = self._get_conn()
        try:
            cur = conn.execute("DELETE FROM in_app_notifications WHERE id = ?", (notification_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    def clear_in_app(self, older_than_days: int = 7) -> int:
        cutoff = time.time() - (older_than_days * 86400)
        conn = self._get_conn()
        try:
            cur = conn.execute("DELETE FROM in_app_notifications WHERE created_at < ?", (cutoff,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    # ── Core events ───────────────────────────────────────────

    def register_core_events(self) -> None:
        """Register built-in TuskData events."""
        core_events = [
            ("core.backup.completed", "core", "Backup Completed", "A database backup has completed"),
            ("core.backup.failed", "core", "Backup Failed", "A database backup has failed"),
            ("core.user.created", "core", "User Created", "A new user has been created"),
            ("core.download.completed", "core", "Download Completed", "A scheduled download has completed"),
            ("core.download.failed", "core", "Download Failed", "A scheduled download has failed"),
        ]
        for event_key, plugin_id, label, desc in core_events:
            self.register_event(event_key, plugin_id, label, desc)


# ─────────────────────────────────────────────────────────────
# Module-level convenience
# ─────────────────────────────────────────────────────────────

def get_notification_service(db_path: Path | None = None) -> NotificationService:
    """Get the singleton notification service."""
    return NotificationService.get_instance(db_path)
