"""Tests for the notification system."""

import json
import time
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from tusk.core.notifications import (
    NotificationService,
    NotificationChannel,
    NotificationEvent,
    NotificationSubscription,
    NotificationHistory,
    InAppNotification,
    get_notification_service,
    _send_email,
    _send_slack,
    _send_discord,
    _send_telegram,
    _send_webhook,
)


@pytest.fixture(autouse=True)
def _reset_service():
    """Reset singleton and use temp DB for each test."""
    NotificationService.reset()
    yield
    NotificationService.reset()


@pytest.fixture
def svc(tmp_path):
    """Create a notification service with temp DB."""
    db_path = tmp_path / "test_notifications.db"
    return NotificationService.get_instance(db_path)


class TestModels:
    def test_channel_defaults(self):
        ch = NotificationChannel(id="abc", name="Test", channel_type="slack", config={})
        assert ch.enabled is True
        assert ch.created_at == 0.0

    def test_event_defaults(self):
        ev = NotificationEvent(id="abc", event_key="test.event", plugin_id="core", label="Test")
        assert ev.description == ""

    def test_in_app_defaults(self):
        n = InAppNotification(id="abc")
        assert n.user_id == ""
        assert n.icon == "bell"
        assert n.variant == "info"
        assert n.read is False


class TestChannelCRUD:
    def test_create_channel(self, svc):
        ch = svc.create_channel("Slack Team", "slack", {"webhook_url": "https://hooks.slack.com/test"})
        assert ch.name == "Slack Team"
        assert ch.channel_type == "slack"
        assert ch.enabled is True
        assert len(ch.id) == 12

    def test_get_channel(self, svc):
        ch = svc.create_channel("Discord", "discord", {"webhook_url": "https://discord.com/api/webhooks/test"})
        fetched = svc.get_channel(ch.id)
        assert fetched is not None
        assert fetched.name == "Discord"
        assert fetched.config["webhook_url"] == "https://discord.com/api/webhooks/test"

    def test_get_nonexistent_channel(self, svc):
        assert svc.get_channel("nonexistent") is None

    def test_list_channels(self, svc):
        svc.create_channel("Ch1", "slack", {"webhook_url": "https://a.com"})
        svc.create_channel("Ch2", "discord", {"webhook_url": "https://b.com"})
        channels = svc.list_channels()
        assert len(channels) == 2

    def test_update_channel(self, svc):
        ch = svc.create_channel("Old Name", "slack", {"webhook_url": "https://old.com"})
        updated = svc.update_channel(ch.id, name="New Name", enabled=False)
        assert updated is True
        fetched = svc.get_channel(ch.id)
        assert fetched.name == "New Name"
        assert fetched.enabled is False

    def test_update_nonexistent(self, svc):
        assert svc.update_channel("nope", name="x") is False

    def test_delete_channel(self, svc):
        ch = svc.create_channel("To Delete", "webhook", {"url": "https://example.com"})
        assert svc.delete_channel(ch.id) is True
        assert svc.get_channel(ch.id) is None

    def test_delete_nonexistent(self, svc):
        assert svc.delete_channel("nope") is False

    def test_invalid_channel_type(self, svc):
        with pytest.raises(ValueError, match="Unsupported channel type"):
            svc.create_channel("Bad", "fax", {})

    def test_delete_cascades_subscriptions(self, svc):
        ch = svc.create_channel("Cascade", "slack", {"webhook_url": "https://a.com"})
        svc.register_event("test.event", "core", "Test")
        svc.subscribe("test.event", ch.id)
        assert len(svc.get_subscriptions(channel_id=ch.id)) == 1
        svc.delete_channel(ch.id)
        assert len(svc.get_subscriptions(channel_id=ch.id)) == 0


class TestEvents:
    def test_register_event(self, svc):
        svc.register_event("ci.pipeline.failed", "tusk-ci", "Pipeline Failed", "A pipeline failed")
        events = svc.list_events()
        assert len(events) == 1
        assert events[0].event_key == "ci.pipeline.failed"
        assert events[0].plugin_id == "tusk-ci"

    def test_register_event_upsert(self, svc):
        svc.register_event("test.ev", "core", "Old Label")
        svc.register_event("test.ev", "core", "New Label")
        events = svc.list_events()
        assert len(events) == 1
        assert events[0].label == "New Label"

    def test_register_core_events(self, svc):
        svc.register_core_events()
        events = svc.list_events()
        keys = [e.event_key for e in events]
        assert "core.backup.completed" in keys
        assert "core.backup.failed" in keys
        assert "core.user.created" in keys


class TestSubscriptions:
    def test_subscribe(self, svc):
        ch = svc.create_channel("Ch", "slack", {"webhook_url": "https://a.com"})
        svc.register_event("test.event", "core", "Test")
        svc.subscribe("test.event", ch.id)
        subs = svc.get_subscriptions(event_key="test.event")
        assert len(subs) == 1
        assert subs[0].channel_id == ch.id
        assert subs[0].enabled is True

    def test_unsubscribe(self, svc):
        ch = svc.create_channel("Ch", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.event", ch.id)
        svc.unsubscribe("test.event", ch.id)
        assert len(svc.get_subscriptions(event_key="test.event")) == 0

    def test_subscribe_upsert(self, svc):
        ch = svc.create_channel("Ch", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.event", ch.id, enabled=True)
        svc.subscribe("test.event", ch.id, enabled=False)
        subs = svc.get_subscriptions(event_key="test.event")
        assert len(subs) == 1
        assert subs[0].enabled is False

    def test_bulk_update(self, svc):
        ch1 = svc.create_channel("Ch1", "slack", {"webhook_url": "https://a.com"})
        ch2 = svc.create_channel("Ch2", "discord", {"webhook_url": "https://b.com"})
        svc.bulk_update_subscriptions([
            {"event_key": "ev1", "channel_id": ch1.id, "enabled": True},
            {"event_key": "ev1", "channel_id": ch2.id, "enabled": False},
            {"event_key": "ev2", "channel_id": ch1.id, "enabled": True},
        ])
        all_subs = svc.get_subscriptions()
        assert len(all_subs) == 3


def _mock_senders(mock_fn=None, side_effect=None):
    """Patch _SENDERS dict with mock functions."""
    mock = MagicMock(side_effect=side_effect) if side_effect else (mock_fn or MagicMock())
    patched = {k: mock for k in ("email", "slack", "discord", "telegram", "webhook")}
    return patch.dict("tusk.core.notifications._SENDERS", patched), mock


class TestSendNotifications:
    def test_send_with_subscribed_channel(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://hooks.slack.com/test"})
        svc.register_event("test.send", "core", "Test Send")
        svc.subscribe("test.send", ch.id)

        patcher, mock_send = _mock_senders()
        with patcher:
            count = svc.send("test.send", "Hello world", title="Test")
            assert count == 1
            mock_send.assert_called_once()

    def test_send_no_subscriptions(self, svc):
        svc.register_event("test.nosub", "core", "No Sub")
        count = svc.send("test.nosub", "Nobody listens")
        assert count == 0

    def test_send_disabled_channel(self, svc):
        ch = svc.create_channel("Disabled", "slack", {"webhook_url": "https://a.com"}, enabled=False)
        svc.subscribe("test.disabled", ch.id)

        patcher, mock_send = _mock_senders()
        with patcher:
            count = svc.send("test.disabled", "Should not send")
            assert count == 0
            mock_send.assert_not_called()

    def test_send_rate_limited(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.rate", ch.id)
        svc._rate_limit_seconds = 60

        patcher, _ = _mock_senders()
        with patcher:
            count1 = svc.send("test.rate", "First")
            assert count1 == 1
            count2 = svc.send("test.rate", "Second (should be rate limited)")
            assert count2 == 0

    def test_send_failure_recorded_in_history(self, svc):
        ch = svc.create_channel("Bad", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.fail", ch.id)

        patcher, _ = _mock_senders(side_effect=Exception("Connection refused"))
        with patcher:
            count = svc.send("test.fail", "Will fail")
            assert count == 0

        history = svc.get_history(event_key="test.fail")
        assert len(history) == 1
        assert history[0].status == "failed"
        assert "Connection refused" in history[0].error

    def test_send_creates_in_app(self, svc):
        svc.send("test.inapp", "In-app test", title="Hello", icon="star", variant="success")
        notifications = svc.get_in_app()
        assert len(notifications) == 1
        assert notifications[0].title == "Hello"
        assert notifications[0].icon == "star"
        assert notifications[0].variant == "success"


class TestRetry:
    def test_retry_failed(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.retry", ch.id)

        # First send fails
        p1, _ = _mock_senders(side_effect=Exception("Timeout"))
        with p1:
            svc.send("test.retry", "Will fail first")

        # Retry succeeds
        p2, _ = _mock_senders()
        with p2:
            retried = svc.retry_failed()
            assert retried == 1

        history = svc.get_history(event_key="test.retry")
        sent = [h for h in history if h.status == "sent"]
        assert len(sent) == 1
        assert sent[0].attempt == 2

    def test_retry_max_attempts(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.maxretry", ch.id)
        svc._max_retries = 2

        # First send fails
        p1, _ = _mock_senders(side_effect=Exception("Timeout"))
        with p1:
            svc.send("test.maxretry", "Will fail")

        # Retry also fails (attempt 2)
        p2, _ = _mock_senders(side_effect=Exception("Timeout"))
        with p2:
            svc.retry_failed()

        # Attempt 3 - should not retry since max_retries=2
        p3, _ = _mock_senders()
        with p3:
            retried = svc.retry_failed()
            assert retried == 0


class TestHistory:
    def test_get_history(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.hist", ch.id)

        p, _ = _mock_senders()
        with p:
            svc.send("test.hist", "Message 1")
            svc._rate_limit.clear()  # bypass rate limit
            svc.send("test.hist", "Message 2")

        history = svc.get_history()
        assert len(history) == 2

    def test_filter_by_status(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.filter", ch.id)

        p1, _ = _mock_senders()
        with p1:
            svc.send("test.filter", "Success")
        svc._rate_limit.clear()
        p2, _ = _mock_senders(side_effect=Exception("Fail"))
        with p2:
            svc.send("test.filter", "Failure")

        sent = svc.get_history(status="sent")
        failed = svc.get_history(status="failed")
        assert len(sent) == 1
        assert len(failed) == 1

    def test_clear_history(self, svc):
        ch = svc.create_channel("Ch", "slack", {"webhook_url": "https://a.com"})
        svc.subscribe("test.clear", ch.id)

        with patch("tusk.core.notifications._send_slack"):
            svc.send("test.clear", "Old message")

        # Clear with 0 days = everything
        cleared = svc.clear_history(older_than_days=0)
        assert cleared == 1
        assert len(svc.get_history()) == 0


class TestInAppNotifications:
    def test_create_and_get(self, svc):
        svc.create_in_app("Title", "Body", event_key="test.ev", icon="star", variant="warning")
        notifications = svc.get_in_app()
        assert len(notifications) == 1
        assert notifications[0].title == "Title"
        assert notifications[0].message == "Body"
        assert notifications[0].read is False

    def test_unread_count(self, svc):
        svc.create_in_app("N1", "Body1")
        svc.create_in_app("N2", "Body2")
        assert svc.get_unread_count() == 2

    def test_mark_read(self, svc):
        svc.create_in_app("N1", "Body")
        n = svc.get_in_app()[0]
        assert svc.mark_read(n.id) is True
        assert svc.get_unread_count() == 0

    def test_mark_all_read(self, svc):
        svc.create_in_app("N1", "B1")
        svc.create_in_app("N2", "B2")
        svc.create_in_app("N3", "B3")
        marked = svc.mark_all_read()
        assert marked == 3
        assert svc.get_unread_count() == 0

    def test_unread_only_filter(self, svc):
        svc.create_in_app("N1", "B1")
        svc.create_in_app("N2", "B2")
        n = svc.get_in_app()[0]
        svc.mark_read(n.id)
        unread = svc.get_in_app(unread_only=True)
        assert len(unread) == 1

    def test_delete_in_app(self, svc):
        svc.create_in_app("N1", "Body")
        n = svc.get_in_app()[0]
        assert svc.delete_in_app(n.id) is True
        assert len(svc.get_in_app()) == 0

    def test_clear_old(self, svc):
        svc.create_in_app("Old", "Body")
        cleared = svc.clear_in_app(older_than_days=0)
        assert cleared == 1

    def test_user_scoped(self, svc):
        svc.create_in_app("Global", "For everyone")
        svc.create_in_app("User1 Only", "Private", user_id="user1")
        svc.create_in_app("User2 Only", "Private", user_id="user2")

        # User1 sees global + their own
        u1 = svc.get_in_app(user_id="user1")
        assert len(u1) == 2
        titles = {n.title for n in u1}
        assert "Global" in titles
        assert "User1 Only" in titles

        # User2 sees global + their own
        u2 = svc.get_in_app(user_id="user2")
        assert len(u2) == 2


class TestChannelTest:
    def test_test_channel_success(self, svc):
        ch = svc.create_channel("Slack", "slack", {"webhook_url": "https://a.com"})
        p, _ = _mock_senders()
        with p:
            success, msg = svc.test_channel(ch.id)
            assert success is True

    def test_test_channel_failure(self, svc):
        ch = svc.create_channel("Bad", "slack", {"webhook_url": "https://a.com"})
        p, _ = _mock_senders(side_effect=Exception("Connection refused"))
        with p:
            success, msg = svc.test_channel(ch.id)
            assert success is False
            assert "Connection refused" in msg

    def test_test_nonexistent(self, svc):
        success, msg = svc.test_channel("nope")
        assert success is False
        assert "not found" in msg


class TestSenders:
    def test_slack_sender(self):
        with patch("httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            _send_slack({"webhook_url": "https://hooks.slack.com/test"}, "Subject", "Body")
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[0][0] == "https://hooks.slack.com/test"

    def test_slack_no_url(self):
        with pytest.raises(ValueError, match="webhook_url"):
            _send_slack({}, "Subject", "Body")

    def test_discord_sender(self):
        with patch("httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            _send_discord({"webhook_url": "https://discord.com/api/webhooks/test"}, "Subject", "Body")
            mock_post.assert_called_once()

    def test_telegram_sender(self):
        with patch("httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            _send_telegram({"bot_token": "123:ABC", "chat_id": "-100123"}, "Subject", "Body")
            mock_post.assert_called_once()
            call_url = mock_post.call_args[0][0]
            assert "123:ABC" in call_url

    def test_telegram_missing_config(self):
        with pytest.raises(ValueError, match="bot_token and chat_id"):
            _send_telegram({}, "Subject", "Body")

    def test_webhook_sender(self):
        with patch("httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            _send_webhook({"url": "https://example.com/hook", "headers": {"X-Token": "abc"}}, "Subject", "Body")
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs[1]["headers"] == {"X-Token": "abc"}

    def test_webhook_no_url(self):
        with pytest.raises(ValueError, match="url"):
            _send_webhook({}, "Subject", "Body")

    def test_email_sender(self):
        with patch("smtplib.SMTP") as mock_smtp_class:
            mock_smtp = MagicMock()
            mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)
            _send_email({
                "host": "smtp.test.com",
                "port": 587,
                "username": "user",
                "password": "pass",
                "from_addr": "from@test.com",
                "to_addr": "to@test.com",
                "tls": True,
            }, "Subject", "Body")
            mock_smtp.starttls.assert_called_once()
            mock_smtp.login.assert_called_once_with("user", "pass")
            mock_smtp.send_message.assert_called_once()


class TestSingleton:
    def test_get_instance(self, tmp_path):
        NotificationService.reset()
        db = tmp_path / "singleton.db"
        svc1 = NotificationService.get_instance(db)
        svc2 = NotificationService.get_instance()
        assert svc1 is svc2

    def test_module_level_helper(self, tmp_path):
        NotificationService.reset()
        db = tmp_path / "helper.db"
        svc = get_notification_service(db)
        assert isinstance(svc, NotificationService)
