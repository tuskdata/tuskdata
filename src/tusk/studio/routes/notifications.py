"""Notification routes for Tusk Studio."""

from litestar import Controller, Request, Response, get, post, put, delete
from litestar.params import Body
from litestar.response import Template

from tusk.core.notifications import get_notification_service
from tusk.studio.htmx import is_htmx, htmx_toast, htmx_trigger
from tusk.studio.routes.base import TuskController, get_base_context


class NotificationPageController(TuskController):
    """Page routes for notification settings."""

    path = "/notifications"

    @get("/settings")
    async def settings_page(self, request: Request) -> Template:
        svc = get_notification_service()
        channels = svc.list_channels()
        events = svc.list_events()
        subscriptions = svc.get_subscriptions()
        history = svc.get_history(limit=50)

        # Build subscription matrix
        sub_map: dict[str, dict[str, bool]] = {}
        for sub in subscriptions:
            sub_map.setdefault(sub.event_key, {})[sub.channel_id] = sub.enabled

        ctx = get_base_context(active_page="notifications")
        ctx.update(
            channels=channels,
            events=events,
            subscriptions=sub_map,
            history=history,
        )
        return Template(template_name="notifications_settings.html", context=ctx)


class NotificationAPIController(Controller):
    """API routes for notifications."""

    path = "/api/notifications"

    # ── In-app notifications ──────────────────────────────────

    @get("/")
    async def list_notifications(self, request: Request) -> dict | Template:
        svc = get_notification_service()
        user_id = _get_user_id(request)
        notifications = svc.get_in_app(user_id=user_id, limit=20)
        if is_htmx(request):
            return Template(
                template_name="partials/notification-list.html",
                context={"notifications": notifications},
            )
        return {
            "notifications": [
                {
                    "id": n.id, "title": n.title, "message": n.message,
                    "icon": n.icon, "variant": n.variant, "link": n.link,
                    "read": n.read, "created_at": n.created_at,
                }
                for n in notifications
            ]
        }

    @get("/unread-count")
    async def unread_count(self, request: Request) -> dict:
        svc = get_notification_service()
        user_id = _get_user_id(request)
        count = svc.get_unread_count(user_id=user_id)
        return {"count": count}

    @get("/badge")
    async def badge(self, request: Request) -> Template:
        """HTMX partial for notification bell badge."""
        svc = get_notification_service()
        user_id = _get_user_id(request)
        count = svc.get_unread_count(user_id=user_id)
        notifications = svc.get_in_app(user_id=user_id, limit=10) if count > 0 else []
        return Template(
            template_name="partials/notification-bell.html",
            context={"unread_count": count, "notifications": notifications},
        )

    @post("/{notification_id:str}/read")
    async def mark_read(self, notification_id: str, request: Request) -> Response:
        svc = get_notification_service()
        svc.mark_read(notification_id)
        return Response(content="", status_code=200, headers=htmx_trigger("refreshNotifications"))

    @post("/read-all")
    async def mark_all_read(self, request: Request) -> Response:
        svc = get_notification_service()
        user_id = _get_user_id(request)
        svc.mark_all_read(user_id=user_id)
        return Response(content="", status_code=200, headers=htmx_toast("All notifications marked as read", "success"))

    # ── Channels ──────────────────────────────────────────────

    @get("/channels")
    async def list_channels(self, request: Request) -> dict:
        svc = get_notification_service()
        channels = svc.list_channels()
        return {
            "channels": [
                {"id": c.id, "name": c.name, "channel_type": c.channel_type, "enabled": c.enabled, "created_at": c.created_at}
                for c in channels
            ]
        }

    @post("/channels")
    async def create_channel(self, request: Request, data: dict = Body()) -> Response:
        svc = get_notification_service()
        try:
            ch = svc.create_channel(
                name=data["name"],
                channel_type=data["channel_type"],
                config=data.get("config", {}),
                enabled=data.get("enabled", True),
            )
            return Response(
                content={"id": ch.id, "name": ch.name},
                status_code=201,
                headers=htmx_toast(f"Channel '{ch.name}' created", "success"),
            )
        except (ValueError, KeyError) as e:
            return Response(content={"error": str(e)}, status_code=400)

    @put("/channels/{channel_id:str}")
    async def update_channel(self, channel_id: str, request: Request, data: dict = Body()) -> Response:
        svc = get_notification_service()
        updated = svc.update_channel(
            channel_id,
            name=data.get("name"),
            config=data.get("config"),
            enabled=data.get("enabled"),
        )
        if not updated:
            return Response(content={"error": "Channel not found"}, status_code=404)
        return Response(content="", status_code=200, headers=htmx_toast("Channel updated", "success"))

    @delete("/channels/{channel_id:str}", status_code=200)
    async def delete_channel(self, channel_id: str, request: Request) -> Response:
        svc = get_notification_service()
        deleted = svc.delete_channel(channel_id)
        if not deleted:
            return Response(content={"error": "Channel not found"}, status_code=404)
        return Response(content="", status_code=200, headers=htmx_toast("Channel deleted", "success"))

    @post("/channels/{channel_id:str}/test")
    async def test_channel(self, channel_id: str, request: Request) -> Response:
        svc = get_notification_service()
        success, msg = svc.test_channel(channel_id)
        if success:
            return Response(content={"message": msg}, status_code=200, headers=htmx_toast(msg, "success"))
        return Response(content={"error": msg}, status_code=400, headers=htmx_toast(msg, "error"))

    # ── Events & Subscriptions ────────────────────────────────

    @get("/events")
    async def list_events(self, request: Request) -> dict:
        svc = get_notification_service()
        events = svc.list_events()
        return {
            "events": [
                {"event_key": e.event_key, "plugin_id": e.plugin_id, "label": e.label, "description": e.description}
                for e in events
            ]
        }

    @get("/subscriptions")
    async def list_subscriptions(self, request: Request) -> dict:
        svc = get_notification_service()
        subs = svc.get_subscriptions()
        return {
            "subscriptions": [
                {"event_key": s.event_key, "channel_id": s.channel_id, "enabled": s.enabled}
                for s in subs
            ]
        }

    @post("/subscriptions")
    async def update_subscriptions(self, request: Request, data: dict = Body()) -> Response:
        svc = get_notification_service()
        subs = data.get("subscriptions", [])
        svc.bulk_update_subscriptions(subs)
        return Response(content="", status_code=200, headers=htmx_toast("Subscriptions updated", "success"))

    # ── History ───────────────────────────────────────────────

    @get("/history")
    async def get_history(self, request: Request) -> dict:
        svc = get_notification_service()
        limit = int(request.query_params.get("limit", "50"))
        event_key = request.query_params.get("event_key")
        status = request.query_params.get("status")
        history = svc.get_history(limit=limit, event_key=event_key, status=status)
        return {
            "history": [
                {
                    "id": h.id, "event_key": h.event_key, "channel_name": h.channel_name,
                    "status": h.status, "message": h.message, "error": h.error,
                    "attempt": h.attempt, "created_at": h.created_at,
                }
                for h in history
            ]
        }

    @post("/history/clear")
    async def clear_history(self, request: Request) -> Response:
        svc = get_notification_service()
        cleared = svc.clear_history(older_than_days=30)
        return Response(content={"cleared": cleared}, status_code=200, headers=htmx_toast(f"Cleared {cleared} history entries", "success"))


def _get_user_id(request: Request) -> str:
    """Extract user ID from session cookie, or empty string."""
    try:
        from tusk.core.auth import get_session
        session_id = request.cookies.get("tusk_session")
        if session_id:
            session = get_session(session_id)
            if session:
                return session.get("user_id", "")
    except Exception:
        pass
    return ""
