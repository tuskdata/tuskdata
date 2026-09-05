# Notifications

![Settings → Notifications — channels, subscriptions, history.](../screenshots/notifications.png){ .screenshot }

Tusk raises events — a scheduled job failed, a backup finished, the schema
changed, a contract broke, a user was created — and you decide which of
them reach you and where. The bell in the header shows the in-app feed;
**Settings → Notifications** configures the rest.

## Channels

| Type | Needs |
|---|---|
| **Slack** / **Discord** | An incoming-webhook URL. |
| **Telegram** | Bot token + chat id. |
| **Email (SMTP)** | Host, port, credentials, from/to addresses. |
| **Custom webhook** | Any URL; receives the event as JSON (`event`, `title`, `message`, `variant`, `context`, `timestamp`). |

Every channel has a **Test** button that sends a sample message before you
wire it to anything.

Webhook and Slack/Discord URLs pointing at private networks are refused
unless `TUSK_ALLOW_PRIVATE_WEBHOOKS=1` is set — a self-hosted Tusk should
not be usable as a proxy into its own LAN.

## Subscriptions

A subscription is *event → channel*. Events are grouped by origin:

- `scheduler.job.success` / `scheduler.job.error` — [Scheduled](scheduled.md)
- `core.backup.completed` / `core.backup.failed`
- `core.download.completed` / `core.download.failed` — open-data downloads
- `schema.changed` — [Schema Watch](schema-watch.md)
- `contract.violated` / `contract.restored` — [Data Contracts](data-contracts.md)
- `core.user.created`
- events registered by plugins (Analytics: dashboard refresh)

Unsubscribed events still land in the in-app feed. Noisy events are
rate-limited per event key, so a flapping check does not page you every
minute.

## History

**Notification History** lists what was sent, to which channel, when, and
whether delivery succeeded — the first place to look when "the Slack
message never arrived".
