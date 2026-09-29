# EFB Linux WeChat Slave

This package is the Session C derivative of `efb-wechat-comwechat-slave`. It keeps the upstream EFB channel/chat/message-adapter design, but removes the Windows ComWechatRobot/Hook backend and talks only to **WeChat Core Interface Contract V1** over HTTP.

The implementation also uses legacy `efb-wechat-slave` as the Telegram UX compatibility reference and is tested with the user-selected `kettly1260/efb-telegram-master` source tree.

## What changed from ComWechat

- one EFB slave can expose multiple Core WeChat accounts;
- every EFB chat UID reversibly encodes both `account_id` and opaque Core `chat_id`;
- `/link` receives real `get_chats()` results instead of the upstream ComWechat empty list;
- durable Core event polling replaces Hook callbacks, TTL delivery cache and global file-pending dictionaries;
- Core media is downloaded through `/v1/media/{media_id}`;
- text/image/file sends use `/v1/send/*` with idempotency keys;
- EFB reply targets map to Core `target_message_id`;
- when Core advertises that its concrete sender has no verified native reply,
  text replies fall back to the old-EWS visible quote instead of entering a
  doomed outbox request;
- Core `message.removed` maps to EFB `MessageRemoval`;
- active channel state is per instance, not class-global.

No Core SQLite database is opened by this package.

## EFB profile configuration

Copy `config-example.yaml` to the EFB module configuration path for `wechat.linux` (or its instance ID):

```yaml
core:
  base_url: http://127.0.0.1:8080
  timeout: 10
  poll_timeout: 15
  verify_tls: true

account_ids: []
consumer_id: efb-linux-wechat
poll_interval: 1.0
event_limit: 50
startup_healthcheck: true
delivery_mode: fast          # or "ordered"
ordered_scope: all         # or "media"
ordered_max_wait_sec: 20
send_reactions:              # react on your Telegram message after a send
  text: false
  image: false
  file: false
  sent: "👌"
  uncertain: "🤔"
  failed: "👎"
login_alert: true            # post a login button when an account logs out
```

`login_alert` watches Core `account.status` events. When an account changes to `login_required`, the bot posts a message in a per-account system chat ("<account> · 登录状态") with a **发起登录** button. The button asks Core to start the Runtime login flow: WeChat shows the phone confirmation, or, if a QR login is needed, the QR code is sent to Telegram. When the account is back `online`, a short note follows. The button uses EFB message commands, so it only works while the slave that posted it is running.

`account_ids: []` exposes all configured Core accounts. If several independent EFB slave instances are desired, give each instance an explicit non-overlapping account list and its own EFB instance ID.

`delivery_mode` picks between speed and order within a chat. `fast` (default) forwards each message as soon as it is ready, so a text can arrive before an image or video sent just before it that is still downloading. `ordered` holds later messages of the same chat until the earlier media is delivered, keeping the original order; a pending media blocks the chat for at most `ordered_max_wait_sec` seconds (default 20), after which later messages go through and the media follows when ready. Other chats are never held. With `ordered_scope: media` only images, videos and files keep their order among themselves and text is forwarded immediately.
`send_reactions` confirms messages you send from Telegram. When Core reports the outcome of a send, the bot reacts on your Telegram message: `sent` once WeChat shows the message (echo reconciled), `uncertain` when WeChat did not confirm it in time, `failed` when the send failed. Switch it on per kind (`text`, `image`, `file`): `true` reacts to every outcome, `problems` only marks uncertain and failed sends (the mark is removed if the send is confirmed later), `false` disables it. An empty emoji disables that outcome. Only Telegram's standard reaction emoji can be used (for example 👌 👍 🤔 👎; ✅ is not available).

A message sent from Telegram can reach WeChat, and come back from Core as an outgoing message, before Core has linked it to the send. Outgoing messages in a chat that still has an unconfirmed send are therefore held for up to `send_echo_wait_sec` (default 20) seconds, so the echo is recognised instead of being delivered to Telegram a second time.

The EFB profile-level `config.yaml` should then select the master/slave modules normally, for example:

```yaml
master_channel: blueset.telegram
slave_channels:
  - wechat.linux
```

## Required Kettly ETM source integration

Development and compatibility testing should use the local fork, not the PyPI master package:

```bash
python -m venv .venv-c
. .venv-c/bin/activate
pip install -e ../../upstream/efb-telegram-master-kettly
pip install -e .
python -c "import efb_telegram_master; print(efb_telegram_master.__file__)"
```

The printed path must resolve inside `upstream/efb-telegram-master-kettly`.

For a Linux host-Python deployment, `sh scripts/setup-host.sh` creates a local
virtual environment and installs both the locked local Kettly source and this
slave editable. `examples/efb-linux-wechat.service` is a systemd template; the
service account still needs a normal EFB profile under its `HOME`.

The host helper defaults to `python3.10` because Kettly ETM currently depends on
the legacy `python-telegram-bot 13.15` / Tornado 6.1 stack. Set `PYTHON_BIN` only
after validating another interpreter against that dependency set.

The repository `Dockerfile` is retained for the stack `efb-multi` service. It
clones the Kettly fork at the exact locked commit during image build instead of
installing a generic master package. The project-level `stack/docker-compose.yml`
owns orchestration; the obsolete ComWechat/Windows package-local compose file
has been removed.

## Core V1 limitations

- Core V1 has no full group-member-list endpoint, so group members are enrolled as normalized message authors are observed.
- Core V1 has no chat/member avatar endpoint, so EFB avatar requests are explicitly unsupported.
- Core V1 has no outgoing recall endpoint. Incoming WeChat recalls are forwarded to Telegram, but Telegram-initiated recall is explicitly unsupported.
- A successful `/v1/send/*` response means Core accepted the send into its outbox; it is not proof of WeChat delivery.
- Optional Core `sender_capabilities` are honored when present. Unsupported
  arbitrary-file sends, non-text replies that cannot preserve target semantics,
  and unsupported image captions are rejected before they are queued. An
  absent capability field remains "unknown" for compatibility with other V1
  Core implementations.

## Development checks

The repository contains unit and Mock Core integration tests. Mock Core is contract simulation only; passing these tests must not be described as a live WeChat or live Telegram end-to-end test.

See `SOURCE_AUDIT_C.md` and `docs/EFB_BEHAVIOR_COMPAT.md` for exact upstream source reuse and compatibility decisions.

## Upstream attribution

Primary source baseline:

- `ehForwarderBot/efb-wechat-comwechat-slave` by honus and contributors, locked by the workspace in `docs/UPSTREAM_LOCK.md`.

Behaviour references:

- `ehForwarderBot/efb-wechat-slave`
- `kettly1260/efb-telegram-master`

The primary upstream `setup.py` declares the project under the MIT License classifier; its locked checkout does not contain a standalone `LICENSE*` file to copy. Existing source attribution and repository history are intentionally retained.
