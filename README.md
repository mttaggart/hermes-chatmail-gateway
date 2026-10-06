# hermes-chatmail-gateway

A gateway plugin for the [Hermes Agent](https://github.com/NousResearch/hermes-agent/) to communicate over [Chatmail](https://github.com/chatmail/core) (Delta Chat).

Users chat with the bot through the [Delta Chat](https://delta.chat) app; messages are relayed to the Hermes agent and responses are sent back as Delta Chat messages.

## How it works

```
User's Delta Chat app  ←→  Chatmail server (IMAP/SMTP)  ←→  deltachat-rpc-server  ←→  ChatmailAdapter  ←→  Hermes Agent
```

The adapter uses the [deltachat2](https://github.com/adbenitez/deltachat2) Python library, which communicates with `deltachat-rpc-server` (the Delta Chat core) over JSON-RPC. The bot event loop runs in a background thread, bridging synchronous Delta Chat events to the async Hermes gateway via an `asyncio.Queue`.

## Installation

### 1. Install dependencies

```bash
pip install 'deltachat2[full]'
```

This installs both the `deltachat2` client library and the `deltachat-rpc-server` binary.

### 2. Install the plugin

Copy `plugin.yaml` and `adapter.py` into your Hermes plugins directory:

```bash
mkdir -p ~/.hermes/plugins/chatmail
cp plugin.yaml adapter.py ~/.hermes/plugins/chatmail/
```

### 3. Configure the bot account

Create a chatmail account (e.g. at [nine.testrun.org](https://nine.testrun.org/new)) and set the credentials as environment variables in `~/.hermes/.env`:

```env
CHATMAIL_ADDR=hermes-bot@nine.testrun.org
CHATMAIL_PASSWORD=your-password
```

Or run the interactive setup wizard:

```bash
hermes gateway setup
```

### 4. Start the gateway

```bash
hermes gateway start
```

On first startup, the adapter will:
1. Start the `deltachat-rpc-server` subprocess
2. Create/configure the Delta Chat account using your email address and password
3. Begin listening for incoming messages

### 5. Chat with the bot

Open Delta Chat on your phone, scan the bot's invite QR code (printed on startup), and start chatting!

## Configuration

### Required environment variables

| Variable | Description |
|----------|-------------|
| `CHATMAIL_ADDR` | Bot email address (e.g. `hermes-bot@nine.testrun.org`) |
| `CHATMAIL_PASSWORD` | Bot email/IMAP password |

### Optional environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CHATMAIL_ACCOUNTS_DIR` | `~/.hermes/chatmail-accounts` | Directory for the Delta Chat account database |
| `CHATMAIL_RPC_EXECUTABLE` | `deltachat-rpc-server` (PATH) | Path to the RPC server binary |
| `CHATMAIL_ALLOWED_USERS` | (empty = deny all) | Comma-separated email addresses allowed to talk to the bot |
| `CHATMAIL_ALLOW_ALL_USERS` | `false` | Set to `true` to allow anyone (dev only) |
| `CHATMAIL_HOME_CHANNEL` | (empty) | Default chat ID for cron / notification delivery |

### config.yaml

You can also configure the plugin in `~/.hermes/config.yaml`:

```yaml
gateway:
  chatmail:
    addr: hermes-bot@nine.testrun.org
    password: your-password
```

## Architecture

The adapter follows the [Hermes Plugin Path](https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters):

- **`plugin.yaml`** — Plugin metadata, env var definitions for the setup wizard
- **`adapter.py`** — `ChatmailAdapter` class extending `BasePlatformAdapter`, plus `register()` entry point

### Key components

- **`ChatmailAdapter.connect()`** — Starts `deltachat-rpc-server`, configures the account, launches the bot event loop in a daemon thread, starts an async drain task
- **`ChatmailAdapter._on_new_message()`** — Delta Chat `NewMessage` hook; filters self/info messages, deduplicates, pushes to async queue via `call_soon_threadsafe`
- **`ChatmailAdapter._drain_inbound()`** — Async task that drains the queue and dispatches `MessageEvent`s to the Hermes gateway
- **`ChatmailAdapter.send()`** — Sends a text message via `rpc.send_msg()`, wrapped in `asyncio.to_thread()` to avoid blocking the event loop
- **`_standalone_send()`** — Ephemeral connection for cron delivery outside the gateway process

## Testing

```bash
pip install -e ".[test]"
pytest tests/ -v
```

Tests use mocked Hermes gateway modules and mocked deltachat2 library, so they run without the full Hermes source tree or `deltachat-rpc-server` installed.

## License

MIT