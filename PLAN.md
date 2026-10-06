# Hermes Chatmail Gateway — Implementation Plan

## Summary

Build a standalone Python package (`hermes-chatmail-gateway`) that bridges the [Hermes Agent](https://github.com/NousResearch/hermes-agent) to [Delta Chat](https://delta.chat) / chatmail using the **deltachat2** (JSON-RPC) library. The Hermes agent source will be available as a git submodule in this repo for interface reference. A real chatmail test account is available for integration testing.

## Key Decisions (Locked)

| Decision | Choice |
|---|---|
| Dev layout | Git submodule of hermes-agent in `vendor/hermes-agent/` for interface reference; plugin lives in this repo's top-level package |
| Delta Chat library | `deltachat2` (JSON-RPC client wrapping `deltachat-rpc-server`) |
| Test environment | Real chatmail server account available (addr + app-password via env vars) |

## Assumptions (to verify against Hermes source once submodule is cloned)

These are stated explicitly so they can be confirmed before implementation. If the Hermes interface differs, the adapter code adjusts accordingly — the plan structure stays the same.

1. **Gateway base class**: Hermes exposes a base class or protocol (assumed `BaseGateway` or similar) with at least:
   - An async `start()` lifecycle method
   - An async `stop()` / cleanup method
   - A callback or method to deliver received messages to the agent (assumed `on_message(message)`)
   - A method the agent calls to send replies back through the gateway (assumed `send_message(chat_id, text)`)
2. **Registration**: Gateways are registered via Python entry points (`pyproject.toml` `[project.entry-points]`) or a Hermes config file. Assumed entry-point group `hermes.gateways`.
3. **Message model**: Hermes has a message dataclass/dict with fields like `text`, `sender`, `chat_id`, `attachments`. The adapter maps Delta Chat message snapshots to this model and back.

---

## Step 1 — Project scaffolding & Hermes submodule

- Add `hermes-agent` as a git submodule: `git submodule add https://github.com/NousResearch/hermes-agent.git vendor/hermes-agent`
- Read `vendor/hermes-agent` source to confirm the actual gateway interface (base class name, required methods, registration mechanism, message model). Adjust the assumptions above.
- Create `pyproject.toml` for the standalone package:
  - Name: `hermes-chatmail-gateway`
  - Python ≥ 3.10
  - Dependencies: `deltachat2`, `hermes-agent` (path or PyPI), `pydantic` (if Hermes uses it)
  - Entry point: `hermes.gateways.chatmail = hermes_chatmail_gateway.gateway:ChatmailGateway`
  - Dev dependencies: `pytest`, `pytest-asyncio`, `pytest-mock`
- Package layout:
  ```
  hermes_chatmail_gateway/
    __init__.py
    gateway.py        # ChatmailGateway — the Hermes adapter
    client.py         # Delta Chat RPC client wrapper (account lifecycle, event loop)
    config.py         # Configuration dataclass (addr, password, db_path, etc.)
    message_mapper.py # Maps Delta Chat msgs ↔ Hermes message model
  tests/
    __init__.py
    conftest.py
    test_message_mapper.py
    test_gateway.py          # Unit tests with mocked DC client
    test_integration.py      # Live test against chatmail server (gated by env vars)
  .env.example               # Template for CHATMAIL_ADDR / CHATMAIL_PASSWORD
  ```

## Step 2 — Identify Chatmail gateway components

Based on the deltachat2 API and the echo-bot pattern:

| Component | Responsibility |
|---|---|
| **`config.py`** | Load credentials/env vars: `CHATMAIL_ADDR`, `CHATMAIL_PASSWORD`, `CHATMAIL_DB_PATH` (SQLite path for DC account), optional `CHATMAIL_BOT_NAME`. Validate at startup. |
| **`client.py`** | Thin wrapper over `deltachat2` `RpcClient`. Manages: connecting to `deltachat-rpc-server`, calling `add_account()` / `configure()` with addr+password, `start_io()`, subscribing to incoming-message events, and `send_msg()` for outbound replies. Exposes async event hooks. |
| **`message_mapper.py`** | Pure functions: `dc_msg_to_hermes(msg_snapshot)` → Hermes message object (text, sender info, chat_id, attachment paths). `hermes_to_dc_msg(text, attachments)` → parameters for `send_msg()`. |
| **`gateway.py`** | The `ChatmailGateway` class implementing the Hermes gateway interface. Wires `client.py` incoming events → `message_mapper` → `on_message()` (agent). Agent responses → `message_mapper` → `client.send_msg()`. |

## Step 3 — Implement the Chatmail gateway

### 3a. Config (`config.py`)
- Pydantic `BaseSettings` or dataclass reading env vars.
- Required: `addr` (email address), `password` (Delta Chat app password).
- Optional: `db_path` (default `~/.hermes-chatmail/dc.db`), `display_name`.

### 3b. Delta Chat client wrapper (`client.py`)
- `class ChatmailClient`:
  - `__init__(config)`: store config, create `RpcClient` instance.
  - `async start()`: connect RPC, `add_account()`, `configure(addr=, mail_pw=)`, `start_io()`, wait for `ConfigureSuccess`.
  - `async on_incoming(callback)`: subscribe to `IncomingMessage` events (or `NewMessage` with filter). Each event calls `callback(msg_snapshot)`.
  - `async send(chat_id, text, attachments=None)`: call `send_msg()` on the appropriate chat.
  - `async stop()`: `stop_io()`, close RPC connection.

### 3c. Message mapper (`message_mapper.py`)
- `def dc_to_hermes(snapshot, account) -> HermesMessage`:
  - Extract `text` from `snapshot.text`
  - Extract sender: `snapshot.get_sender_contact()` → name + address
  - Map DC `chat_id` to Hermes conversation/session ID
  - If `snapshot.has_file()`, record attachment path + MIME type
- `def hermes_to_dc_params(text, attachments)` → dict for `send_msg()`.

### 3d. Gateway (`gateway.py`)
- `class ChatmailGateway(BaseGateway)` (or whatever Hermes base class is):
  - `__init__(config)`: instantiate `ChatmailClient(config)`.
  - `async start()`: `await client.start()`, register `client.on_incoming(self._handle_incoming)`.
  - `async _handle_incoming(snapshot)`: map to Hermes message, call `self.on_message(hermes_msg)` to feed agent.
  - `async send_message(conversation_id, text, attachments=None)`: map to DC params, `await client.send(...)`.
  - `async stop()`: `await client.stop()`.
- Entry-point registration in `pyproject.toml` pointing to `ChatmailGateway`.

## Step 4 — Testing plan

### Unit tests (no network, mock DC client)
- **`test_message_mapper.py`**: 
  - Text-only message → correct Hermes message fields.
  - Message with attachment → attachment path + MIME captured.
  - Outbound mapping: Hermes reply text → DC send params.
  - Edge: empty text with attachment only; multi-line text; special characters.
- **`test_gateway.py`** (mock `ChatmailClient`):
  - `start()` calls `client.start()` and registers the incoming callback.
  - Incoming event triggers `on_message` with correctly mapped message.
  - `send_message()` calls `client.send()` with correct chat_id + text.
  - `stop()` calls `client.stop()`.
  - Error path: `client.start()` raises → gateway propagates or handles per Hermes convention.
- **`test_config.py`**:
  - Env vars correctly loaded.
  - Missing required vars raises clear error.

### Integration test (live chatmail server, gated by env vars)
- **`test_integration.py`** — skipped unless `CHATMAIL_ADDR` and `CHATMAIL_PASSWORD` are set:
  - **Test 1 — Account configure**: `ChatmailClient.start()` successfully configures against the real chatmail server.
  - **Test 2 — Round-trip echo**: Send a message from a second test account (or use the echo-bot pattern) to the bot's address; verify the gateway receives and maps it; send a reply; verify delivery.
  - **Test 3 — Attachment round-trip**: Send an image attachment; verify gateway receives it with correct MIME and file path.
  - **Test 4 — Multi-turn conversation**: Verify chat_id stability across multiple messages in the same Delta Chat chat.
  - **Test 5 — Lifecycle**: `start()` → exchange messages → `stop()` → `start()` again, verify account persistence via db_path.

### Test fixtures (`conftest.py`)
- `mock_client` fixture: `MagicMock`/`AsyncMock` of `ChatmailClient` for unit tests.
- `mock_snapshot` factory: creates fake DC message snapshots with configurable text/sender/attachment.
- `live_config` fixture: reads env vars, `pytest.skip` if not present.

## Step 5 — Run the tests

1. Install package + dev deps: `pip install -e ".[dev]"` (this also installs `deltachat2` and entry point).
2. Ensure `deltachat-rpc-server` binary is available (deltachat2 may bundle or require it — verify during implementation).
3. Run unit tests: `pytest tests/test_config.py tests/test_message_mapper.py tests/test_gateway.py -v`
4. Set env vars: `export CHATMAIL_ADDR=... CHATMAIL_PASSWORD=...`
5. Run integration tests: `pytest tests/test_integration.py -v`
6. Verify all pass; debug failures.

## Verification / Acceptance Criteria

- [ ] `hermes-agent` submodule is present and its gateway interface has been read and confirmed (assumptions updated if needed).
- [ ] `pip install -e .` succeeds and registers the `hermes.gateways.chatmail` entry point.
- [ ] All unit tests pass with `pytest -v`.
- [ ] Integration tests pass against the real chatmail account (round-trip text + attachment).
- [ ] A message sent via Delta Chat to the bot's address reaches the Hermes agent, and the agent's reply is delivered back to the Delta Chat user.
- [ ] `start()` / `stop()` lifecycle is clean (no hung RPC connections, account persists across restarts via db_path).

## Out of Scope

- Group chat / multi-user routing beyond basic 1:1 (can be a follow-up).
- Delta Chat webxdc / interactive app messages.
- Voice messages / audio transcoding.
- Deployment packaging (Docker, systemd) — plugin only.
