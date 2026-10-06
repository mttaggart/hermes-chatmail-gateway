# Project Plan

Our objective is to build a new type of gateway for the [Hermes Agent](https://github.com/NousResearch/hermes-agent/). The Hermes [Documentation](https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters) has specific details on performing this type of task.

The type of gateway we want to add is based on [chatmail](https://github.com/chatmail/core/blob/main/README.md). Ultimately we want to use [Delta Chat](https://delta.chat) to speak to Hermes. Here is an [example bot](https://github.com/deltachat-bot/echo/tree/main/python) in Python (Hermes's native language) that implements a Delta Chat (chatmail) bot.

## Steps

1. ✅ Build the base structure for a Hermes gateway plugin
   - `plugin.yaml` (Hermes plugin manifest with env var definitions)
   - `adapter.py` (ChatmailAdapter + register() entry point)
   - `pyproject.toml` (package metadata with deltachat2 dependency)
   - Hermes agent added as git submodule for reference

2. ✅ Identify the necessary components for implementing a Chatmail gateway
   - `BasePlatformAdapter` interface: connect(), disconnect(), send(), get_chat_info()
   - Inbound: build_source() + handle_message(MessageEvent)
   - deltachat2 library: IOTransport, Rpc, Bot, events.NewMessage hook
   - Async/sync bridge: bot event loop in daemon thread → asyncio.Queue → drain task
   - Plugin registration: ctx.register_platform() with check_fn, validate_config, env_enablement_fn, standalone_sender_fn

3. ✅ Implement the Chatmail gateway
   - ChatmailAdapter with full lifecycle (connect/disconnect/send/get_chat_info)
   - Inbound message filtering (self-messages, info messages, deduplication)
   - Chat type mapping (DM vs Group)
   - Token lock for multi-profile safety
   - Standalone sender for cron delivery
   - Interactive setup wizard for `hermes gateway setup`
   - Platform hint for LLM context

4. ✅ Build a testing plan for the gateway
   - 29 unit tests covering: adapter construction, config parsing, check_requirements, validate_config, env_enablement, connect/disconnect lifecycle, send/get_chat_info, inbound message filtering (self/info/dedup), standalone send, and plugin registration
   - Mock infrastructure for Hermes gateway modules and deltachat2 library

5. ✅ Run the tests
   - All 29 tests pass: `pytest tests/ -v`

## Implementation details

- **Library**: deltachat2 (JSON-RPC client) with deltachat-rpc-server
- **Dev layout**: Standalone package with hermes-agent as git submodule
- **Test env**: User has a test chatmail account (e.g. nine.testrun.org)
- **Plugin path**: Drop into ~/.hermes/plugins/chatmail/ — zero core code changes
