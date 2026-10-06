"""Unit tests for the Chatmail (Delta Chat) platform adapter."""

from __future__ import annotations

import asyncio
import datetime
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

# ── Mock deltachat2 objects ───────────────────────────────────────────────────

class MockSpecialContactId:
    SELF = 1
    INFO = 2
    DEVICE = 5
    LAST_SPECIAL = 9


class MockChatType:
    SINGLE = "Single"
    GROUP = "Group"


@dataclass
class MockContact:
    address: str = "user@example.com"
    display_name: str = "Test User"
    id: int = 10
    is_bot: bool = False


@dataclass
class MockMessage:
    id: int = 100
    chat_id: int = 200
    from_id: int = 10
    text: str = "Hello, bot!"
    is_info: bool = False
    is_bot: bool = False
    timestamp: int = int(time.time())
    sender: MockContact = field(default_factory=MockContact)


@dataclass
class MockBasicChat:
    id: int = 200
    name: str = "Test Chat"
    chat_type: str = MockChatType.SINGLE


@dataclass
class MockMessageData:
    text: Optional[str] = None
    file: Optional[str] = None
    filename: Optional[str] = None
    html: Optional[str] = None
    quoted_message_id: Optional[int] = None
    quoted_text: Optional[str] = None
    viewtype: Optional[str] = None
    override_sender_name: Optional[str] = None
    location: Optional[tuple] = None


@dataclass
class MockEnteredLoginParam:
    addr: str = ""
    password: str = ""
    certificate_checks: Optional[str] = None
    imap_folder: Optional[str] = None
    imap_port: Optional[int] = None
    imap_security: Optional[str] = None
    imap_server: Optional[str] = None
    imap_user: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_security: Optional[str] = None
    smtp_server: Optional[str] = None
    smtp_user: Optional[str] = None


class MockEventTypeIncomingMsg:
    """Mock for EventTypeIncomingMsg."""
    pass


@dataclass
class MockNewMsgEvent:
    command: str = ""
    payload: str = ""
    msg: MockMessage = field(default_factory=MockMessage)


class MockRpc:
    """Mock Rpc that simulates the Delta Chat JSON-RPC interface."""

    def __init__(self, transport=None):
        self._transport = transport
        self._configured: Dict[int, bool] = {1: True}
        self._sent_messages: List[Dict[str, Any]] = []
        self._seen_messages: List[int] = []
        self._started_io: List[int] = []
        self._stopped_io: List[int] = []
        self._next_msg_id = 1000
        self._chats: Dict[int, MockBasicChat] = {
            200: MockBasicChat(id=200, name="Test Chat", chat_type=MockChatType.SINGLE),
            300: MockBasicChat(id=300, name="Group Chat", chat_type=MockChatType.GROUP),
        }
        self._accounts = [1]
        self._config: Dict[int, Dict[str, str]] = {}

    def get_all_account_ids(self) -> list[int]:
        return list(self._accounts)

    def add_account(self) -> int:
        new_id = max(self._accounts) + 1
        self._accounts.append(new_id)
        self._configured[new_id] = False
        return new_id

    def is_configured(self, account_id: int) -> bool:
        return self._configured.get(account_id, False)

    def set_config(self, account_id: int, key: str, value: Optional[str]) -> None:
        self._config.setdefault(account_id, {})[key] = value or ""

    def get_config(self, account_id: int, key: str) -> Optional[str]:
        return self._config.get(account_id, {}).get(key)

    def add_or_update_transport(self, account_id: int, param: Any) -> None:
        self._configured[account_id] = True

    def start_io(self, account_id: int) -> None:
        self._started_io.append(account_id)

    def stop_io(self, account_id: int) -> None:
        self._stopped_io.append(account_id)

    def start_io_for_all_accounts(self) -> None:
        for aid in self._accounts:
            self._started_io.append(aid)

    def stop_io_for_all_accounts(self) -> None:
        for aid in self._accounts:
            self._stopped_io.append(aid)

    def send_msg(self, account_id: int, chat_id: int, data: Any) -> int:
        msg_id = self._next_msg_id
        self._next_msg_id += 1
        self._sent_messages.append({
            "account_id": account_id,
            "chat_id": chat_id,
            "text": data.text,
            "msg_id": msg_id,
        })
        return msg_id

    def get_basic_chat_info(self, account_id: int, chat_id: int) -> MockBasicChat:
        return self._chats.get(chat_id, MockBasicChat(id=chat_id, name=str(chat_id)))

    def get_message(self, account_id: int, msg_id: int) -> MockMessage:
        return MockMessage(id=msg_id, chat_id=200, from_id=10)

    def markseen_msgs(self, account_id: int, msg_ids: list[int]) -> None:
        self._seen_messages.extend(msg_ids)

    def get_next_event(self) -> Any:
        """Block forever — simulates waiting for events."""
        threading.Event().wait(3600)
        raise RuntimeError("Should not reach here in tests")


class MockBot:
    """Mock Bot that records the run_until call."""

    def __init__(self, rpc, hooks=None, logger=None, command_prefix="/"):
        self.rpc = rpc
        self._hooks = hooks
        self._logger = logger
        self._run_until_called = False
        self._stop_predicate = None

    def run_until(self, func, account_id: int = 0) -> Any:
        self._run_until_called = True
        self._stop_predicate = func
        # In tests, immediately return as if the stop predicate was True.
        # The real implementation blocks; we simulate instant stop.
        return None

    def run_forever(self, account_id: int = 0) -> None:
        self.run_until(lambda _: False, account_id)

    def add_hook(self, hook, event=None) -> None:
        pass


class MockIOTransport:
    """Mock IOTransport that doesn't spawn a subprocess."""

    def __init__(self, accounts_dir=None, rpc_executable="deltachat-rpc-server", **kwargs):
        self.accounts_dir = accounts_dir
        self.rpc_executable = rpc_executable
        self._started = False
        self._closed = False

    def start(self) -> None:
        self._started = True

    def close(self) -> None:
        self._closed = True

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.close()


class MockEvents:
    """Mock for deltachat2.events module."""

    class HookCollection:
        def __init__(self):
            self._hooks = []

        def on(self, event_filter):
            def decorator(func):
                self._hooks.append((func, event_filter))
                return func
            return decorator

    class NewMessage:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class RawEvent:
        def __init__(self, **kwargs):
            self.kwargs = kwargs


# ── Test helper to install deltachat2 mocks ───────────────────────────────────

def _install_deltachat2_mocks():
    """Install mock deltachat2 modules into sys.modules."""
    import sys
    import types

    dc2 = types.ModuleType("deltachat2")
    dc2.Bot = MockBot
    dc2.IOTransport = MockIOTransport
    dc2.Rpc = MockRpc
    dc2.events = MockEvents()
    dc2.EnteredLoginParam = MockEnteredLoginParam
    dc2.MessageData = MockMessageData
    dc2.SpecialContactId = MockSpecialContactId
    dc2.ChatType = MockChatType
    dc2.JsonRpcError = type("JsonRpcError", (Exception,), {})

    sys.modules["deltachat2"] = dc2


_install_deltachat2_mocks()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestAdapterConstruction:
    """Tests for adapter __init__ and config parsing."""

    def test_construction_with_env(self, make_adapter):
        """Adapter should read addr/password from env vars."""
        from chatmail.adapter import ChatmailAdapter
        config = make_adapter(addr="bot@testrun.org", password="pass123")
        adapter = ChatmailAdapter(config)
        assert adapter._addr == "bot@testrun.org"
        assert adapter._password == "pass123"
        assert adapter.name == "Chatmail"

    def test_construction_with_extra(self, clean_secrets):
        """Adapter should read addr/password from config.extra dict."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter
        config = PlatformConfig(extra={"addr": "extra@testrun.org", "password": "extrapass"})
        adapter = ChatmailAdapter(config)
        assert adapter._addr == "extra@testrun.org"
        assert adapter._password == "extrapass"

    def test_construction_defaults(self, clean_secrets):
        """Adapter should have empty addr/password when nothing is configured."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter
        config = PlatformConfig()
        adapter = ChatmailAdapter(config)
        assert adapter._addr == ""
        assert adapter._password == ""
        assert adapter._accid is None
        assert adapter._rpc is None

    def test_construction_custom_accounts_dir(self, clean_secrets):
        """Adapter should read custom accounts dir from env."""
        from gateway.platforms._shared import set_scoped_secret
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter
        set_scoped_secret("CHATMAIL_ADDR", "bot@testrun.org")
        set_scoped_secret("CHATMAIL_PASSWORD", "pass")
        set_scoped_secret("CHATMAIL_ACCOUNTS_DIR", "/tmp/custom-dc")
        config = PlatformConfig()
        adapter = ChatmailAdapter(config)
        assert adapter._accounts_dir == "/tmp/custom-dc"


class TestCheckRequirements:
    """Tests for the check_requirements function."""

    def test_requirements_met(self, chatmail_env):
        from chatmail.adapter import check_requirements
        assert check_requirements() is True

    def test_requirements_missing_password(self, clean_secrets):
        from gateway.platforms._shared import set_scoped_secret
        from chatmail.adapter import check_requirements
        set_scoped_secret("CHATMAIL_ADDR", "bot@testrun.org")
        # No password set
        assert check_requirements() is False

    def test_requirements_missing_addr(self, clean_secrets):
        from gateway.platforms._shared import set_scoped_secret
        from chatmail.adapter import check_requirements
        set_scoped_secret("CHATMAIL_PASSWORD", "pass")
        # No addr set
        assert check_requirements() is False

    def test_requirements_nothing_set(self, clean_secrets):
        from chatmail.adapter import check_requirements
        assert check_requirements() is False


class TestValidateConfig:
    """Tests for the validate_config function."""

    def test_valid_config_with_env(self, chatmail_env):
        from gateway.config import PlatformConfig
        from chatmail.adapter import validate_config
        assert validate_config(PlatformConfig()) is True

    def test_valid_config_with_extra(self, clean_secrets):
        from gateway.config import PlatformConfig
        from chatmail.adapter import validate_config
        config = PlatformConfig(extra={"addr": "bot@testrun.org", "password": "pass"})
        assert validate_config(config) is True

    def test_invalid_config(self, clean_secrets):
        from gateway.config import PlatformConfig
        from chatmail.adapter import validate_config
        assert validate_config(PlatformConfig()) is False


class TestEnvEnablement:
    """Tests for the _env_enablement function."""

    def test_env_enablement_seeds_addr_password(self, chatmail_env):
        from chatmail.adapter import _env_enablement
        result = _env_enablement()
        assert result is not None
        assert result["addr"] == "bot@testrun.org"
        assert result["password"] == "secret123"

    def test_env_enablement_returns_none_when_unconfigured(self, clean_secrets):
        from chatmail.adapter import _env_enablement
        assert _env_enablement() is None

    def test_env_enablement_seeds_accounts_dir(self, chatmail_env):
        from gateway.platforms._shared import set_scoped_secret
        from chatmail.adapter import _env_enablement
        set_scoped_secret("CHATMAIL_ACCOUNTS_DIR", "/tmp/test-accounts")
        result = _env_enablement()
        assert result is not None
        assert result["accounts_dir"] == "/tmp/test-accounts"


class TestConnect:
    """Tests for the connect() lifecycle method."""

    @pytest.mark.asyncio
    async def test_connect_fails_without_config(self, clean_secrets):
        """connect() should fail with non-retryable error when addr/password missing."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter
        adapter = ChatmailAdapter(PlatformConfig())
        result = await adapter.connect()
        assert result is False
        assert adapter._fatal_error is not None
        assert adapter._fatal_error["retryable"] is False
        assert "CHATMAIL_ADDR" in adapter._fatal_error["message"]

    @pytest.mark.asyncio
    async def test_connect_succeeds_with_config(self, chatmail_env):
        """connect() should succeed and mark connected when properly configured."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        # Patch IOTransport and Rpc to use mocks
        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            result = await adapter.connect()

        assert result is True
        assert adapter.is_connected is True
        assert adapter._accid is not None
        assert adapter._bot_thread is not None
        assert adapter._drain_task is not None

        # Clean up
        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_connect_configures_new_account(self, clean_secrets):
        """connect() should configure a new account if not already configured."""
        from gateway.platforms._shared import set_scoped_secret
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        set_scoped_secret("CHATMAIL_ADDR", "newbot@testrun.org")
        set_scoped_secret("CHATMAIL_PASSWORD", "newpass")

        mock_rpc = MockRpc()
        mock_rpc._configured = {1: False}  # Account not configured

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", return_value=mock_rpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            result = await adapter.connect()

        assert result is True
        # Verify add_or_update_transport was called (configured is now True)
        assert mock_rpc._configured[1] is True
        # Verify bot flag was set
        assert mock_rpc._config[1].get("bot") == "1"

        await adapter.disconnect()


class TestDisconnect:
    """Tests for the disconnect() lifecycle method."""

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up(self, chatmail_env):
        """disconnect() should stop the bot, close transport, and mark disconnected."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()
            assert adapter.is_connected is True

            await adapter.disconnect()

        assert adapter.is_connected is False
        assert adapter._trans is None
        assert adapter._rpc is None
        assert adapter._accid is None


class TestSend:
    """Tests for the send() method."""

    @pytest.mark.asyncio
    async def test_send_text_message(self, chatmail_env):
        """send() should send a text message and return a SendResult."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        mock_rpc = MockRpc()

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", return_value=mock_rpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()

            result = await adapter.send("200", "Hello from Hermes!")

        assert result.success is True
        assert result.message_id is not None
        assert len(mock_rpc._sent_messages) == 1
        assert mock_rpc._sent_messages[0]["chat_id"] == 200
        assert mock_rpc._sent_messages[0]["text"] == "Hello from Hermes!"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_send_not_connected(self, clean_secrets):
        """send() should fail when not connected."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter
        adapter = ChatmailAdapter(PlatformConfig())
        result = await adapter.send("200", "test")
        assert result.success is False
        assert "Not connected" in (result.error or "")


class TestGetChatInfo:
    """Tests for get_chat_info()."""

    @pytest.mark.asyncio
    async def test_get_chat_info_dm(self, chatmail_env):
        """get_chat_info should return correct info for a DM chat."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        mock_rpc = MockRpc()

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", return_value=mock_rpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()

            info = await adapter.get_chat_info("200")

        assert info["name"] == "Test Chat"
        assert info["type"] == "dm"
        assert info["chat_id"] == "200"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_get_chat_info_group(self, chatmail_env):
        """get_chat_info should return correct info for a group chat."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        mock_rpc = MockRpc()

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", return_value=mock_rpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()

            info = await adapter.get_chat_info("300")

        assert info["name"] == "Group Chat"
        assert info["type"] == "group"

        await adapter.disconnect()


class TestInboundMessage:
    """Tests for inbound message handling."""

    @pytest.mark.asyncio
    async def test_on_new_message_filters_self(self, chatmail_env):
        """The hook should skip messages from SELF."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()

            # Create a self-message
            self_msg = MockMessage(
                id=999, chat_id=200, from_id=MockSpecialContactId.SELF,
                text="My own message", sender=MockContact(address="bot@testrun.org")
            )
            event = MockNewMsgEvent(msg=self_msg)

            # Call the hook directly
            adapter._on_new_message(None, adapter._accid, event)

            # Queue should be empty (self-message filtered)
            assert adapter._inbound_queue.empty()

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_on_new_message_filters_info(self, chatmail_env):
        """The hook should skip info/system messages."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            await adapter.connect()

            info_msg = MockMessage(
                id=998, chat_id=200, from_id=10,
                text="User joined the group", is_info=True
            )
            event = MockNewMsgEvent(msg=info_msg)

            adapter._on_new_message(None, adapter._accid, event)

            assert adapter._inbound_queue.empty()

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_on_new_message_dispatches_user_message(self, chatmail_env):
        """The hook should dispatch legitimate user messages to handle_message."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        # Track the event dispatched to handle_message
        received_events = []

        async def mock_handler(event):
            received_events.append(event)

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            adapter._message_handler = mock_handler
            await adapter.connect()

            user_msg = MockMessage(
                id=500, chat_id=200, from_id=10,
                text="Hello bot!", sender=MockContact(address="user@example.com", display_name="Alice")
            )
            event = MockNewMsgEvent(msg=user_msg)

            adapter._on_new_message(None, adapter._accid, event)

            # Give the event loop time to process: call_soon_threadsafe + drain task
            await asyncio.sleep(0.1)

            # Message should have been dispatched to handle_message
            assert len(received_events) == 1
            assert received_events[0].text == "Hello bot!"
            assert received_events[0].message_id == "500"

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_on_new_message_deduplicates(self, chatmail_env):
        """The hook should skip duplicate messages."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import ChatmailAdapter

        received_events = []

        async def mock_handler(event):
            received_events.append(event)

        with patch("deltachat2.IOTransport", MockIOTransport), \
             patch("deltachat2.Rpc", MockRpc), \
             patch("deltachat2.Bot", MockBot):
            config = PlatformConfig()
            adapter = ChatmailAdapter(config)
            adapter._message_handler = mock_handler
            await adapter.connect()

            msg = MockMessage(id=501, chat_id=200, from_id=10, text="Duplicate test")
            event = MockNewMsgEvent(msg=msg)

            # First call should dispatch the message
            adapter._on_new_message(None, adapter._accid, event)
            await asyncio.sleep(0.1)
            assert len(received_events) == 1

            # Second call with same message ID should be filtered (deduplicated)
            adapter._on_new_message(None, adapter._accid, event)
            await asyncio.sleep(0.1)
            assert len(received_events) == 1  # Still only one event

        await adapter.disconnect()


class TestStandaloneSend:
    """Tests for the _standalone_send function."""

    @pytest.mark.asyncio
    async def test_standalone_send_missing_config(self, clean_secrets):
        """_standalone_send should error when config is missing."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import _standalone_send
        result = await _standalone_send(PlatformConfig(), "200", "test message")
        assert "error" in result
        assert "CHATMAIL_ADDR" in result["error"]

    @pytest.mark.asyncio
    async def test_standalone_send_invalid_chat_id(self, chatmail_env):
        """_standalone_send should error on non-numeric chat_id."""
        from gateway.config import PlatformConfig
        from chatmail.adapter import _standalone_send
        result = await _standalone_send(PlatformConfig(), "not-a-number", "test")
        assert "error" in result
        assert "invalid chat_id" in result["error"]


class TestRegistration:
    """Tests for the register() function and plugin metadata."""

    def test_register_calls_register_platform(self, chatmail_env):
        """register() should call ctx.register_platform with correct name."""
        from chatmail.adapter import register
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["name"] == "chatmail"
        assert kwargs["label"] == "Chatmail (Delta Chat)"
        assert kwargs["adapter_factory"] is not None
        assert kwargs["check_fn"] is not None
        assert kwargs["validate_config"] is not None
        assert kwargs["required_env"] == ["CHATMAIL_ADDR", "CHATMAIL_PASSWORD"]
        assert kwargs["cron_deliver_env_var"] == "CHATMAIL_HOME_CHANNEL"
        assert kwargs["standalone_sender_fn"] is not None
        assert kwargs["allowed_users_env"] == "CHATMAIL_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "CHATMAIL_ALLOW_ALL_USERS"
        assert kwargs["emoji"] == "📧"
        assert "Delta Chat" in kwargs["platform_hint"]
        assert kwargs["max_message_length"] == 30000