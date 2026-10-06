"""Mock infrastructure for testing the Chatmail adapter without the full
Hermes Agent source tree.

The adapter imports from ``gateway.platforms.*`` and ``gateway.config`` —
modules that only exist inside the Hermes Agent runtime.  This conftest
installs lightweight stand-ins into ``sys.modules`` so the adapter can be
imported and tested in isolation.
"""

from __future__ import annotations

import sys
import types
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, AsyncMock

import pytest

# ── Hermes mock modules ───────────────────────────────────────────────────────

def _install_hermes_mocks() -> None:
    """Populate sys.modules with minimal Hermes gateway stubs."""

    # --- gateway.config ---
    config_mod = types.ModuleType("gateway.config")

    class Platform:
        """Minimal stand-in for the Platform enum.

        The real Platform is an Enum, but the plugin path uses
        ``Platform("chatmail")`` which invokes ``_missing_`` to accept
        any registered name.  Our stub just stores the string value.
        """
        def __init__(self, value: str):
            self.value = value

        def __str__(self):
            return self.value

        def __eq__(self, other):
            if isinstance(other, Platform):
                return self.value == other.value
            if isinstance(other, str):
                return self.value == other
            return NotImplemented

        def __hash__(self):
            return hash(self.value)

    @dataclass
    class PlatformConfig:
        enabled: bool = True
        token: Optional[str] = None
        extra: Dict[str, Any] = field(default_factory=dict)

    config_mod.Platform = Platform
    config_mod.PlatformConfig = PlatformConfig
    sys.modules["gateway.config"] = config_mod

    # --- gateway package ---
    gateway_pkg = types.ModuleType("gateway")
    sys.modules["gateway"] = gateway_pkg

    # --- gateway.platforms package ---
    platforms_pkg = types.ModuleType("gateway.platforms")
    sys.modules["gateway.platforms"] = platforms_pkg

    # --- gateway.platforms._shared ---
    shared_mod = types.ModuleType("gateway.platforms._shared")

    # In-memory secret store for tests.
    _secret_store: Dict[str, str] = {}

    def get_scoped_secret(name: str, default: str = "") -> str:
        return _secret_store.get(name, default)

    def set_scoped_secret(name: str, value: str) -> None:
        _secret_store[name] = value

    def clear_secrets() -> None:
        _secret_store.clear()

    def extra_or_secret(extra: dict, key: str, env_var: str, default: Any = None) -> Any:
        val = get_scoped_secret(env_var)
        if val:
            return val
        if extra and key in extra:
            return extra[key]
        return default

    def seed_extra_from_env(
        entries,
        home_env: Optional[str] = None,
        home_default: Any = None,
    ) -> dict:
        result: dict = {}
        for entry in entries:
            if len(entry) == 3:
                env_var, key, conv = entry
            else:
                env_var, key = entry
                conv = None
            val = get_scoped_secret(env_var)
            if not val:
                continue
            if conv is not None:
                try:
                    val = conv(val)
                except (ValueError, TypeError):
                    continue
            result[key] = val
        if home_env:
            home_val = get_scoped_secret(home_env)
            if home_val:
                result["home_channel"] = {"chat_id": home_val, "name": "Home"}
            elif home_default:
                result["home_channel"] = {"chat_id": home_default, "name": "Home"}
        return result

    def send_error(msg: str) -> Dict[str, Any]:
        return {"error": msg}

    def coerce_port(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    shared_mod.get_scoped_secret = get_scoped_secret
    shared_mod.set_scoped_secret = set_scoped_secret
    shared_mod.clear_secrets = clear_secrets
    shared_mod.extra_or_secret = extra_or_secret
    shared_mod.seed_extra_from_env = seed_extra_from_env
    shared_mod.send_error = send_error
    shared_mod.coerce_port = coerce_port
    sys.modules["gateway.platforms._shared"] = shared_mod

    # --- gateway.platforms.base ---
    base_mod = types.ModuleType("gateway.platforms.base")

    @dataclass
    class SendResult:
        success: bool = False
        message_id: str = ""
        error: Optional[str] = None

    class BasePlatformAdapter:
        """Minimal base class mirroring the real BasePlatformAdapter."""

        gateway_runner: Optional[Any] = None

        def __init__(self, config=None, platform=None, **kwargs):
            self._config = config
            self._platform = platform
            self._connected = False
            self._message_handler = None
            self._fatal_error: Optional[dict] = None

        @property
        def is_connected(self) -> bool:
            return self._connected

        def _mark_connected(self) -> None:
            self._connected = True

        def _mark_disconnected(self) -> None:
            self._connected = False

        def _set_fatal_error(self, code: str, message: str, *, retryable: bool) -> None:
            self._fatal_error = {"code": code, "message": message, "retryable": retryable}

        async def _notify_fatal_error(self) -> None:
            pass

        def _acquire_platform_lock(self, platform: str, key: str, label: str) -> bool:
            return True

        def _release_platform_lock(self) -> None:
            pass

        def build_source(self, **kwargs):
            """Return a dict-like source object."""
            return dict(kwargs)

        async def handle_message(self, event) -> None:
            """Record the event for test inspection."""
            if self._message_handler:
                await self._message_handler(event)
            self._last_event = event

        def _wire_plugin_handlers(self, *args, **kwargs) -> None:
            pass

    base_mod.BasePlatformAdapter = BasePlatformAdapter
    base_mod.SendResult = SendResult
    sys.modules["gateway.platforms.base"] = base_mod

    # --- gateway.platforms.event ---
    event_mod = types.ModuleType("gateway.platforms.event")

    class MessageType:
        TEXT = "text"

    @dataclass
    class MessageEvent:
        text: str
        message_type: str
        source: Any
        message_id: str
        timestamp: Any = None

    event_mod.MessageType = MessageType
    event_mod.MessageEvent = MessageEvent
    sys.modules["gateway.platforms.event"] = event_mod

    # --- gateway.platforms.helpers ---
    helpers_mod = types.ModuleType("gateway.platforms.helpers")

    class MessageDeduplicator:
        """Simple in-memory deduplicator for tests."""
        def __init__(self, ttl_seconds: int = 600):
            self._seen: set[str] = set()
            self._ttl = ttl_seconds

        def is_duplicate(self, key: str) -> bool:
            if key in self._seen:
                return True
            self._seen.add(key)
            return False

    async def cancel_task(task: asyncio.Task) -> None:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    helpers_mod.MessageDeduplicator = MessageDeduplicator
    helpers_mod.cancel_task = cancel_task
    sys.modules["gateway.platforms.helpers"] = helpers_mod


# Install mocks before any test module imports the adapter.
_install_hermes_mocks()


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_secrets():
    """Clear the secret store before and after each test."""
    from gateway.platforms._shared import clear_secrets
    clear_secrets()
    yield
    clear_secrets()


@pytest.fixture
def chatmail_env(clean_secrets):
    """Set up standard Chatmail env vars."""
    from gateway.platforms._shared import set_scoped_secret
    set_scoped_secret("CHATMAIL_ADDR", "bot@testrun.org")
    set_scoped_secret("CHATMAIL_PASSWORD", "secret123")
    yield
    from gateway.platforms._shared import clear_secrets
    clear_secrets()


@pytest.fixture
def platform_config(chatmail_env):
    """Return a PlatformConfig with the chatmail env vars set."""
    from gateway.config import PlatformConfig
    return PlatformConfig()


@pytest.fixture
def make_adapter():
    """Factory to create adapter instances with a given config."""
    from gateway.config import PlatformConfig
    from gateway.platforms._shared import set_scoped_secret

    def _make(addr="bot@testrun.org", password="secret123", **extra):
        set_scoped_secret("CHATMAIL_ADDR", addr)
        set_scoped_secret("CHATMAIL_PASSWORD", password)
        for k, v in extra.items():
            set_scoped_secret(k.upper(), str(v))
        config = PlatformConfig()
        return config

    return _make