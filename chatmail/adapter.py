"""Chatmail (Delta Chat) Platform Adapter for Hermes Agent.

Connects Hermes to a Delta Chat / chatmail account via the deltachat2
JSON-RPC client library.  Users chat with the bot through the Delta Chat
app; messages are relayed to the Hermes agent and responses are sent back
as Delta Chat messages.

Required env vars (or config.yaml ``gateway.platforms.chatmail.extra``):
    CHATMAIL_ADDR      — bot email address (e.g. bot@nine.testrun.org)
    CHATMAIL_PASSWORD  — bot email/IMAP password

Optional env vars:
    CHATMAIL_ACCOUNTS_DIR       — directory for the Delta Chat account database
    CHATMAIL_RPC_EXECUTABLE     — path to deltachat-rpc-server (default: PATH lookup)
    CHATMAIL_ALLOWED_USERS      — comma-separated email addresses allowed to talk to the bot
    CHATMAIL_ALLOW_ALL_USERS    — allow anyone to talk to the bot (dev only)
    CHATMAIL_HOME_CHANNEL       — default chat ID for cron delivery
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from gateway.platforms._shared import (
    extra_or_secret,
    get_scoped_secret as _get_scoped_secret,
    seed_extra_from_env as _seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator, cancel_task
from gateway.config import Platform

logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _default_accounts_dir() -> str:
    """Return the default Delta Chat accounts directory under ~/.hermes/."""
    try:
        from hermes_constants import get_hermes_home
        return str(get_hermes_home() / "chatmail-accounts")
    except Exception:
        return os.path.expanduser("~/.hermes/chatmail-accounts")


def _env_or_extra(extra: dict, env_var: str, key: str, default: Any = None) -> Any:
    """Read from profile-scoped env first, then config.extra, then default."""
    val = _get_scoped_secret(env_var)
    if val:
        return val
    return extra.get(key, default)


# ── Adapter ───────────────────────────────────────────────────────────────────

class ChatmailAdapter(BasePlatformAdapter):
    """Async Delta Chat / chatmail adapter implementing BasePlatformAdapter.

    The deltachat2 library is synchronous and blocking (it spawns
    ``deltachat-rpc-server`` as a subprocess and communicates over
    stdin/stdout JSON-RPC).  This adapter bridges the sync Delta Chat
    event loop to the async Hermes gateway by running the bot in a
    daemon thread and funnelling inbound messages through an
    ``asyncio.Queue``.
    """

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("chatmail"))
        extra = getattr(config, "extra", {}) or {}

        self._addr: str = _env_or_extra(extra, "CHATMAIL_ADDR", "addr", "")
        self._password: str = _env_or_extra(extra, "CHATMAIL_PASSWORD", "password", "")
        self._accounts_dir: str = _env_or_extra(
            extra, "CHATMAIL_ACCOUNTS_DIR", "accounts_dir", _default_accounts_dir()
        )
        self._rpc_executable: str = _env_or_extra(
            extra, "CHATMAIL_RPC_EXECUTABLE", "rpc_executable", "deltachat-rpc-server"
        )

        # Runtime state — populated in connect()
        self._trans = None       # IOTransport
        self._rpc = None         # Rpc
        self._bot = None         # Bot
        self._accid: Optional[int] = None
        self._bot_thread: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()
        self._inbound_queue: Optional[asyncio.Queue] = None
        self._drain_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._dedup = MessageDeduplicator(ttl_seconds=600)

    @property
    def name(self) -> str:
        return "Chatmail"

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Start the Delta Chat RPC server, configure the account, and begin
        listening for incoming messages."""
        if not self._addr or not self._password:
            logger.error("Chatmail: CHATMAIL_ADDR and CHATMAIL_PASSWORD must be configured")
            return self._fail(
                "config_missing",
                "CHATMAIL_ADDR and CHATMAIL_PASSWORD must be set",
                retryable=False,
            )

        # Prevent two profiles from using the same Delta Chat account.
        if not self._acquire_platform_lock(
            "chatmail", self._addr, f"Chatmail account {self._addr}"
        ):
            return False

        # Import deltachat2 lazily so the plugin module can be loaded
        # even when the library is not yet installed.
        try:
            from deltachat2 import Bot, IOTransport, Rpc, events
            from deltachat2 import EnteredLoginParam
        except ImportError:
            logger.error("Chatmail: deltachat2 library not installed")
            return self._fail(
                "deps_missing",
                "The deltachat2 package is not installed. Run: pip install 'deltachat2[full]'",
                retryable=False,
            )

        # 1. Start the RPC server subprocess.
        try:
            self._trans = IOTransport(
                accounts_dir=self._accounts_dir,
                rpc_executable=self._rpc_executable,
            )
            self._trans.start()
        except FileNotFoundError:
            logger.error("Chatmail: deltachat-rpc-server binary not found")
            return self._fail(
                "rpc_server_missing",
                "deltachat-rpc-server not found. Install with: pip install 'deltachat2[full]'",
                retryable=False,
            )
        except Exception as e:
            logger.error("Chatmail: failed to start RPC server — %s", e)
            return self._fail("rpc_start_failed", str(e), retryable=True)

        self._rpc = Rpc(self._trans)

        # 2. Get or create the account.
        try:
            accounts = self._rpc.get_all_account_ids()
            self._accid = accounts[0] if accounts else self._rpc.add_account()
        except Exception as e:
            logger.error("Chatmail: failed to get/create account — %s", e)
            await self._cleanup_transport()
            return self._fail("account_failed", str(e), retryable=True)

        # 3. Configure if necessary.
        try:
            if not self._rpc.is_configured(self._accid):
                self._rpc.set_config(self._accid, "bot", "1")
                self._rpc.add_or_update_transport(
                    self._accid,
                    EnteredLoginParam(addr=self._addr, password=self._password),
                )
                logger.info("Chatmail: configured account for %s", self._addr)
            else:
                # Ensure bot flag is set even on already-configured accounts.
                self._rpc.set_config(self._accid, "bot", "1")
        except Exception as e:
            logger.error("Chatmail: account configuration failed — %s", e)
            await self._cleanup_transport()
            return self._fail("configure_failed", str(e), retryable=True)

        # 4. Set up hooks and Bot.
        hooks = events.HookCollection()
        hooks.on(events.NewMessage(is_info=False))(self._on_new_message)
        self._bot = Bot(self._rpc, hooks, logger=logger)

        # 5. Set up the async/sync bridge.
        self._loop = asyncio.get_running_loop()
        self._inbound_queue = asyncio.Queue()
        self._stop_requested.clear()

        # 6. Start the bot event loop in a daemon thread.
        self._bot_thread = threading.Thread(
            target=self._run_bot_loop, daemon=True, name="chatmail-bot"
        )
        self._bot_thread.start()

        # 7. Start the async drain task.
        self._drain_task = asyncio.create_task(self._drain_inbound())

        self._mark_connected()
        logger.info("Chatmail: connected as %s", self._addr)
        return True

    async def disconnect(self) -> None:
        """Stop the bot, close the RPC server, and clean up."""
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()

        # Signal the bot thread to stop.
        self._stop_requested.set()
        if self._rpc and self._accid is not None:
            with contextlib.suppress(Exception):
                self._rpc.stop_io(self._accid)

        # Wait for the bot thread to finish.
        if self._bot_thread:
            self._bot_thread.join(timeout=10)
            if self._bot_thread.is_alive():
                logger.warning("Chatmail: bot thread did not stop within timeout")

        # Close the transport (stops the RPC server subprocess).
        await self._cleanup_transport()

        # Cancel the drain task.
        if self._drain_task:
            await cancel_task(self._drain_task)
            self._drain_task = None

        # Signal the drain queue to flush.
        if self._inbound_queue:
            self._inbound_queue.put_nowait(None)

        self._bot = None
        self._rpc = None
        self._accid = None
        self._bot_thread = None
        logger.info("Chatmail: disconnected")

    async def _cleanup_transport(self) -> None:
        """Close the IOTransport if it exists."""
        if self._trans:
            with contextlib.suppress(Exception):
                self._trans.close()
            self._trans = None

    # ── Bot thread ─────────────────────────────────────────────────────────

    def _run_bot_loop(self) -> None:
        """Run the Delta Chat event loop in a background thread.

        ``Bot.run_until`` processes events until the stop predicate
        returns True.  ``stop_io`` is called from ``disconnect`` to
        emit events that unblock ``get_next_event``, after which the
        stop predicate fires and the loop exits.
        """
        try:
            self._bot.run_until(lambda _: self._stop_requested.is_set())
        except Exception as e:
            if not self._stop_requested.is_set():
                logger.error("Chatmail: bot event loop error — %s", e)

    def _on_new_message(self, bot, accid: int, event) -> None:
        """NewMessage hook — called from the bot thread for each incoming
        user message.  Pushes the message into the asyncio queue so the
        drain task can hand it to the Hermes gateway."""
        msg = event.msg

        # Redundant safety: skip self-messages and info messages
        # (the NewMessage filter already handles these, but belt-and-suspenders).
        from deltachat2 import SpecialContactId
        if msg.from_id == SpecialContactId.SELF:
            return
        if msg.is_info:
            return

        # Deduplicate by Delta Chat message ID.
        if self._dedup.is_duplicate(str(msg.id)):
            return

        # Skip empty text messages (images, files, etc. without text).
        if not msg.text or not msg.text.strip():
            return

        # Thread-safe handoff to the asyncio event loop.
        if self._loop and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(
                    self._inbound_queue.put_nowait, msg
                )
            except RuntimeError:
                pass  # loop closed during shutdown

    # ── Inbound message dispatch ───────────────────────────────────────────

    async def _drain_inbound(self) -> None:
        """Async task that drains the inbound queue and dispatches messages
        to the Hermes gateway via ``handle_message``."""
        try:
            while True:
                msg = await self._inbound_queue.get()
                if msg is None:  # shutdown sentinel
                    break
                try:
                    await self._dispatch_message(msg)
                except Exception as e:
                    logger.warning("Chatmail: error dispatching message — %s", e)
        except asyncio.CancelledError:
            raise

    async def _dispatch_message(self, msg) -> None:
        """Build a ``MessageEvent`` from a Delta Chat message and hand it to
        the base class handler."""
        if not self._message_handler:
            return

        chat_id = str(msg.chat_id)

        # Determine chat type (dm vs group) from the chat metadata.
        chat_type = "dm"
        chat_name = chat_id
        try:
            chat = await asyncio.to_thread(
                self._rpc.get_basic_chat_info, self._accid, msg.chat_id
            )
            chat_name = chat.name
            from deltachat2 import ChatType
            if chat.chat_type == ChatType.GROUP:
                chat_type = "group"
        except Exception:
            pass  # Fall back to defaults if chat info lookup fails.

        # Mark the message as seen.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                self._rpc.markseen_msgs, self._accid, [msg.id]
            )

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=msg.sender.address,
            user_name=msg.sender.display_name or msg.sender.address,
        )

        event = MessageEvent(
            text=msg.text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(msg.id),
            timestamp=datetime.datetime.fromtimestamp(msg.timestamp),
        )
        await self.handle_message(event)

    # ── Outbound ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a Delta Chat chat."""
        if not self._rpc or self._accid is None:
            return SendResult(success=False, error="Not connected")

        from deltachat2 import MessageData

        data = MessageData(text=content)
        if reply_to:
            try:
                data.quoted_message_id = int(reply_to)
            except (ValueError, TypeError):
                pass  # ignore invalid reply_to IDs

        try:
            msg_id = await asyncio.to_thread(
                self._rpc.send_msg, self._accid, int(chat_id), data
            )
            return SendResult(success=True, message_id=str(msg_id))
        except Exception as e:
            logger.error("Chatmail: send failed — %s", e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Delta Chat has no typing indicator — no-op."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return metadata about a Delta Chat chat."""
        if not self._rpc or self._accid is None:
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}

        try:
            from deltachat2 import ChatType
            chat = await asyncio.to_thread(
                self._rpc.get_basic_chat_info, self._accid, int(chat_id)
            )
            return {
                "name": chat.name,
                "type": "group" if chat.chat_type == ChatType.GROUP else "dm",
                "chat_id": chat_id,
            }
        except Exception:
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}


# ── Plugin registration ──────────────────────────────────────────────────────

def check_requirements() -> bool:
    """Check if deltachat2 is installed and the account is configured."""
    try:
        import deltachat2  # noqa: F401
    except ImportError:
        return False
    return bool(
        _get_scoped_secret("CHATMAIL_ADDR", "").strip()
        and _get_scoped_secret("CHATMAIL_PASSWORD", "").strip()
    )


def validate_config(config) -> bool:
    """Validate that the platform config has enough info to connect."""
    extra = getattr(config, "extra", {}) or {}
    addr = extra_or_secret(extra, "addr", "CHATMAIL_ADDR")
    password = extra_or_secret(extra, "password", "CHATMAIL_PASSWORD")
    return bool(addr and password)


def is_connected(config) -> bool:
    """Check whether Chatmail is configured (env or config.yaml)."""
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env vars before adapter construction."""
    addr = _get_scoped_secret("CHATMAIL_ADDR", "").strip()
    password = _get_scoped_secret("CHATMAIL_PASSWORD", "").strip()
    if not (addr and password):
        return None
    seed = _seed_extra_from_env(
        (
            ("CHATMAIL_ACCOUNTS_DIR", "accounts_dir", None),
            ("CHATMAIL_RPC_EXECUTABLE", "rpc_executable", None),
        ),
        home_env="CHATMAIL_HOME_CHANNEL",
    )
    return {"addr": addr, "password": password, **seed}


def interactive_setup() -> None:
    """`hermes gateway setup` flow for Chatmail."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )
    from hermes_cli.setup_platforms import declines_reconfigure

    def info(*lines: str) -> None:
        for line in lines:
            print_info(line)

    print_header("Chatmail (Delta Chat)")
    if declines_reconfigure("Chatmail", "Reconfigure Chatmail?", "CHATMAIL_ADDR"):
        return

    info(
        "Connect Hermes to a Delta Chat / chatmail account.",
        " The bot will listen for messages from Delta Chat users and relay",
        " them to the Hermes agent.",
        "",
        " First, create a chatmail account at https://nine.testrun.org/new",
        " (or any other chatmail server). You will get an email address and",
        " password — enter them below.",
        "",
        " Install the Delta Chat app on your phone to chat with the bot.",
        " Scan the bot's invite QR code (printed on startup) to start chatting.",
    )
    print()

    existing_addr = get_env_value("CHATMAIL_ADDR")
    addr = prompt("Bot email address (e.g. hermes-bot@nine.testrun.org)", default=existing_addr or "")
    if not addr:
        print_warning("Email address is required — skipping Chatmail setup")
        return
    save_env_value("CHATMAIL_ADDR", addr.strip())

    password = prompt("Bot password", password=True)
    if not password:
        print_warning("Password is required — skipping Chatmail setup")
        return
    save_env_value("CHATMAIL_PASSWORD", password)

    print()
    info("🔑 Optional settings")
    accounts_dir = prompt(
        "Accounts database directory (leave empty for default ~/.hermes/chatmail-accounts)",
        default=get_env_value("CHATMAIL_ACCOUNTS_DIR") or "",
    )
    if accounts_dir:
        save_env_value("CHATMAIL_ACCOUNTS_DIR", accounts_dir)
    elif get_env_value("CHATMAIL_ACCOUNTS_DIR"):
        save_env_value("CHATMAIL_ACCOUNTS_DIR", "")

    print()
    info(
        "🔒 Access control: restrict who can message the bot",
        " Without restrictions, anyone who knows the bot's address can chat.",
    )
    if prompt_yes_no("Allow anyone to talk to the bot?", False):
        save_env_value("CHATMAIL_ALLOW_ALL_USERS", "true")
        save_env_value("CHATMAIL_ALLOWED_USERS", "")
        print_warning("⚠️ Open access — anyone with the bot's address can command it.")
    else:
        save_env_value("CHATMAIL_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed email addresses (comma-separated, leave empty to deny everyone)",
            default=get_env_value("CHATMAIL_ALLOWED_USERS") or "",
        )
        if allowed:
            save_env_value("CHATMAIL_ALLOWED_USERS", allowed.replace(" ", ""))
            print_success("Allowlist configured")
        else:
            save_env_value("CHATMAIL_ALLOWED_USERS", "")
            print_info("No addresses allowed — the bot will ignore all messages until you add addresses.")

    print()
    print_success("Chatmail configuration saved to ~/.hermes/.env")
    print_info("Install dependencies: pip install 'deltachat2[full]'")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


# ── Standalone sender (cron delivery) ─────────────────────────────────────────

async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Open an ephemeral Delta Chat connection, send a message, and close.

    Used by cron jobs that run outside the gateway process.
    ``thread_id``/``media_files``/``force_document`` are accepted for
    signature parity only — Delta Chat does not have threads or
    separate document delivery modes.
    """
    extra = getattr(pconfig, "extra", {}) or {}
    addr = extra_or_secret(extra, "addr", "CHATMAIL_ADDR")
    password = extra_or_secret(extra, "password", "CHATMAIL_PASSWORD")
    accounts_dir = extra_or_secret(extra, "accounts_dir", "CHATMAIL_ACCOUNTS_DIR", _default_accounts_dir())

    if not addr or not password:
        return send_error("CHATMAIL_ADDR and CHATMAIL_PASSWORD must be configured")

    try:
        chat_id_int = int(chat_id)
    except (ValueError, TypeError):
        return send_error(f"invalid chat_id {chat_id!r} — expected a numeric Delta Chat chat ID")

    def _send_sync() -> int:
        from deltachat2 import IOTransport, Rpc, MessageData, EnteredLoginParam
        trans = IOTransport(accounts_dir=accounts_dir)
        trans.start()
        try:
            rpc = Rpc(trans)
            accounts = rpc.get_all_account_ids()
            if not accounts:
                raise RuntimeError("No configured Delta Chat account found in " + str(accounts_dir))
            accid = accounts[0]
            if not rpc.is_configured(accid):
                rpc.set_config(accid, "bot", "1")
                rpc.add_or_update_transport(accid, EnteredLoginParam(addr=addr, password=password))
            rpc.start_io(accid)
            try:
                msg_id = rpc.send_msg(accid, chat_id_int, MessageData(text=message))
                # Give the core time to actually send the message via SMTP.
                time.sleep(3)
                return msg_id
            finally:
                with contextlib.suppress(Exception):
                    rpc.stop_io(accid)
        finally:
            with contextlib.suppress(Exception):
                trans.close()

    try:
        msg_id = await asyncio.to_thread(_send_sync)
        return {"success": True, "message_id": str(msg_id)}
    except Exception as e:
        logger.debug("Chatmail standalone send failed", exc_info=True)
        return send_error(f"Chatmail standalone send failed: {e}")


# ── Registration ──────────────────────────────────────────────────────────────

def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="chatmail",
        label="Chatmail (Delta Chat)",
        adapter_factory=ChatmailAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["CHATMAIL_ADDR", "CHATMAIL_PASSWORD"],
        install_hint=(
            "Install the Delta Chat client library: "
            "pip install 'deltachat2[full]'"
        ),
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="CHATMAIL_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="CHATMAIL_ALLOWED_USERS",
        allow_all_env="CHATMAIL_ALLOW_ALL_USERS",
        max_message_length=30000,
        emoji="📧",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Delta Chat (Chatmail). "
            "Delta Chat is a messenger that uses email as its transport. "
            "It supports plain text and basic markdown formatting. "
            "Messages can be sent to individual contacts (DMs) or groups. "
            "Keep responses conversational and well-formatted."
        ),
    )