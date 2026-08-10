"""Bounded Conduit Decision Return support for Hermes MCP sessions.

This module owns the complete optional adapter: the closed notification
contract, origin correlation, canonical same-connection read, exact-session
wake callback, duplicate suppression, and reconnect reconciliation.  The
ordinary MCP path does not depend on it unless a server explicitly sets
``decision_return: true``.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import hashlib
import json
import logging
import threading
import time
from typing import Any, Callable, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel


logger = logging.getLogger(__name__)

CONDUIT_DECISION_RETURN_CAPABILITY = "io.conduit/decision-return"
CONDUIT_DECISION_RETURN_METHOD = "notifications/conduit/decision-return"
CONDUIT_DECISION_RETURN_VERSION = 1
MAX_DECISION_ID_LENGTH = 1024
TERMINAL_STATUSES = frozenset({"responded", "cancelled", "superseded", "expired"})


class _DecisionReturnParams(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    version: int = Field(ge=1, le=1)
    decision_id: str = Field(min_length=1, max_length=MAX_DECISION_ID_LENGTH)
    stream_seq: int = Field(gt=0)


class ConduitDecisionReturnNotification(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    jsonrpc: Literal["2.0"] = "2.0"
    method: str = Field(pattern=r"^notifications/conduit/decision-return$")
    params: _DecisionReturnParams


def compatible_server_capability(capabilities: Any) -> bool:
    """Return whether capabilities advertise exactly Return protocol v1."""
    experimental = getattr(capabilities, "experimental", None)
    if not isinstance(experimental, dict):
        return False
    value = experimental.get(CONDUIT_DECISION_RETURN_CAPABILITY)
    return (
        isinstance(value, dict)
        and set(value) == {"version"}
        and value.get("version") == CONDUIT_DECISION_RETURN_VERSION
    )


class _Origin(BaseModel):
    model_config = ConfigDict(frozen=True)

    server_name: str
    session_id: str


class ConduitDecisionReturnBridge:
    """Correlate returned Decisions to the exact originating Hermes session."""

    def __init__(self, *, max_origins: int = 256) -> None:
        self._max_origins = max(1, max_origins)
        self._origins: OrderedDict[str, _Origin] = OrderedDict()
        self._consumed: OrderedDict[str, int] = OrderedDict()
        self._fingerprints: dict[str, str] = {}
        self._inflight: set[tuple[str, int]] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._last_reconcile: dict[str, float] = {}
        self._waker: Optional[Callable[[str, str], bool]] = None
        self._state_lock = threading.RLock()

    def set_waker(self, waker: Optional[Callable[[str, str], bool]]) -> None:
        """Install the host-owned exact-session wake function."""
        with self._state_lock:
            self._waker = waker

    def forget_session(self, session_id: str) -> None:
        """Drop correlations when an interactive session is finalized/reset."""
        if not session_id:
            return
        with self._state_lock:
            for decision_id, origin in list(self._origins.items()):
                if origin.session_id == session_id:
                    self._origins.pop(decision_id, None)
                    self._consumed.pop(decision_id, None)
                    self._fingerprints.pop(decision_id, None)

    def register_tool_result(
        self,
        *,
        server_name: str,
        tool_name: str,
        session_id: str,
        result: Any,
    ) -> Optional[str]:
        """Record only successful creation results before the tool call yields."""
        if tool_name not in {"create_decision", "create_evidenced_decision"}:
            return None
        if not server_name or not session_id or getattr(result, "isError", False):
            return None
        structured = getattr(result, "structuredContent", None)
        if not isinstance(structured, dict):
            return None
        decision = structured.get("decision")
        request = decision.get("request") if isinstance(decision, dict) else None
        decision_id = request.get("id") if isinstance(request, dict) else None
        if (
            not isinstance(decision_id, str)
            or not decision_id
            or len(decision_id) > MAX_DECISION_ID_LENGTH
        ):
            return None
        with self._state_lock:
            self._origins.pop(decision_id, None)
            self._origins[decision_id] = _Origin(
                server_name=server_name,
                session_id=session_id,
            )
            self._consumed.pop(decision_id, None)
            self._fingerprints.pop(decision_id, None)
            while len(self._origins) > self._max_origins:
                expired_id, _ = self._origins.popitem(last=False)
                self._consumed.pop(expired_id, None)
                self._fingerprints.pop(expired_id, None)
        return decision_id

    async def reconcile_decision(self, server: Any, decision_id: str) -> None:
        """Close the response-before-registration race with one immediate read."""
        capabilities = getattr(
            getattr(server, "initialize_result", None), "capabilities", None
        )
        if not compatible_server_capability(capabilities):
            return
        with self._state_lock:
            origin = self._origins.get(decision_id)
            key = (decision_id, 0)
            if (
                origin is None
                or origin.server_name != getattr(server, "name", None)
                or key in self._inflight
            ):
                return
            self._inflight.add(key)
        self._spawn_delivery(server, decision_id, 0)

    async def handle_notification(self, server: Any, notification: Any) -> None:
        """Validate and enqueue a Return without blocking the MCP receive loop."""
        capabilities = getattr(
            getattr(server, "initialize_result", None), "capabilities", None
        )
        if not compatible_server_capability(capabilities):
            return
        if isinstance(notification, ConduitDecisionReturnNotification):
            parsed = notification
        else:
            try:
                parsed = ConduitDecisionReturnNotification.model_validate(notification)
            except Exception:
                return
        decision_id = parsed.params.decision_id
        stream_seq = parsed.params.stream_seq
        with self._state_lock:
            origin = self._origins.get(decision_id)
            if origin is None or origin.server_name != getattr(server, "name", None):
                return
            if self._consumed.get(decision_id, 0) >= stream_seq:
                return
            key = (decision_id, stream_seq)
            if key in self._inflight:
                return
            self._inflight.add(key)
        self._spawn_delivery(server, decision_id, stream_seq)

    async def reconcile(self, server: Any) -> None:
        """Perform one bounded read of outstanding origins after attachment."""
        capabilities = getattr(
            getattr(server, "initialize_result", None), "capabilities", None
        )
        if not compatible_server_capability(capabilities):
            return
        server_name = str(getattr(server, "name", ""))
        now = time.monotonic()
        with self._state_lock:
            if now - self._last_reconcile.get(server_name, 0.0) < 2.0:
                return
            self._last_reconcile[server_name] = now
            candidates = [
                decision_id
                for decision_id, origin in self._origins.items()
                if origin.server_name == server_name
                and decision_id not in self._consumed
                and (decision_id, 0) not in self._inflight
            ]
            for decision_id in candidates:
                self._inflight.add((decision_id, 0))
        for decision_id in candidates:
            self._spawn_delivery(server, decision_id, 0)

    def _spawn_delivery(self, server: Any, decision_id: str, stream_seq: int) -> None:
        task = asyncio.create_task(
            self._deliver(server, decision_id, stream_seq),
            name=f"conduit-return:{getattr(server, 'name', 'server')}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _deliver(self, server: Any, decision_id: str, stream_seq: int) -> None:
        key = (decision_id, stream_seq)
        try:
            session = getattr(server, "session", None)
            if session is None:
                return
            async with server._rpc_lock:
                result = await session.call_tool(
                    "get_decision", arguments={"decision_id": decision_id}
                )
            if getattr(result, "isError", False):
                return
            structured = getattr(result, "structuredContent", None)
            decision = structured.get("decision") if isinstance(structured, dict) else None
            request = decision.get("request") if isinstance(decision, dict) else None
            if (
                not isinstance(decision, dict)
                or not isinstance(request, dict)
                or request.get("id") != decision_id
                or decision.get("status") not in TERMINAL_STATUSES
            ):
                return
            with self._state_lock:
                origin = self._origins.get(decision_id)
                waker = self._waker
            if origin is None or origin.server_name != getattr(server, "name", None):
                return
            if waker is None:
                return
            canonical = json.dumps(decision, ensure_ascii=False, separators=(",", ":"))
            fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            with self._state_lock:
                previous_seq = self._consumed.get(decision_id)
                previous_fingerprint = self._fingerprints.get(decision_id)
            if (
                previous_seq == 0
                and stream_seq > 0
                and previous_fingerprint == fingerprint
            ):
                with self._state_lock:
                    self._consumed[decision_id] = stream_seq
                return
            message = (
                "[Conduit Decision Return]\n"
                "The Decision below was read canonically through the same Conduit "
                "connection and belongs to this originating session. Continue from "
                "the human custody result; this Return grants no additional effect "
                "authority.\n"
                f"decision_id={decision_id}\n"
                f"stream_seq={stream_seq}\n"
                f"canonical_decision={canonical}"
            )
            if not waker(origin.session_id, message):
                return
            with self._state_lock:
                self._consumed.pop(decision_id, None)
                self._consumed[decision_id] = stream_seq
                self._fingerprints[decision_id] = fingerprint
                while len(self._consumed) > self._max_origins:
                    expired_id, _ = self._consumed.popitem(last=False)
                    self._fingerprints.pop(expired_id, None)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Conduit Decision Return delivery failed", exc_info=True)
        finally:
            with self._state_lock:
                self._inflight.discard(key)

    async def wait_idle(self) -> None:
        """Test/cleanup seam: wait until currently scheduled deliveries settle."""
        while self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)


decision_return_bridge = ConduitDecisionReturnBridge()


# The Python MCP SDK models server notifications as a closed union.  Extend
# that one validation boundary only for explicitly enabled Conduit sessions.
try:
    from mcp import ClientSession as _ClientSession, types as _mcp_types
    from mcp.client.session import (
        SUPPORTED_PROTOCOL_VERSIONS as _SUPPORTED_PROTOCOL_VERSIONS,
        _default_elicitation_callback,
        _default_list_roots_callback,
        _default_sampling_callback,
    )

    _stock_notification_union = _mcp_types.ServerNotification.model_fields[
        "root"
    ].annotation
    _extended_notification_union = Union[
        _stock_notification_union, ConduitDecisionReturnNotification
    ]

    class ConduitServerNotification(RootModel[_extended_notification_union]):
        root: _extended_notification_union


    class ConduitClientSession(_ClientSession):
        """ClientSession advertising and accepting only Return protocol v1."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._receive_notification_type = ConduitServerNotification

        async def initialize(self) -> Any:
            sampling = (
                (self._sampling_capabilities or _mcp_types.SamplingCapability())
                if self._sampling_callback is not _default_sampling_callback
                else None
            )
            elicitation = (
                _mcp_types.ElicitationCapability(
                    form=_mcp_types.FormElicitationCapability(),
                    url=_mcp_types.UrlElicitationCapability(),
                )
                if self._elicitation_callback is not _default_elicitation_callback
                else None
            )
            roots = (
                _mcp_types.RootsCapability(listChanged=True)
                if self._list_roots_callback is not _default_list_roots_callback
                else None
            )
            result = await self.send_request(
                _mcp_types.ClientRequest(
                    _mcp_types.InitializeRequest(
                        params=_mcp_types.InitializeRequestParams(
                            protocolVersion=_mcp_types.LATEST_PROTOCOL_VERSION,
                            capabilities=_mcp_types.ClientCapabilities(
                                sampling=sampling,
                                elicitation=elicitation,
                                experimental={
                                    CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}
                                },
                                roots=roots,
                                tasks=self._task_handlers.build_capability(),
                            ),
                            clientInfo=self._client_info,
                        )
                    )
                ),
                _mcp_types.InitializeResult,
            )
            if result.protocolVersion not in _SUPPORTED_PROTOCOL_VERSIONS:
                raise RuntimeError(
                    "Unsupported protocol version from the server: "
                    f"{result.protocolVersion}"
                )
            self._server_capabilities = result.capabilities
            await self.send_notification(
                _mcp_types.ClientNotification(_mcp_types.InitializedNotification())
            )
            return result

except ImportError:  # pragma: no cover - MCP is an optional Hermes extra.
    ConduitClientSession = None  # type: ignore[assignment,misc]
    ConduitServerNotification = None  # type: ignore[assignment,misc]
