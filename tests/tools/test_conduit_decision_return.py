from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anyio
import pytest
from pydantic import ValidationError
from mcp import types as mcp_types

from tools.conduit_decision_return import (
    CONDUIT_DECISION_RETURN_CAPABILITY,
    ConduitClientSession,
    ConduitDecisionReturnBridge,
    ConduitDecisionReturnNotification,
    ConduitServerNotification,
    compatible_server_capability,
)


def _result(decision_id: str, status: str = "ready") -> SimpleNamespace:
    return SimpleNamespace(
        isError=False,
        structuredContent={
            "decision": {
                "request": {"id": decision_id, "title": "Bounded test"},
                "response": None if status == "ready" else {
                    "kind": "approve_reject",
                    "decision": "approve",
                },
                "status": status,
            }
        },
        content=[],
    )


def test_capability_and_notification_contract_are_exact() -> None:
    assert compatible_server_capability(SimpleNamespace(
        experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
    ))
    assert not compatible_server_capability(SimpleNamespace(
        experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1, "extra": True}},
    ))
    assert not compatible_server_capability(SimpleNamespace(experimental={}))

    parsed = ConduitDecisionReturnNotification.model_validate({
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_1", "stream_seq": 3},
    })
    assert parsed.params.decision_id == "dec_1"
    with pytest.raises(Exception):
        ConduitDecisionReturnNotification.model_validate({
            "method": "notifications/conduit/decision-return",
            "params": {
                "version": 1,
                "decision_id": "dec_1",
                "stream_seq": 3,
                "title": "must not travel unsolicited",
            },
        })


def test_extended_notification_union_accepts_exact_jsonrpc_envelope() -> None:
    wrapped = ConduitServerNotification.model_validate({
        "jsonrpc": "2.0",
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_wire", "stream_seq": 3},
    })

    assert isinstance(wrapped.root, ConduitDecisionReturnNotification)
    assert wrapped.root.jsonrpc == "2.0"

    with pytest.raises(ValidationError):
        ConduitServerNotification.model_validate({
            "jsonrpc": "1.0",
            "method": "notifications/conduit/decision-return",
            "params": {"version": 1, "decision_id": "dec_wire", "stream_seq": 3},
        })


@pytest.mark.asyncio
async def test_extended_client_advertises_return_capability_during_initialize() -> None:
    incoming_send, incoming_receive = anyio.create_memory_object_stream(1)
    outgoing_send, _outgoing_receive = anyio.create_memory_object_stream(1)
    session = ConduitClientSession(incoming_receive, outgoing_send)
    result = mcp_types.InitializeResult(
        protocolVersion=mcp_types.LATEST_PROTOCOL_VERSION,
        capabilities=mcp_types.ServerCapabilities(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}}
        ),
        serverInfo=mcp_types.Implementation(name="fixture", version="1"),
    )
    session.send_request = AsyncMock(return_value=result)
    session.send_notification = AsyncMock()

    assert await session.initialize() is result
    request = session.send_request.await_args.args[0]
    assert request.root.params.capabilities.experimental == {
        CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}
    }
    session.send_notification.assert_awaited_once()
    await incoming_send.aclose()
    await outgoing_send.aclose()


@pytest.mark.asyncio
async def test_return_reads_canonical_decision_and_wakes_only_recorded_session() -> None:
    calls: list[tuple[str, dict]] = []
    wakes: list[tuple[str, str]] = []

    class Session:
        async def call_tool(self, name: str, arguments: dict):
            calls.append((name, arguments))
            return _result(arguments["decision_id"], "responded")

    server = SimpleNamespace(
        name="conduit",
        session=Session(),
        _rpc_lock=asyncio.Lock(),
        initialize_result=SimpleNamespace(capabilities=SimpleNamespace(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
        )),
    )
    bridge = ConduitDecisionReturnBridge(max_origins=4)
    bridge.set_waker(lambda session_id, content: wakes.append((session_id, content)) or True)
    bridge.register_tool_result(
        server_name="conduit",
        tool_name="create_decision",
        session_id="session-a",
        result=_result("dec_return"),
    )

    await bridge.handle_notification(server, {
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_return", "stream_seq": 3},
    })
    await bridge.wait_idle()

    assert calls == [("get_decision", {"decision_id": "dec_return"})]
    assert len(wakes) == 1
    assert wakes[0][0] == "session-a"
    assert '"status":"responded"' in wakes[0][1]

    # At-least-once transport must not produce duplicate continuation turns.
    await bridge.handle_notification(server, {
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_return", "stream_seq": 3},
    })
    await bridge.wait_idle()
    assert len(wakes) == 1

    await bridge.handle_notification(server, {
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_return", "stream_seq": 4},
    })
    await bridge.wait_idle()
    assert len(wakes) == 2
    assert calls[-1] == ("get_decision", {"decision_id": "dec_return"})


@pytest.mark.asyncio
async def test_unknown_unready_and_wrong_server_returns_do_not_wake() -> None:
    wakes: list[str] = []

    class Session:
        async def call_tool(self, _name: str, arguments: dict):
            return _result(arguments["decision_id"], "ready")

    server = SimpleNamespace(
        name="conduit",
        session=Session(),
        _rpc_lock=asyncio.Lock(),
        initialize_result=SimpleNamespace(capabilities=SimpleNamespace(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
        )),
    )
    bridge = ConduitDecisionReturnBridge(max_origins=2)
    bridge.set_waker(lambda _session_id, content: wakes.append(content) or True)
    bridge.register_tool_result(
        server_name="conduit",
        tool_name="create_decision",
        session_id="session-a",
        result=_result("dec_known"),
    )
    for decision_id in ("dec_unknown", "dec_known"):
        await bridge.handle_notification(server, {
            "method": "notifications/conduit/decision-return",
            "params": {"version": 1, "decision_id": decision_id, "stream_seq": 2},
        })
    await bridge.wait_idle()
    assert wakes == []

    other = SimpleNamespace(**{**server.__dict__, "name": "other"})
    await bridge.handle_notification(other, {
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_known", "stream_seq": 4},
    })
    await bridge.wait_idle()
    assert wakes == []


@pytest.mark.asyncio
async def test_reverse_order_returns_route_to_their_exact_originators() -> None:
    wakes: list[tuple[str, str]] = []

    class Session:
        async def call_tool(self, _name: str, arguments: dict):
            return _result(arguments["decision_id"], "responded")

    server = SimpleNamespace(
        name="conduit",
        session=Session(),
        _rpc_lock=asyncio.Lock(),
        initialize_result=SimpleNamespace(capabilities=SimpleNamespace(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
        )),
    )
    bridge = ConduitDecisionReturnBridge(max_origins=4)
    bridge.set_waker(
        lambda session_id, content: wakes.append((session_id, content)) or True
    )
    for decision_id, session_id in (("dec_a", "session-a"), ("dec_b", "session-b")):
        bridge.register_tool_result(
            server_name="conduit",
            tool_name="create_decision",
            session_id=session_id,
            result=_result(decision_id),
        )

    for decision_id in ("dec_b", "dec_a"):
        await bridge.handle_notification(server, {
            "method": "notifications/conduit/decision-return",
            "params": {"version": 1, "decision_id": decision_id, "stream_seq": 3},
        })
        await bridge.wait_idle()

    assert [session_id for session_id, _ in wakes] == ["session-b", "session-a"]
    assert "decision_id=dec_b" in wakes[0][1]
    assert "decision_id=dec_a" in wakes[1][1]

@pytest.mark.asyncio
async def test_reconnect_reconciles_once_without_periodic_polling() -> None:
    calls = 0
    wakes: list[str] = []

    class Session:
        async def call_tool(self, _name: str, arguments: dict):
            nonlocal calls
            calls += 1
            return _result(arguments["decision_id"], "responded")

    server = SimpleNamespace(
        name="conduit",
        session=Session(),
        _rpc_lock=asyncio.Lock(),
        initialize_result=SimpleNamespace(capabilities=SimpleNamespace(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
        )),
    )
    bridge = ConduitDecisionReturnBridge(max_origins=2)
    bridge.set_waker(lambda session_id, _content: wakes.append(session_id) or True)
    bridge.register_tool_result(
        server_name="conduit",
        tool_name="create_decision",
        session_id="session-a",
        result=_result("dec_reconcile"),
    )

    await bridge.reconcile(server)
    await bridge.wait_idle()
    await bridge.reconcile(server)
    await bridge.wait_idle()

    assert calls == 1
    assert wakes == ["session-a"]


@pytest.mark.asyncio
async def test_immediate_read_closes_response_before_registration_race() -> None:
    wakes: list[str] = []

    class Session:
        async def call_tool(self, _name: str, arguments: dict):
            return _result(arguments["decision_id"], "responded")

    server = SimpleNamespace(
        name="conduit",
        session=Session(),
        _rpc_lock=asyncio.Lock(),
        initialize_result=SimpleNamespace(capabilities=SimpleNamespace(
            experimental={CONDUIT_DECISION_RETURN_CAPABILITY: {"version": 1}},
        )),
    )
    bridge = ConduitDecisionReturnBridge(max_origins=2)
    bridge.set_waker(lambda session_id, _content: wakes.append(session_id) or True)
    decision_id = bridge.register_tool_result(
        server_name="conduit",
        tool_name="create_decision",
        session_id="session-race",
        result=_result("dec_race"),
    )
    assert decision_id == "dec_race"

    await bridge.reconcile_decision(server, decision_id)
    await bridge.wait_idle()

    assert wakes == ["session-race"]
    await bridge.handle_notification(server, {
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_race", "stream_seq": 3},
    })
    await bridge.wait_idle()
    assert wakes == ["session-race"]
