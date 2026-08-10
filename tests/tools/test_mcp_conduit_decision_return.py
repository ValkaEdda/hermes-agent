from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tools import mcp_tool
from tools.conduit_decision_return import (
    ConduitDecisionReturnNotification,
    ConduitServerNotification,
    decision_return_bridge,
)


@pytest.mark.asyncio
async def test_opted_in_message_handler_routes_closed_return_notification() -> None:
    server = mcp_tool.MCPServerTask("conduit")
    server._config = {"decision_return": True}
    notification = ConduitDecisionReturnNotification.model_validate({
        "method": "notifications/conduit/decision-return",
        "params": {"version": 1, "decision_id": "dec_routed", "stream_seq": 3},
    })
    wrapped = ConduitServerNotification(root=notification)

    with patch.object(
        decision_return_bridge, "handle_notification", new=AsyncMock()
    ) as handled:
        await server._make_message_handler()(wrapped)

    handled.assert_awaited_once_with(server, notification)


@pytest.mark.asyncio
async def test_opted_in_message_handler_preserves_stock_list_changed() -> None:
    from mcp.types import ToolListChangedNotification

    server = mcp_tool.MCPServerTask("conduit")
    server._config = {"decision_return": True}
    wrapped = ConduitServerNotification(
        root=ToolListChangedNotification(
            method="notifications/tools/list_changed"
        )
    )

    with patch.object(
        mcp_tool.MCPServerTask, "_schedule_tools_refresh"
    ) as scheduled:
        await server._make_message_handler()(wrapped)

    scheduled.assert_called_once_with()


def _run_mcp_call(coro_or_factory, timeout=30):
    coro = coro_or_factory() if callable(coro_or_factory) else coro_or_factory
    loop = asyncio.new_event_loop()
    try:
        async def run():
            server = mcp_tool._servers["conduit"]
            server._rpc_lock = asyncio.Lock()
            return await coro
        return loop.run_until_complete(run())
    finally:
        loop.close()


def test_create_result_is_correlated_before_handler_returns() -> None:
    result = SimpleNamespace(
        isError=False,
        content=[],
        structuredContent={
            "decision": {
                "request": {"id": "dec_origin"},
                "response": None,
                "status": "ready",
            }
        },
    )
    session = SimpleNamespace(call_tool=AsyncMock(return_value=result))
    server = SimpleNamespace(
        name="conduit",
        session=session,
        _rpc_lock=None,
        _config={"decision_return": True},
    )
    with patch.dict(mcp_tool._servers, {"conduit": server}), patch(
        "tools.mcp_tool._run_on_mcp_loop", side_effect=_run_mcp_call
    ), patch.object(
        decision_return_bridge,
        "register_tool_result",
        return_value="dec_origin",
    ) as registered, patch.object(
        decision_return_bridge,
        "reconcile_decision",
        new=AsyncMock(),
    ) as reconciled:
        handler = mcp_tool._make_tool_handler("conduit", "create_decision", 30.0)
        payload = json.loads(handler({}, session_id="session-origin"))

    assert payload["result"]["decision"]["request"]["id"] == "dec_origin"
    registered.assert_called_once_with(
        server_name="conduit",
        tool_name="create_decision",
        session_id="session-origin",
        result=result,
    )
    reconciled.assert_awaited_once_with(server, "dec_origin")


def test_decision_return_client_session_is_strictly_opt_in() -> None:
    assert mcp_tool._mcp_client_session_class({}) is mcp_tool.ClientSession
    assert (
        mcp_tool._mcp_client_session_class({"decision_return": True})
        is not mcp_tool.ClientSession
    )
    assert (
        mcp_tool._mcp_client_session_class({"decision_return": "true"})
        is mcp_tool.ClientSession
    )
