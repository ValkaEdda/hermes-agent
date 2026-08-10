"""Exact-session wake binding for the optional Conduit MCP Return adapter."""

from __future__ import annotations

from typing import Any

from tools.conduit_decision_return import decision_return_bridge


def register(ctx: Any) -> None:
    decision_return_bridge.set_waker(ctx.inject_message_for_session)

    def forget_finalized_session(session_id: str = "", **_: Any) -> None:
        decision_return_bridge.forget_session(session_id)

    ctx.register_hook("on_session_finalize", forget_finalized_session)
