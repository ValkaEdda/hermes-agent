# Conduit Decision Return

This optional bundled plugin binds Conduit Decision Returns to Hermes' exact
originating interactive CLI session. It does not grant effect authority and it
does not poll. The MCP adapter performs one canonical `get_decision` read on
notification (and once after reconnect for outstanding origins), then injects
that canonical result only if the recorded Hermes session is still active.

Enable the plugin and the matching MCP server option:

```yaml
plugins:
  enabled:
    - conduit-decision-return

mcp_servers:
  conduit:
    command: conduit-mcp
    decision_return: true
```

Both legs must advertise the exact experimental capability
`io.conduit/decision-return: {version: 1}`. Without either opt-in, Hermes uses
its ordinary MCP client unchanged.
