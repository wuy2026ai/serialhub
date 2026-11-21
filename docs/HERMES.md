# Using serialhub from Hermes Agent

Lowest-latency wiring of [Hermes Agent](https://github.com/NousResearch/hermes-agent) (Nous Research) to serialhub: one MCP stdio server that Hermes starts once and keeps open, using the 8-tool `lite` profile ([AGENT-LITE.md](AGENT-LITE.md)).

## Prerequisites

- The daemon is already running: `serialhub serve` (default `http://127.0.0.1:8765`). The MCP process is only a thin client; it does not start the daemon.
- The browser page can stay open at the same time; the daemon serves the page and the agent together.

## Config (`~/.hermes/config.yaml`)

```yaml
mcp_servers:
  serialhub:
    command: "serialhub"
    args: ["mcp", "--profile", "lite", "--url", "http://127.0.0.1:8765"]
    lazy: false              # start at Hermes startup, not on first call
    idle_timeout_seconds: 0  # never recycle the process
    timeout: 150             # per tool call, seconds; must exceed your longest `wait`
    connect_timeout: 30
    tools:
      resources: false       # no list_resources / read_resource wrappers
      prompts: false         # no list_prompts / get_prompt wrappers
```

If `serialhub` is not on Hermes' PATH, use the absolute path of the venv script (`.../.venv/bin/serialhub`) as `command`.

A second daemon (e.g. a second COM port on 8766) is a second entry under a different name; tools then appear prefixed by that server name:

```yaml
  serialhub_b:
    command: "serialhub"
    args: ["mcp", "--profile", "lite", "--url", "http://127.0.0.1:8766"]
    lazy: false
    idle_timeout_seconds: 0
    timeout: 150
    tools: {resources: false, prompts: false}
```

Restart Hermes after editing the file.

## Measured latency

Measured on a simulated serial port (socat PTY), median per call. "Before" is the `agent-lite`
branch; "after" is this branch (`perf-latency`). A real USB-serial adapter adds its own delay on top
(FTDI: up to 16 ms by default).

| Path | Before | After |
|---|---|---|
| MCP, kept open: `send` with `wait` | about 207 ms | about 2 ms |
| In-process `send` without `wait` | about 518 ms | about 106 ms (100 ms of silence) |
| In-process `send` without `wait`, port has a matching `prompt` regex | — | about 1 ms |
| MCP, kept open: `read` | about 2 ms | about 2 ms |
| CLI (`serialhub call ...`) process start-up, per call | about 111 ms | about 83 ms |

## Advice for the agent's system prompt

- Always pass `wait` with the expected reply or prompt regex (e.g. `wait="=> $"`); it returns as soon as it matches instead of after a fixed delay.
- Pass the `next=` value from the previous result as `since` in the following call, so you read only new output.
- Use `zsend` / `zrecv` to move files; never paste file contents through `send`.
- Keep `wait` timeouts below the Hermes `timeout` above.

## Why not a shell command per call

Calling `serialhub call ...` through a shell/terminal tool starts a new Python process each time (about 83 ms here) plus a shell round trip and extra LLM tokens for quoting. The MCP server is started once and each call is a plain pipe message, so a call costs a few milliseconds of overhead.

## Verification status

Confirmed against the Hermes MCP documentation: `mcp_servers` in `~/.hermes/config.yaml`; stdio keys `command`/`args`/`env`; `lazy` (default false), `idle_timeout_seconds` (default 0 = never), `timeout`, `connect_timeout`; `tools.resources: false` and `tools.prompts: false` disable the wrapper tools.

Unverified: that a server without recycling keys stays open for the whole session (the docs imply it but do not state it); the "four extra wrapper tools" count; the millisecond figures above are our own measurements, not Hermes'; the tool-name prefix format for multiple servers.

## Sources

- https://hermes-agent.nousresearch.com/docs/user-guide/features/mcp
- https://github.com/NousResearch/hermes-agent
