# serialhub lite — a compact agent interface

The 18-tool MCP server returns verbose JSON — costly for a small local model (Qwen-class)
driven through the Anthropic Messages tool format. **lite** is a compact second interface
over the same daemon: six tools, plain-text results, one cursor habit. It is a thin client
(`serialhub/lite.py` over `serialhub/client.py`); `serialhub serve` must be running.
REST, the other CLI commands and the full MCP profile are unchanged.

## Measured schema sizes

Compact JSON (`separators=(",", ":")`) of name + description + input schema per tool
(a test keeps lite at or below 3,000):

| interface | tools | chars |
|---|---|---|
| full MCP profile (`mcp_server.py`) | 18 | 14,957 (18,432 as the raw `tools/list` payload) |
| lite, Anthropic format (`serialhub tools`) | 6 | 2,572 |
| lite, OpenAI format (`serialhub tools --format openai`) | 6 | 2,746 |

## The six tools

| tool | args | result |
|---|---|---|
| `ports` | — | one line per port: `<name> open\|closed <baud> <device> last=<last_seq>` (+ ` err=<last_error>`) |
| `send` | `port`, `data`, `wait?` (regex), `timeout_ms?` (3000, max 60000), `eol?` (`crlf`\|`lf`\|`cr`\|`none`) | `sent <n>B` (+ ` matched`\|` timeout` with `wait`), then the reply block |
| `read` | `port`, `since?`, `limit?` (50, max 200), `filter?` (regex) | lines with seq > `since`; without `since`, the last `limit` lines |
| `wait` | `port`, `regex`, `timeout_ms?` (10000, max 120000), `since?` | `matched` + the line, or `timeout` |
| `search` | `port`, `regex`, `limit?` (20, max 100), `context?` (0-5) | hits over the whole buffer; windows separated by `--` |
| `control` | `port`, `action` (`open`\|`close`\|`clear`\|`mark`), `baud?`, `label?` | `open 115200` / `closed` / `cleared 12` / `mark 345` |

- `send` collects **rx** lines that arrive after the write: until `wait` matches, or (no
  `wait`) until 300 ms pass with no new line, or until `timeout_ms`.
- `wait` without `since` only considers lines arriving after the call; pass `since` to
  also check buffered lines after that seq.
- Out-of-range `limit`/`timeout_ms`/`context` are clamped; `"50"` is accepted as 50.
- Every failure (unknown tool/port, bad regex, missing or wrongly typed argument, daemon
  unreachable) is returned as text starting with `error: ` — `call()` never raises.

## Line format (send / read / wait / search)

```
<port> <first_seq>-<last_seq> next=<cursor> more=<0|1>
<seq> <text>            received line (rx)
<seq> ><text>           sent by the host (tx)
<seq> #<text>           marker
<seq> <text> (x12)      12 consecutive identical lines collapsed; seq is the first one
```

- Pass `next=` as `since` on the next `read` — you never see a line twice. An empty
  result is `<port> empty next=<cursor> more=0`.
- `more=1`: more lines are waiting (the `limit` was reached, or the 4000-char block cap
  was hit); `read` again with `since=<next>`. No lines are lost or repeated.
- No timestamps; ANSI escapes are stripped; lines over 240 chars become
  `<first 240 chars>…+<dropped>`.

Real output (`loop://` echoes what is written):

```
> send {"port":"console","data":"uname -a","wait":"uname"}
sent 10B matched
console 11-11 next=11 more=0
11 uname -a
```

## Three ways to use it

### 1. MCP, lite profile

`serialhub mcp --profile lite` registers exactly the six tools (same names, descriptions
and schemas) and returns the same text. URL/token: `--url`/`--token`, else the
`SERIALHUB_URL`/`SERIALHUB_TOKEN` environment variables. `.mcp.json`:

```json
{
  "mcpServers": {
    "serialhub": {
      "command": "python",
      "args": ["-m", "serialhub", "mcp", "--profile", "lite"],
      "env": { "SERIALHUB_URL": "http://127.0.0.1:8765" }
    }
  }
}
```

Run it from the repo or after `pip install -e .`; `--profile full` (default) is [`MCP.md`](MCP.md).

### 2. Your own loop: `tools` + `call`

```bash
serialhub tools                    # Anthropic tool array, one line of JSON (no daemon needed)
serialhub tools --format openai    # {"type":"function","function":{...}} entries
serialhub call ports
serialhub call send '{"port":"console","data":"help","wait":"ready"}'
```

Unlike other subcommands there is **no** `{"ok":..}` envelope: `tools` prints a raw JSON
array, `call` plain text (exit 1 on `error:`). From Python: `lite.to_anthropic()` /
`lite.to_openai()` and `lite.call(Client(url, token), name, args)`.

### 3. The example agent

[`examples/anthropic_agent.py`](../examples/anthropic_agent.py) (`pip install anthropic`):
while `stop_reason == "tool_use"`, run each `tool_use` block through `lite.call()` and
answer with a `tool_result` (`is_error` when the text starts with `error:`).

```bash
MODEL=<name> ANTHROPIC_BASE_URL=http://127.0.0.1:8080 ANTHROPIC_API_KEY=x \
  python examples/anthropic_agent.py "open port console and show the boot banner"
```

## System prompt snippet

```text
You operate serial ports through serialhub tools. Start with ports.
Output lines are '<seq> <text>'; '>' marks text you sent, '#' a marker.
Each block header has next=N: pass N as since on your next read so you
never re-read old lines. more=1 means call read again with that since.
Use send with wait (a regex) to get a command's reply; use wait for later events.
Results starting with 'error:' mean the call failed: fix the arguments, do not repeat it.
```
