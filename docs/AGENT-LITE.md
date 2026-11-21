# serialhub lite — a compact agent interface

**lite** is a compact second interface for small local models (Qwen-class) using the
Anthropic Messages tool format: six tools, plain-text results, one cursor habit. A thin
client (`serialhub/lite.py`); `serialhub serve` must be running. Nothing else changes.

## Measured schema sizes

Compact JSON of name + description + input schema per tool (a test keeps lite ≤ 3,000):

| interface | tools | chars |
|---|---|---|
| full MCP profile (`mcp_server.py`) | 18 | 14,957 (18,432 as the raw `tools/list` payload) |
| lite, Anthropic format (`serialhub tools`) | 6 | 2,648 |
| lite, OpenAI format (`serialhub tools --format openai`) | 6 | 2,822 |

## The six tools

| tool | args | result |
|---|---|---|
| `ports` | — | one line per port: `<name> open\|closed <baud> <device> last=<last_seq>` (+ ` err=<last_error>`) |
| `send` | `port`, `data`, `wait?` (regex), `timeout_ms?` (3000, max 60000), `eol?` (`crlf`\|`lf`\|`cr`\|`none`; default: the port's `eol`) | `sent <n>B` (+ ` matched`\|` timeout` with `wait`), then the reply block |
| `read` | `port`, `since?`, `limit?` (50, max 200), `filter?` (regex) | lines with seq > `since`; without `since`, the latest `limit` lines |
| `wait` | `port`, `regex`, `timeout_ms?` (10000, max 120000), `since?` | `matched` + every line up to the match, or `timeout` |
| `search` | `port`, `regex`, `limit?` (20, max 100), `context?` (0-5) | hits over the whole buffer; windows separated by `--`; **does not move the cursor** |
| `control` | `port`, `action` (`open`\|`close`\|`clear`\|`mark`), `baud?`, `label?` | `open 115200` / `closed` / `cleared 12` / `mark 345` |

- `send` collects **rx** lines after the write until `wait` matches, or (no `wait`) 300 ms
  of silence, or `timeout_ms`. `wait` ignores the first rx line ending with `data` (the
  device's echo, maybe prompt-prefixed: `root@dev:~# uname -a`); it is still shown.
- `wait` without `since` only considers lines arriving after the call. On a match it shows
  all lines in (since or call start, match]; `next=` is the matched seq.
- Regexes: `send`'s `wait` matches ANSI-stripped text; `wait`/`search`/`filter` run in the
  daemon on raw text (ANSI escapes included). Output is always stripped.
- Out-of-range values are clamped; `"50"` counts as 50; failures are text `error: ...`.

## Line format (send / read / wait)

```
<port> <first_seq>-<last_seq> next=<cursor> more=<0|1>[ skipped=<n>][ dropped=<n>][ reset=1]
<seq> <text>            received line (rx)
<seq> ><text>           sent by the host (tx)
<seq> #<text>           marker
<seq> <text> (x12)      12 adjacent identical lines collapsed; seq is the first one
```

- Pass `next=` as `since` next time — no line is shown twice. Empty: `<port> empty next=<cursor> more=0`.
  seq is global across all ports, so `<first>-<last>` is a range, not a line count.
- `more=1`: more lines wait (the `limit` or the 4000-char cap was hit); read again with
  `since=<next>` — nothing is lost or repeated.
- `skipped=<n>`: `read` without `since` and `wait` keep the newest lines; `n` were left out.
- `dropped=<n>`: `n` lines after your cursor were evicted from the ring before you read them.
- `reset=1`: `since` was beyond the buffer (e.g. daemon restarted, seq began again); the
  latest lines are returned instead.
- `search` header: `<port> hits=<n> more=<0|1>` (no `next=`: search is not a cursor read).
- Collapse is for unfiltered blocks only. No timestamps; lines over 240 chars become `<240 chars>…+<n>`.

Real output (`loop://` echoes what is written; the reply line was scripted):

```
> send {"port":"console","data":"uname -a","wait":"Linux"}
sent 10B matched
console 11-12 next=12 more=0
11 uname -a
12 Linux dev 6.1.0
```

## Prompts are not lines

Default `framing: line` emits a line only at a line ending, so a bare prompt (`login: `,
`# `) never appears and a `wait` for it times out. Wait for output lines, or set
`framing: idle` (with `flush_ms`) on ports that drive interactive shells.

## Three ways to use it

### 1. MCP, lite profile

`serialhub mcp --profile lite`: the same six tools and text (`error:` results flagged
`isError` where the SDK keeps the text identical). `--url`/`--token`, else `SERIALHUB_URL`/`SERIALHUB_TOKEN`:

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

### 2. Your own loop: `tools` + `call`

```bash
serialhub tools                    # Anthropic tool array, one line of JSON (no daemon needed)
serialhub call send '{"port":"console","data":"help","wait":"ready"}'
```

No `{"ok":..}` envelope here: `tools` prints a raw JSON array, `call` plain text (exit 1 on
`error:`; URL from `--url`, else `SERIALHUB_URL`). From Python: `lite.to_anthropic()` /
`lite.to_openai()` and `lite.call(Client(url, token), name, args)`.

### 3. The example agent

[`examples/anthropic_agent.py`](../examples/anthropic_agent.py) (`pip install anthropic`;
env `MODEL`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY`, `SERIALHUB_URL`): each `tool_use`
block goes through `lite.call()` and back as a `tool_result` (`is_error` on `error:`).

## System prompt snippet

```text
You operate serial ports through serialhub tools. Start with ports.
Lines are '<seq> <text>'; '>' = sent by you, '#' = marker.
Each block header has next=N: pass N as since next time so you never re-read lines;
more=1 means read again with that since.
Use send with wait (regex for the reply, not your echoed command); wait for later events.
Wait for output lines, not a bare prompt (a prompt has no newline).
On 'error:' fix the arguments; do not repeat the call.
```
