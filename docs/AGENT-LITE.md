# serialhub lite — a compact agent interface

**lite** is a compact second interface for small local models (Qwen-class) using the
Anthropic Messages tool format: eight tools, plain-text results, one cursor habit. A thin
client (`serialhub/lite.py`); `serialhub serve` must be running. Nothing else changes.

## Measured schema sizes

Compact JSON of name + description + input schema per tool (a test keeps lite ≤ 4,200):

| interface | tools | chars |
|---|---|---|
| full MCP profile (`mcp_server.py`) | 20 | 18,244 |
| lite, Anthropic format (`serialhub tools`) | 8 | 4,192 |
| lite, OpenAI format (`serialhub tools --format openai`) | 8 | 4,424 |

The six serial tools alone are 2,648 chars; `zsend` + `zrecv` add 1,544.

## The eight tools

| tool | args | result |
|---|---|---|
| `ports` | — | one line per port: `<name> open\|closed <baud> <device> last=<last_seq>` (+ ` err=<last_error>`) |
| `send` | `port`, `data`, `wait?` (regex), `timeout_ms?` (3000, max 60000), `eol?` (`crlf`\|`lf`\|`cr`\|`none`; default: the port's `eol`) | `sent <n>B` (+ ` matched`\|` timeout` with `wait`), then the reply block |
| `read` | `port`, `since?`, `limit?` (50, max 200), `filter?` (regex) | lines with seq > `since`; without `since`, the latest `limit` lines |
| `wait` | `port`, `regex`, `timeout_ms?` (10000, max 120000), `since?` | `matched` + every line up to the match, or `timeout` |
| `search` | `port`, `regex`, `limit?` (20, max 100), `context?` (0-5) | hits over the whole buffer; windows separated by `--`; **does not move the cursor** |
| `control` | `port`, `action` (`open`\|`close`\|`clear`\|`mark`), `baud?`, `label?` | `open 115200` / `closed` / `cleared 12` / `mark 345` |
| `zsend` | `port`; `path` (array of local paths; a bare string is accepted) **or** `content` + `name` (`encoding?`: `text` default, or `base64`); `overwrite?` (true); `cmd?` (`rz`; `""` = none); `timeout_ms?` (auto, max 1800000) | one line per file: `sent <name> <bytes>B crc=<CRC32> in <s>s` |
| `zrecv` | `port`, `remote` (array of device names; a bare string is accepted), `dir?` (cwd if writable, else temp), `inline?` (false), `overwrite?` (false), `cmd?` (`sz {remote}`), `timeout_ms?` (auto) | one line per file: `received <name> <bytes>B crc=<CRC32> -> <absolute path>`; with `inline`, `--- <name>` + content for each text file |

- `send` collects **rx** lines after the write until `wait` matches, or (no `wait`) 300 ms
  of silence, or `timeout_ms`. `wait` ignores the first rx line ending with `data` (the
  device's echo, maybe prompt-prefixed: `root@dev:~# uname -a`); it is still shown.
- `wait` without `since` only considers lines arriving after the call. On a match it shows
  all lines in (since or call start, match]; `next=` is the matched seq.
- Regexes: `send`'s `wait` matches ANSI-stripped text; `wait`/`search`/`filter` run in the
  daemon on raw text (ANSI escapes included). Output is always stripped.
- Out-of-range values are clamped; `"50"` counts as 50; failures are text `error: ...`.

## File transfer (zsend / zrecv)

ZMODEM through the daemon's `/ws` relay, with a pure-Python engine running **in the tool's
own process** (`serialhub/zmodem_proto.py` + `zmodem_client.py`): no browser, no Node, no
host lrzsz, and local paths are on the machine where the agent runs (the daemon may be
remote). The tools send `rz` / `sz <remote>` themselves — do not `send` them first.

- `zsend`: exactly one of `path` / `content`. `content` is the file body written by the
  model (UTF-8 text, or `encoding: "base64"`), at most 1 MiB decoded; `name` is reduced to a
  basename. `cmd: ""` when the device is already waiting in `rz`. `overwrite` (default true)
  asks the receiver to replace an existing file (ZMODEM `ZMCLOB`); if it still refuses, the
  line is `skipped <name>: device refused it (already exists? try cmd "rz -y")`, and a reply
  where nothing was sent starts with `error: nothing was sent`.
- `zrecv` always saves into `dir` (created if missing; default: the current directory when
  writable, else the system temp directory), only under the basename the device sent (names
  with `:`, Windows device names like `CON`/`COM1`, or trailing dots/spaces are refused); an
  existing file is kept and the new one saved as `name.1`, `name.2`, … unless `overwrite`.
  Files land under a temporary name and are renamed when complete; partial files are removed
  on failure. Saved paths are always absolute. `{remote}` in `cmd` is the names joined by
  spaces (quoted when they contain spaces).
- `inline: true` appends the content of text files (first 64 KiB, no NUL, fewer than 5 %
  undecodable bytes — shown with replacement characters); other files get
  ` (binary, not shown)` on their line. Inline content is capped at 4000 chars per reply; a
  cut file ends with `…+<n> chars, full file at <saved path>`.
- `crc=` is CRC-32 (zlib) of the file bytes, so the agent can check a transfer end to end.
- **Timeout**: omitted = automatic, `bytes × 10 / baud × 1.5 + 30 s` (zrecv extends it when
  the device announces each file's size), at most 30 min. On expiry:
  `error: timed out at <n>/<m> bytes after <t>s; at <baud> baud this needs ≈<x>s; retry with timeout_ms>=<y>`.
- **Low baud**: the sender paces itself from the port's baud — one ZCRCQ acknowledgement per
  ~1 s of line time (1–8 KiB) and at most four of those unacknowledged — and waits for an
  acknowledgement as long as the bytes still on the line need, so a 9600-baud line is not
  flooded. 1 MiB takes ≈ 95 s at 115200 and ≈ 18 min at 9600.
- **Partial results**: when a transfer fails after some files, those files are listed first
  and the last line is the `error:`.
- **Errors**: `error: device did not start ZMODEM; device said: <last device lines>`
  (nothing started within 10 s); `error: device sent no file (missing/unreadable remote
  file?); device said: sz done rc=128` (lrzsz's `sz` starts a session even for a missing
  file, then cancels); `error: device cancelled the transfer; device said: …` /
  `error: device ended the transfer (its rz/sz exited)…`; `error: ZMODEM session on 'a' was
  claimed by another client (a browser tab?); close it and retry in ~5 s`; wrong direction
  (`… retry in ~5 s`); `error: zmodem: session active on port 'a'` (HTTP 409); daemon
  unreachable; timeout (above). `device said:` only ever quotes the device's own output,
  never the shell's reaction to protocol bytes.
- **The shell stays clean.** The engine writes nothing until the device's program has
  spoken (a cancelling `sz` gets no reply at all). After any failure the client writes the
  abort sequence once (only if the device's program may still be running), waits until the
  device is quiet, writes one line terminator to flush the device's input line, discards the
  shell's reply to it, and returns the device's own text to the ring buffer. The terminator is
  the per-port setting `zmodem_cleanup_eol`: `cr` (default) | `lf` | `crlf` | `ctrl-u-cr` |
  `none` — use `ctrl-u-cr` or `none` on U-Boot-style shells, which repeat the last command on
  an empty line.
- After a transfer the device's own output (`rz done rc=0`, the prompt) is returned to the
  ring buffer as normal lines, so `read` shows it.

An agent round trip, then a failure followed by one clean `ping` (real output from the
simulator, mock device running lrzsz; temp dir shown as `/work`):

```
> zsend {"port": "console", "content": "mode=fast\n", "name": "agent.cfg"}
sent agent.cfg 10B crc=195BE1DD in 0.0s
> zrecv {"port": "console", "remote": ["agent.cfg", "fw.bin"], "dir": "pulled", "inline": true}
received agent.cfg 10B crc=195BE1DD -> /work/pulled/agent.cfg
received fw.bin 262144B crc=6618A177 -> /work/pulled/fw.bin (binary, not shown)
--- agent.cfg
mode=fast

> zrecv {"port": "console", "remote": "nope.bin"}
error: device sent no file (missing/unreadable remote file?); device said: sz done rc=128
> send {"port": "console", "data": "ping", "wait": "pong"}
sent 6B matched
console 14-14 next=14 more=0
14 pong
```

CLI: `serialhub zsend <port> <file>... [--content TEXT|- --name NAME] [--no-overwrite]
[--cmd CMD] [--timeout-ms N]` and `serialhub zrecv <port> <remote>... [--dir DIR] [--cmd CMD]
[--overwrite] [--inline] [--timeout-ms N]` print the same text (progress on stderr only when
it is a TTY; `--content -` reads stdin as bytes; exit 1 on an `error:`, `skipped` or
`not received` line). The full MCP profile has the same operations as `zmodem_send` (`path`,
`content`, `name`, `encoding`, `overwrite`, `cmd`, `timeout_ms`) / `zmodem_receive` (`remote`,
`dir`, `inline`, `overwrite`, `cmd`, `timeout_ms`) with the usual `{"ok": ...}` envelope.

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

`serialhub mcp --profile lite`: the same eight tools and text (`error:` results flagged
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
Move files with zsend/zrecv (they run rz/sz themselves).
On 'error:' fix the arguments; do not repeat the call.
```
