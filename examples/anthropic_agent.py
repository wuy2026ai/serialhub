#!/usr/bin/env python3
"""Minimal agent loop: an LLM drives serial ports through the six serialhub lite tools.

    pip install anthropic          # not in requirements.txt on purpose
    serialhub serve --config config.yaml &
    MODEL=<model> ANTHROPIC_BASE_URL=http://127.0.0.1:8080 ANTHROPIC_API_KEY=x \\
        python examples/anthropic_agent.py "reboot the board on port console and show the banner"

Env: ANTHROPIC_BASE_URL (any Messages-API-compatible endpoint), ANTHROPIC_API_KEY or
ANTHROPIC_AUTH_TOKEN, MODEL, SERIALHUB_URL (default http://127.0.0.1:8765), SERIALHUB_TOKEN.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # run from a checkout

import anthropic  # noqa: E402

from serialhub import lite  # noqa: E402
from serialhub.client import Client  # noqa: E402

SYSTEM = (
    "You operate serial ports through serialhub tools. Start with ports. "
    "Lines are '<seq> <text>'; '>' = sent by you, '#' = marker. "
    "Each block header has next=N: pass N as since next time so you never re-read lines; "
    "more=1 means read again with that since. "
    "Use send with wait (regex for the reply, not your echoed command); wait for later events. "
    "Wait for output lines, not a bare prompt (a prompt has no newline). "
    "Move files with zsend/zrecv (they run rz/sz themselves). "
    "On 'error:' fix the arguments; do not repeat the call."
)
MAX_TURNS = 30


def main(argv: list[str]) -> None:
    task = " ".join(argv).strip() or "List the serial ports."
    model = os.environ.get("MODEL")
    if not model:
        sys.exit("set MODEL to the model name your endpoint serves")
    # The SDK reads ANTHROPIC_BASE_URL, ANTHROPIC_API_KEY and ANTHROPIC_AUTH_TOKEN itself.
    llm = anthropic.Anthropic()
    hub = Client(os.environ.get("SERIALHUB_URL", "http://127.0.0.1:8765"),
                 os.environ.get("SERIALHUB_TOKEN"))
    messages: list = [{"role": "user", "content": task}]
    for _ in range(MAX_TURNS):
        reply = llm.messages.create(model=model, max_tokens=4096, system=SYSTEM,
                                    tools=lite.to_anthropic(), messages=messages)
        for block in reply.content:
            if block.type == "text" and block.text.strip():
                print(block.text)
        if reply.stop_reason != "tool_use":
            if reply.stop_reason != "end_turn":
                print(f"[stopped: {reply.stop_reason}]")
            return
        messages.append({"role": "assistant", "content": reply.content})
        results = []
        for block in reply.content:
            if block.type != "tool_use":
                continue
            text = lite.call(hub, block.name, block.input)
            print(f"[{block.name} {block.input}]\n{text}", file=sys.stderr)
            results.append({"type": "tool_result", "tool_use_id": block.id,
                            "content": text, "is_error": text.startswith("error:")})
        messages.append({"role": "user", "content": results})
    print(f"stopped after {MAX_TURNS} turns", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
