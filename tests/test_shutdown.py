"""``POST /api/shutdown`` (Web UI "Exit" button): config, auth, CSRF and exit wiring.

TestClient-level tests: the real uvicorn exit path lives in
``tests/integration/test_shutdown_live.py``.
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from serialhub.config import Config
from serialhub.server import create_app


def _until(pred, timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return pred()


@pytest.fixture
def make_app(hub_factory, make_config):
    """``make_app(token=None, allow_shutdown=True, exit_cb=None)`` -> (TestClient, hub, calls)."""
    clients: list[TestClient] = []

    def _make(*, token=None, allow_shutdown=True, with_exit=True):
        cfg = make_config(token=token)
        cfg.server.allow_shutdown = allow_shutdown
        hub = hub_factory(cfg)
        app = create_app(hub, cfg)
        calls: list[float] = []
        if with_exit:
            app.state.request_exit = lambda: calls.append(time.time())
        tc = TestClient(app)
        tc.__enter__()
        clients.append(tc)
        return tc, hub, calls

    yield _make
    for tc in clients:
        tc.__exit__(None, None, None)


# ---- config ---------------------------------------------------------------- #
def test_allow_shutdown_defaults_true_and_round_trips() -> None:
    cfg = Config.from_dict({"ports": []})
    assert cfg.server.allow_shutdown is True
    assert cfg.to_dict()["server"]["allow_shutdown"] is True

    off = Config.from_dict({"server": {"allow_shutdown": False}, "ports": []})
    assert off.server.allow_shutdown is False
    dumped = off.to_dict(redact=False)
    assert dumped["server"]["allow_shutdown"] is False
    again = Config.from_dict({k: v for k, v in dumped.items() if k != "path"})
    assert again.server.allow_shutdown is False
    # merge_overrides keeps the flag
    assert off.merge_overrides(port=9999).server.allow_shutdown is False


def test_allow_shutdown_must_be_bool() -> None:
    with pytest.raises(ValueError, match="server.allow_shutdown"):
        Config.from_dict({"server": {"allow_shutdown": "nope"}, "ports": []})


# ---- exposure to the UI ----------------------------------------------------- #
def test_health_and_config_expose_allow_shutdown(make_app) -> None:
    tc, _hub, _calls = make_app(allow_shutdown=False)
    assert tc.get("/health").json()["allow_shutdown"] is False
    assert tc.get("/api/config").json()["config"]["server"]["allow_shutdown"] is False
    tc2, _hub2, _ = make_app()
    assert tc2.get("/health").json()["allow_shutdown"] is True


# ---- refusals --------------------------------------------------------------- #
def test_no_exit_callback_is_503(make_app) -> None:
    tc, hub, _calls = make_app(with_exit=False)
    r = tc.post("/api/shutdown")
    assert r.status_code == 503
    body = r.json()
    assert body["ok"] is False and body["error"]
    time.sleep(0.4)
    assert hub.status("t")["open"] is True  # nothing was stopped


def test_disabled_is_403(make_app) -> None:
    tc, hub, calls = make_app(allow_shutdown=False)
    r = tc.post("/api/shutdown")
    assert r.status_code == 403
    assert r.json() == {"ok": False, "error": "shutdown disabled"}
    time.sleep(0.4)
    assert calls == [] and hub.status("t")["open"] is True


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
def test_token_missing_or_wrong_is_401(make_app, headers) -> None:
    tc, _hub, calls = make_app(token="s3cret")
    r = tc.post("/api/shutdown", headers=headers)
    assert r.status_code == 401
    assert r.json()["ok"] is False
    time.sleep(0.4)
    assert calls == []


@pytest.mark.parametrize("origin", [
    "http://evil.example", "http://testserver:8080", "null", "https://testserver.evil",
])
def test_origin_mismatch_is_403(make_app, origin) -> None:
    tc, _hub, calls = make_app()
    r = tc.post("/api/shutdown", headers={"Origin": origin})
    assert r.status_code == 403
    assert r.json()["ok"] is False
    time.sleep(0.4)
    assert calls == []


def _request(headers: dict[str, str]):
    from starlette.requests import Request

    return Request({"type": "http", "method": "POST", "path": "/api/shutdown",
                    "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                                for k, v in headers.items()]})


@pytest.mark.parametrize("origin,host,ok", [
    ("http://h:8765", None, False),                    # no Host header at all
    ("http://[::1]:8765", "[::1]:8765", True),         # IPv6 literal
    ("http://[::1]:8765", "[::2]:8765", False),
    ("http://[::1]:8765", "[::1]:8766", False),
    ("http://LOCALHOST:8765", "localhost:8765", True),  # host names are case-insensitive
    ("http://localhost:8765", "LocalHost:8765", True),
    ("http://h", "h:80", True),                        # default port, either side
    ("http://h", "h", True),
    ("http://h:80", "h", True),
    ("https://h", "h:443", True),
    ("https://h", "h:80", False),
    ("http://h:99999", "h:99999", False),              # malformed port
    ("http://h:abc", "h:abc", False),
    ("http://h:8765", "h:abc", False),
    ("null", "h:8765", False),                         # sandboxed iframe / file:// page
    ("", "h:8765", False),
], ids=["no-host", "ipv6", "ipv6-other-host", "ipv6-other-port", "upper-origin", "upper-host",
        "default-port-host-80", "default-port-bare", "explicit-80-bare-host", "https-443",
        "https-vs-80", "port-out-of-range", "port-not-a-number", "host-port-malformed", "null",
        "empty"])
def test_same_origin_cases(origin, host, ok) -> None:
    from serialhub.server import _same_origin

    headers = {"Origin": origin}
    if host is not None:
        headers["Host"] = host
    assert _same_origin(_request(headers)) is ok


def test_ipv6_same_origin_is_accepted_end_to_end(make_app) -> None:
    tc, _hub, calls = make_app()
    r = tc.post("/api/shutdown", headers={"Host": "[::1]:8765", "Origin": "http://[::1]:8765"})
    assert r.status_code == 200, r.text
    assert _until(lambda: calls, 5.0)


# ---- accepted ----------------------------------------------------------------- #
@pytest.mark.parametrize("headers", [
    {},                                   # non-browser client
    {"Origin": "http://testserver"},      # same-origin browser page
])
def test_accepted_stops_hub_then_requests_exit(make_app, headers) -> None:
    tc, hub, calls = make_app()
    sub = hub.subscribe()
    t0 = time.time()
    r = tc.post("/api/shutdown", headers=headers)
    assert r.status_code == 200
    assert r.json() == {"ok": True, "data": {"stopping": True}}
    assert calls == []  # the response goes out first
    assert _until(lambda: calls, 5.0)
    assert calls[0] - t0 >= 0.15
    assert len(calls) == 1
    assert hub.status("t")["open"] is False
    seen = []
    while not sub.empty():
        seen.append(sub.get_nowait())
    assert {"type": "shutdown"} in seen


def test_accepted_with_valid_token(make_app) -> None:
    tc, _hub, calls = make_app(token="s3cret")
    r = tc.post("/api/shutdown", headers={"Authorization": "Bearer s3cret"})
    assert r.status_code == 200 and r.json()["data"]["stopping"] is True
    assert _until(lambda: calls, 5.0)


def test_second_request_does_not_exit_twice(make_app) -> None:
    tc, _hub, calls = make_app()
    assert tc.post("/api/shutdown").status_code == 200
    assert tc.post("/api/shutdown").status_code == 200
    assert _until(lambda: calls, 5.0)
    time.sleep(0.5)
    assert len(calls) == 1


def test_websocket_receives_shutdown(make_app) -> None:
    tc, _hub, calls = make_app()
    with tc.websocket_connect("/ws") as ws:
        assert ws.receive_json()["type"] == "hello"
        assert tc.post("/api/shutdown").status_code == 200
        for _ in range(200):
            msg = ws.receive_json()
            if msg.get("type") == "shutdown":
                break
        else:
            raise AssertionError("no shutdown message")
        assert msg == {"type": "shutdown"}
    assert _until(lambda: calls, 5.0)
