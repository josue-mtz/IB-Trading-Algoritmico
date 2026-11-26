"""Offline tests for the trade ledger, dashboard controls, and local API."""
import json
import threading
import urllib.request
from datetime import datetime, timezone

import pytest

from src.runtime import state
from src.runtime.ledger import build_trades, compute_stats, mark_open_trade


def _fill(tid, role, side, px, ts, ex=None, qty=10000.0):
    return {"type": "fill", "timestamp": ts, "exec_id": ex or f"{tid}-{role}", "trade_id": tid,
            "role": role, "side": side, "shares": qty, "price": px}


def _orders(*tids):
    return [{"event": "order_placed", "trade_id": t, "id_algoritmo": "ema_cross_trend",
             "symbol": "EURUSD"} for t in tids]


def test_buy_tp_sell_sl_and_open():
    recs = [
        _fill("a", "entry", "BOT", 1.1000, "2026-11-20T10:00:00Z"), _fill("a", "tp", "SLD", 1.1020, "2026-11-20T10:30:00Z"),
        _fill("b", "entry", "SLD", 1.1000, "2026-11-21T10:00:00Z"), _fill("b", "sl", "BOT", 1.1010, "2026-11-21T10:10:00Z"),
        _fill("c", "entry", "BOT", 1.1000, "2026-11-22T10:00:00Z"),
        {"type": "commission", "exec_id": "a-entry", "commission": 1.0},
    ]
    closed, open_ = build_trades(_orders("a", "b", "c"), recs)
    assert [t["trade_id"] for t in closed] == ["a", "b"] and [t["trade_id"] for t in open_] == ["c"]
    a, b = closed
    assert a["exit_reason"] == "TP" and a["pips"] == pytest.approx(20) and a["pnl"] == pytest.approx(20 - 1.0)
    assert b["exit_reason"] == "SL" and b["pips"] == pytest.approx(-10) and b["pnl"] == pytest.approx(-10)
    m = mark_open_trade(open_[0], bid=1.1005, ask=1.1007)
    assert m["pnl"] == pytest.approx(5.0)  # Long positions are marked to the bid.
    assert mark_open_trade({**open_[0], "side": "SELL"}, 1.1005, 1.1007)["pnl"] == pytest.approx(-7.0)


def test_duplicate_exec_ids_ignored():
    recs = [_fill("a", "entry", "BOT", 1.1, "2026-11-20T10:00:00Z"), _fill("a", "entry", "BOT", 1.1, "2026-11-20T10:00:00Z"),
            _fill("a", "tp", "SLD", 1.102, "2026-11-20T10:30:00Z")]
    closed, _ = build_trades(_orders("a"), recs)
    assert closed[0]["qty"] == 10000.0


def test_stats_daily_total_drawdown_and_json_safe():
    recs = []
    for i, (px_out, role) in enumerate([(1.102, "tp"), (1.099, "sl"), (1.099, "sl"), (1.102, "tp")]):
        day = f"2026-11-2{i}T"
        recs += [_fill(f"t{i}", "entry", "BOT", 1.1, day + "10:00:00Z"), _fill(f"t{i}", role, "SLD", px_out, day + "11:00:00Z")]
    closed, _ = build_trades(_orders(*[f"t{i}" for i in range(4)]), recs)
    st = compute_stats(closed, tz=timezone.utc, now=datetime(2026, 11, 23, 12, tzinfo=timezone.utc))
    assert [d["pnl"] for d in st["daily"]] == pytest.approx([20, -10, -10, 20])
    assert st["today"]["pnl"] == pytest.approx(20) and st["today"]["trades"] == 1
    o = st["overall"]
    assert o["pnl"] == pytest.approx(20) and o["max_drawdown"] == pytest.approx(20) and o["winrate"] == 0.5
    assert o["profit_factor"] == pytest.approx(2.0) and o["streak"] == 1
    json.dumps(compute_stats(closed[:1], tz=timezone.utc), allow_nan=False)  # No Infinity or NaN.


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    for name in ("CONTROL_FILE", "EXECUTOR_STATUS_FILE", "ALGO_STATUS_FILE", "FILLS_FILE", "WORKERS_FILE"):
        monkeypatch.setattr(state, name, tmp_path / f"{name}.json")
    return tmp_path


def test_control_accumulates_only_active_time(runtime):
    assert not state.is_trading_enabled()
    c = state.set_trading_enabled(True)
    assert state.is_trading_enabled() and c["first_started_at"]
    c["resumed_at"] = "2026-01-01T00:00:00+00:00"; state.save_control(c)
    c = state.set_trading_enabled(False)
    assert c["accumulated_active_s"] > 1e6 and c["resumed_at"] is None
    before = state.active_seconds(c)
    assert state.active_seconds(c) == before  # The active timer pauses with trading.


def test_dashboard_api_token_and_pause_flow(runtime):
    from dashboard.demo import DemoProcessManager
    from dashboard.server import DashboardApp, serve
    app = DashboardApp(pm=DemoProcessManager(), orders_log=runtime / "orders.jsonl", demo=True)
    httpd = serve(app, "127.0.0.1", 0)
    port = httpd.server_address[1]
    httpd.RequestHandlerClass = __import__("dashboard.server", fromlist=["x"]).make_handler(app, port)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    def call(path, token=None, body=b"{}", host=None):
        req = urllib.request.Request(base + path, data=body if token is not None or path.startswith("/api/") and body else None,
                                     method="POST" if body is not None and path != "/api/state" else "GET")
        if token: req.add_header("X-Dashboard-Token", token)
        if host: req.add_header("Host", host)
        try:
            with urllib.request.urlopen(req) as r: return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    try:
        assert call("/api/state")[1]["state"] == "stopped"
        assert call("/api/start", token="mal")[0] == 403  # Invalid tokens cannot start workers.
        assert call("/api/state", host="evil.com")[0] == 403  # Reject DNS-rebinding host headers.
        assert call("/api/start", token=app.token)[1]["state"]["state"] == "running"
        assert call("/api/variant", token=app.token, body=b'{"variant":"v1"}')[0] == 409  # no cambia en marcha
        assert call("/api/pause", token=app.token)[1]["state"]["state"] == "paused"
        assert call("/api/variant", token=app.token, body=b'{"variant":"v1"}')[1]["state"]["control"]["variant"] == "v1"
        assert call("/api/stop", token=app.token)[1]["state"]["state"] == "stopped"
    finally:
        httpd.shutdown()
