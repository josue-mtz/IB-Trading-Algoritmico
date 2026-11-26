"""Serve the local dashboard and its small JSON API using Python's standard library.

The server is intended for local use only. It binds to loopback, checks the
Host header, and requires a per-session token for state-changing requests.
"""
import csv
import json
import secrets
import socket
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from config.settings import (
    BASE_DIR, DASHBOARD_MANAGE_PRODUCER, DASHBOARD_TZ, IB_HOST, IB_PORT,
    KAFKA_BOOTSTRAP_SERVERS, LOGS_DIR, REPORTS_DIR,
)
from src.algorithms.variants import VARIANTS
from src.runtime import state
from src.runtime.ledger import build_trades, compute_stats, mark_open_trade
from src.runtime.process_manager import ProcessManager

INDEX_HTML = Path(__file__).parent / "index.html"
MAX_TRADES_SENT = 300
MAX_EVENTS_SENT = 60


def _resolve_tz(name: str):
    """Resolve an optional IANA time-zone name."""
    if not name:
        return None  # Use the system time zone.
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:
        return None


def tcp_open(host: str, port: int, timeout: float = 0.4) -> bool:
    """Return whether a TCP service accepts a short connection attempt."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _num(value) -> Optional[float]:
    """Convert a JSON value to a number, returning None when it is not numeric."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_backtest_summary(reports_dir: Path = REPORTS_DIR) -> dict:
    """Load per-variant in-sample and out-of-sample metrics from the newest report."""
    files = sorted(Path(reports_dir).glob("variants_comparison_*/comparison.csv"))
    if not files:
        return {}
    out = {}
    with open(files[-1], newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row["variant"]] = {
                "source": files[-1].parent.name,
                **{k: _num(v) for k, v in row.items() if k not in ("variant", "id_algoritmo")},
            }
    return out


class DashboardApp:
    """Coordinate dashboard actions and assemble data for the browser interface."""

    def __init__(self, pm=None, orders_log: Path = LOGS_DIR / "orders.jsonl",
                 manage_producer: bool = DASHBOARD_MANAGE_PRODUCER, demo: bool = False,
                 tz_name: str = DASHBOARD_TZ):
        self.pm = pm or ProcessManager()
        self.orders_log = Path(orders_log)
        self.manage_producer = manage_producer
        self.demo = demo
        self.tz = _resolve_tz(tz_name)
        self.token = secrets.token_urlsafe(24)
        self._lock = threading.Lock()

    # ---------- User actions ----------

    def start(self) -> dict:
        with self._lock:
            variant = state.load_control()["variant"]
            self.pm.start_all(variant, manage_producer=self.manage_producer)
            state.set_trading_enabled(True)
        return self.build_state()

    def pause(self) -> dict:
        with self._lock:
            state.set_trading_enabled(False)
        return self.build_state()

    def stop(self) -> dict:
        with self._lock:
            state.set_trading_enabled(False)
            self.pm.stop_all()
        return self.build_state()

    def set_variant(self, variant: str) -> dict:
        if variant not in VARIANTS:
            raise ValueError(f"Unknown strategy variant: {variant}")
        with self._lock:
            if state.load_control()["trading_enabled"]:
                raise PermissionError("Pause trading before changing the strategy.")
            state.set_variant(variant)
        return self.build_state()

    # ---------- Dashboard state ----------

    def build_state(self) -> dict:
        now = datetime.now(timezone.utc)
        control = state.load_control()
        workers = self.pm.status()
        exec_status = state.read_json(state.EXECUTOR_STATUS_FILE, {}) or {}
        algo_status = state.read_json(state.ALGO_STATUS_FILE, {}) or {}

        orders = state.read_jsonl(self.orders_log)
        records = state.read_jsonl(state.FILLS_FILE)
        closed, open_ = build_trades(orders, records)
        stats = compute_stats(closed, self.tz, now)
        bid, ask = _num(exec_status.get("bid")), _num(exec_status.get("ask"))
        open_marked = [mark_open_trade(t, bid, ask) for t in open_]

        def age(iso):
            dt = state.parse_iso(iso)
            return None if dt is None else max(0.0, (now - dt).total_seconds())

        enabled = bool(control["trading_enabled"])
        any_alive = any(w["alive"] for w in workers.values())
        if enabled and not (workers["executor"]["alive"] and workers["algorithm"]["alive"]):
            overall = "degraded"
        elif enabled:
            overall = "running"
        elif any_alive:
            overall = "paused"
        else:
            overall = "stopped"

        kafka_host, _, kafka_port = KAFKA_BOOTSTRAP_SERVERS.split(",")[0].partition(":")
        health = {
            "kafka": True if self.demo else tcp_open(kafka_host, int(kafka_port or 9092)),
            "ib_port": True if self.demo else tcp_open(IB_HOST, IB_PORT),
            "ib_connected": bool(exec_status.get("ib_connected")),
            "accounts": exec_status.get("accounts", []),
            "executor_age_s": age(exec_status.get("updated_at")),
            "velas_age_s": age(algo_status.get("updated_at")),
        }

        tz_name = getattr(self.tz, "key", None) or str(now.astimezone().tzinfo)

        def day_of(iso):
            dt = state.parse_iso(iso)
            return dt.astimezone(self.tz).date().isoformat() if dt else None

        trades = [{**t, "day": day_of(t["exit_time"])} for t in reversed(closed[-MAX_TRADES_SENT:])]
        events = [
            e for e in orders if e.get("event") in
            ("order_placed", "rejected", "error", "ignored_paused", "signal_received")
        ][-MAX_EVENTS_SENT:][::-1]

        backtest = load_backtest_summary()
        variants = [
            {"variant": k, "id": v["id_algoritmo"], "label": v["label"], "backtest": backtest.get(k)}
            for k, v in VARIANTS.items()
        ]

        return {
            "now": now.isoformat(),
            "demo": self.demo,
            "state": overall,
            "tz": tz_name,
            "control": {
                "trading_enabled": enabled,
                "variant": control["variant"],
                "active_s": state.active_seconds(control, now),
                "first_started_at": control.get("first_started_at"),
            },
            "variants": variants,
            "workers": workers,
            "health": health,
            "exec": {k: exec_status.get(k) for k in
                     ("symbol", "bid", "ask", "net_liquidation", "risk", "positions")},
            "model": algo_status,
            "stats": stats,
            "open": open_marked,
            "trades": trades,
            "trades_total": len(closed),
            "events": events,
        }


def make_handler(app: DashboardApp, port: int):
    allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}

    class Handler(BaseHTTPRequestHandler):
        server_version = "FXDashboard/1.0"

        def log_message(self, fmt, *args):  # silencioso
            pass

        # -- helpers --
        def _send(self, status: int, body: bytes, ctype: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, payload: dict) -> None:
            self._send(status, json.dumps(payload, default=str).encode("utf-8"),
                       "application/json; charset=utf-8")

        def _host_ok(self) -> bool:
            return self.headers.get("Host", "") in allowed_hosts

        # -- rutas --
        def do_GET(self):
            if not self._host_ok():
                return self._json(HTTPStatus.FORBIDDEN, {"error": "Host is not allowed."})
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                html = INDEX_HTML.read_text(encoding="utf-8").replace("__TOKEN__", app.token)
                return self._send(HTTPStatus.OK, html.encode("utf-8"), "text/html; charset=utf-8")
            if path == "/api/state":
                try:
                    return self._json(HTTPStatus.OK, app.build_state())
                except Exception as e:  # noqa: BLE001
                    return self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return self._json(HTTPStatus.NOT_FOUND, {"error": "Not found."})

        def do_POST(self):
            if not self._host_ok():
                return self._json(HTTPStatus.FORBIDDEN, {"error": "Host is not allowed."})
            if not secrets.compare_digest(self.headers.get("X-Dashboard-Token", ""), app.token):
                return self._json(HTTPStatus.FORBIDDEN, {"error": "Invalid dashboard token."})
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
            except json.JSONDecodeError:
                return self._json(HTTPStatus.BAD_REQUEST, {"error": "Invalid JSON request."})
            try:
                if self.path == "/api/start":
                    result = app.start()
                elif self.path == "/api/pause":
                    result = app.pause()
                elif self.path == "/api/stop":
                    result = app.stop()
                elif self.path == "/api/variant":
                    result = app.set_variant(str(body.get("variant", "")))
                else:
                    return self._json(HTTPStatus.NOT_FOUND, {"error": "Not found."})
            except PermissionError as e:
                return self._json(HTTPStatus.CONFLICT, {"error": str(e)})
            except ValueError as e:
                return self._json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            except Exception as e:  # noqa: BLE001
                return self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
            return self._json(HTTPStatus.OK, {"ok": True, "state": result})

    return Handler


def serve(app: DashboardApp, host: str = "127.0.0.1", port: int = 8050) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), make_handler(app, port))
    httpd.daemon_threads = True
    return httpd
