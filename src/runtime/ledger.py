"""
Libro de operaciones: convierte fills de IB (entrada + salida TP/SL) en trades
cerrados/abiertos y calcula métricas por día y acumuladas.

Funciones puras (sin IB/Kafka) para poder testearlas.

Supuesto de P&L: el par cotiza en USD (EURUSD, GBPUSD) => P&L USD = Δprecio × unidades.
Para pares JPY el P&L se reportaría en JPY (no soportado aquí).
"""
from collections import defaultdict
from datetime import datetime, timezone, tzinfo
from typing import Dict, List, Optional, Tuple

from src.runtime.state import parse_iso

PIP_SIZE_DEFAULT = 0.0001
PIP_SIZE_JPY = 0.01


def pip_size(symbol: str) -> float:
    return PIP_SIZE_JPY if (symbol or "").upper().endswith("JPY") else PIP_SIZE_DEFAULT


def _vwap(fills: List[dict]) -> Tuple[float, float]:
    qty = sum(float(f["shares"]) for f in fills)
    if qty <= 0:
        return 0.0, 0.0
    return sum(float(f["price"]) * float(f["shares"]) for f in fills) / qty, qty


def build_trades(orders: List[dict], records: List[dict]) -> Tuple[List[dict], List[dict]]:
    """
    orders:  líneas de orders.jsonl (se usan los `order_placed` con `trade_id`).
    records: líneas de fills.jsonl (type=fill | type=commission).
    Devuelve (cerrados ordenados por cierre, abiertos).
    """
    placed: Dict[str, dict] = {
        o["trade_id"]: o for o in orders
        if o.get("event") == "order_placed" and o.get("trade_id")
    }
    commissions: Dict[str, float] = {}
    fills_by_trade: Dict[str, List[dict]] = defaultdict(list)
    seen_exec = set()
    for r in records:
        if r.get("type") == "commission":
            if r.get("exec_id") is not None:
                commissions[r["exec_id"]] = float(r.get("commission") or 0.0)
        elif r.get("type", "fill") == "fill" and r.get("trade_id"):
            key = r.get("exec_id")
            if key is not None:
                if key in seen_exec:  # IB puede reenviar la misma ejecución
                    continue
                seen_exec.add(key)
            fills_by_trade[r["trade_id"]].append(r)

    closed: List[dict] = []
    open_: List[dict] = []
    for trade_id, fills in fills_by_trade.items():
        entries = [f for f in fills if f.get("role") == "entry"]
        exits = [f for f in fills if f.get("role") in ("tp", "sl", "manual")]
        if not entries:
            continue
        meta = placed.get(trade_id, {})
        entry_price, entry_qty = _vwap(entries)
        side = "BUY" if entries[0]["side"] in ("BOT", "BUY") else "SELL"
        direction = 1 if side == "BUY" else -1
        symbol = meta.get("symbol") or entries[0].get("symbol") or "EURUSD"
        base = {
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side,
            "qty": entry_qty,
            "entry_price": entry_price,
            "entry_time": min(f["timestamp"] for f in entries),
            "algorithm": meta.get("id_algoritmo"),
            "sl_price": meta.get("sl_price"),
            "tp_price": meta.get("tp_price"),
        }
        exit_price, exit_qty = _vwap(exits)
        if exits and exit_qty >= entry_qty * 0.999:
            pips = (exit_price - entry_price) * direction / pip_size(symbol)
            gross = (exit_price - entry_price) * direction * entry_qty
            commission = sum(commissions.get(f.get("exec_id"), 0.0) for f in entries + exits)
            last_exit = max(exits, key=lambda f: f["timestamp"])
            t_in, t_out = parse_iso(base["entry_time"]), parse_iso(last_exit["timestamp"])
            closed.append({
                **base,
                "exit_price": exit_price,
                "exit_time": last_exit["timestamp"],
                "exit_reason": {"tp": "TP", "sl": "SL"}.get(last_exit["role"], "Manual"),
                "pips": pips,
                "pnl_gross": gross,
                "commission": commission,
                "pnl": gross - commission,
                "duration_s": (t_out - t_in).total_seconds() if t_in and t_out else None,
            })
        else:
            commission = sum(commissions.get(f.get("exec_id"), 0.0) for f in entries)
            open_.append({**base, "commission": commission})

    closed.sort(key=lambda t: t["exit_time"])
    open_.sort(key=lambda t: t["entry_time"])
    return closed, open_


def mark_open_trade(trade: dict, bid: Optional[float], ask: Optional[float]) -> dict:
    """Agrega P&L flotante a una operación abierta (largo se valúa al bid, corto al ask)."""
    out = dict(trade)
    mark = bid if trade["side"] == "BUY" else ask
    if mark:
        direction = 1 if trade["side"] == "BUY" else -1
        out["mark_price"] = mark
        out["pips"] = (mark - trade["entry_price"]) * direction / pip_size(trade["symbol"])
        out["pnl"] = (mark - trade["entry_price"]) * direction * trade["qty"] - trade.get("commission", 0.0)
    else:
        out["mark_price"] = None
        out["pips"] = None
        out["pnl"] = None
    return out


# ---------- Métricas ----------

def _day_key(iso: str, tz: Optional[tzinfo]) -> str:
    dt = parse_iso(iso)
    return dt.astimezone(tz).date().isoformat() if dt else "?"


def _summary(trades: List[dict]) -> dict:
    n = len(trades)
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] < 0]
    gp, gl = sum(wins), abs(sum(losses))
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "pnl": sum(t["pnl"] for t in trades),
        "winrate": (len(wins) / n) if n else None,
        # None si no hay pérdidas (JSON no admite Infinity); `no_losses` distingue "∞" de "sin datos"
        "profit_factor": (gp / gl) if gl > 0 else None,
        "no_losses": gl == 0 and gp > 0,
        "expectancy": (sum(t["pnl"] for t in trades) / n) if n else None,
        "avg_win": (gp / len(wins)) if wins else None,
        "avg_loss": (-gl / len(losses)) if losses else None,
        "best_trade": max((t["pnl"] for t in trades), default=None),
        "worst_trade": min((t["pnl"] for t in trades), default=None),
        "pips": sum(t["pips"] for t in trades),
    }


def compute_stats(closed: List[dict], tz: Optional[tzinfo] = None, now: Optional[datetime] = None) -> dict:
    """Métricas de hoy, acumuladas, por día y curva de P&L acumulado."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(tz).date().isoformat()  # tz=None => zona del sistema

    by_day: Dict[str, List[dict]] = defaultdict(list)
    for t in closed:
        by_day[_day_key(t["exit_time"], tz)].append(t)

    daily = []
    for day in sorted(by_day):
        pnls = [t["pnl"] for t in by_day[day]]
        daily.append({
            "date": day,
            "pnl": sum(pnls),
            "trades": len(pnls),
            "wins": sum(1 for p in pnls if p > 0),
        })

    # Curva acumulada y drawdown (en USD, pico→valle sobre P&L acumulado, arrancando en 0)
    equity, cum, peak, max_dd = [], 0.0, 0.0, 0.0
    for t in closed:
        cum += t["pnl"]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
        equity.append({"t": t["exit_time"], "pnl": cum})

    overall = _summary(closed)
    overall["max_drawdown"] = max_dd
    overall["best_day"] = max(daily, key=lambda d: d["pnl"]) if daily else None
    overall["worst_day"] = min(daily, key=lambda d: d["pnl"]) if daily else None
    overall["avg_day"] = (sum(d["pnl"] for d in daily) / len(daily)) if daily else None
    overall["days_traded"] = len(daily)
    durations = [t["duration_s"] for t in closed if t.get("duration_s") is not None]
    overall["avg_duration_s"] = (sum(durations) / len(durations)) if durations else None

    # Racha actual
    streak = 0
    for t in reversed(closed):
        sign = 1 if t["pnl"] > 0 else -1 if t["pnl"] < 0 else 0
        if sign == 0:
            break
        if streak == 0 or (streak > 0) == (sign > 0):
            streak += sign
        else:
            break
    overall["streak"] = streak

    return {
        "today_date": today,
        "today": _summary(by_day.get(today, [])),
        "overall": overall,
        "daily": daily,
        "equity": equity,
    }
