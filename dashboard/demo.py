"""Genera datos sintéticos aislados para paneles de control, sin necesidad de conectarte a IB o Kafka"""
import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.algorithms.indicators import ema_series
from src.runtime import state


class DemoProcessManager:
    """Simular el estado del worker sin iniciar procesos en segundo plano"""

    def __init__(self):
        self.alive = {"producer": False, "algorithm": False, "executor": False}
        self.variant = None

    def status(self):
        return {n: {"alive": a, "pid": 1000 + i if a else None, "started_at": None,
                    "variant": self.variant if n == "algorithm" else None}
                for i, (n, a) in enumerate(self.alive.items())}

    def start_all(self, variant, manage_producer=True):
        self.variant = variant
        for n in self.alive:
            self.alive[n] = True
        return list(self.alive)

    def stop_all(self):
        for n in self.alive:
            self.alive[n] = False
        return []


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def seed_demo(orders_log: Path, days: int = 18, seed: int = 7) -> None:
    rnd = random.Random(seed)
    now = datetime.now(timezone.utc)
    algo = "ema_cross_trend"
    orders, records = [], []
    price = 1.0850

    # Simula unas cuantas operaciones por día hábil con paper-trading size
    for d in range(days, 0, -1):
        day = (now - timedelta(days=d)).replace(hour=8, minute=0, second=0, microsecond=0)
        if day.weekday() >= 5:
            continue
        for _ in range(rnd.randint(2, 4)):
            t_in = day + timedelta(minutes=rnd.randint(0, 600))
            dur = timedelta(minutes=rnd.randint(4, 140))
            side = rnd.choice(["BUY", "SELL"])
            direction = 1 if side == "BUY" else -1
            win = rnd.random() < 0.37
            price += rnd.uniform(-0.002, 0.002)
            entry = round(price, 5)
            exit_ = round(entry + direction * (0.0020 if win else -0.0010), 5)
            tid = f"demo{len(orders):04d}"
            orders.append({
                "timestamp": _iso(t_in), "event": "order_placed", "trade_id": tid,
                "id_algoritmo": algo, "symbol": "EURUSD", "signal": side, "action": side,
                "precio_referencia": entry, "quantity": 10000,
                "sl_price": round(entry - direction * 0.0010, 5),
                "tp_price": round(entry + direction * 0.0020, 5),
            })
            entry_side, exit_side = ("BOT", "SLD") if side == "BUY" else ("SLD", "BOT")
            for role, ts, sd, px in (("entry", t_in, entry_side, entry),
                                     ("tp" if win else "sl", t_in + dur, exit_side, exit_)):
                ex = f"{tid}-{role}"
                records.append({"type": "fill", "timestamp": _iso(ts), "exec_id": ex, "order_id": 1,
                                "trade_id": tid, "role": role, "symbol": "EURUSD",
                                "side": sd, "shares": 10000.0, "price": px})
                records.append({"type": "commission", "exec_id": ex, "commission": 0.4})

    # Añadir una operación actualmente abierta
    tid = "demo_open"
    entry = round(price, 5)
    t_in = now - timedelta(minutes=23)
    orders.append({"timestamp": _iso(t_in), "event": "order_placed", "trade_id": tid,
                   "id_algoritmo": algo, "symbol": "EURUSD", "signal": "BUY", "action": "BUY",
                   "precio_referencia": entry, "quantity": 10000,
                   "sl_price": round(entry - 0.0010, 5), "tp_price": round(entry + 0.0020, 5)})
    records.append({"type": "fill", "timestamp": _iso(t_in), "exec_id": "open-entry", "order_id": 9,
                    "trade_id": tid, "role": "entry", "symbol": "EURUSD", "side": "BOT",
                    "shares": 10000.0, "price": entry})
    records.append({"type": "commission", "exec_id": "open-entry", "commission": 0.4})

    # Añadir eventos de señal y de rechazo de muestra
    orders.append({"timestamp": _iso(now - timedelta(minutes=9)), "event": "signal_received",
                   "id_algoritmo": algo, "symbol": "EURUSD", "signal": "SELL", "precio_referencia": entry})
    orders.append({"timestamp": _iso(now - timedelta(minutes=9)), "event": "rejected",
                   "id_algoritmo": algo, "symbol": "EURUSD", "signal": "SELL", "precio_referencia": entry,
                   "reason": "Maximum open positions reached for EURUSD (1/1)."})
    orders.sort(key=lambda o: o["timestamp"])

    orders_log.parent.mkdir(parents=True, exist_ok=True)
    orders_log.write_text("\n".join(json.dumps(o) for o in orders) + "\n", encoding="utf-8")
    state.FILLS_FILE.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

    # Escribe señales de estado (heartbeats) de ejemplo para el ejecutor y la estrategia
    bid = round(entry + 0.0007, 5)
    state.write_json_atomic(state.EXECUTOR_STATUS_FILE, {
        "updated_at": state.now_iso(), "ib_connected": True, "accounts": ["DU1234567"],
        "symbol": "EURUSD", "bid": bid, "ask": round(bid + 0.00012, 5), "net_liquidation": 1000012.4,
        "positions": [{"symbol": "EURUSD", "position": 10000.0, "avg_cost": entry}],
        "risk": {"trade_size": 10000, "sl_pips": 10, "tp_pips": 20, "max_positions": 1},
    })
    closes = [entry - 0.0030 + 0.0012 * math.sin(i / 7) + 0.0000 * i + rnd.uniform(-0.0003, 0.0003)
              + i * 0.00005 for i in range(120)]
    fast, slow, trend = ema_series(closes, 9), ema_series(closes, 21), ema_series(closes, 50)
    series = []
    for i in range(60, 120):
        t = now - timedelta(minutes=120 - i)
        series.append({"t": _iso(t), "close": round(closes[i], 5), "fast": round(fast[i], 5),
                       "slow": round(slow[i], 5), "trend": round(trend[i], 5)})
    state.write_json_atomic(state.ALGO_STATUS_FILE, {
        "id_algoritmo": algo, "updated_at": state.now_iso(), "symbol": "EURUSD",
        "last_vela": {"inicio": _iso(now), "close": series[-1]["close"]},
        "velas_received": 240, "buffer_len": 120, "warmup_needed": 52,
        "last_signal": "BUY", "last_signal_at": _iso(now - timedelta(minutes=23)),
        "model": {"params": {"fast": 9, "slow": 21, "trend": 50, "use_trend_filter": True,
                             "use_rsi_filter": False},
                  "ema_fast": series[-1]["fast"], "ema_slow": series[-1]["slow"],
                  "ema_trend": series[-1]["trend"], "rsi": None,
                  "bias": "BULLISH" if series[-1]["fast"] > series[-1]["slow"] else "BEARISH",
                  "series": series},
    })
    # Comienza en pausa para que el usuario pueda probar el botón de inicio del panel
    state.save_control({**state._default_control(), "variant": "v3"})
