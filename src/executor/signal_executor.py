"""Consume Kafka signals, apply risk rules, and submit bracket orders to IB.

The executor refuses live-account ports and non-paper accounts. A bracket
contains a market entry, a take-profit limit order, and a stop-loss order.
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from ib_insync import IB, Forex, LimitOrder, MarketOrder, StopOrder

from config.settings import (
    IB_HOST, IB_PORT, IB_CLIENT_ID_EXECUTOR,
    TOPIC_SIGNALS, SYMBOL, LOGS_DIR,
)
from src.common.kafka_utils import (
    create_topic_if_not_exists,
    make_consumer,
    deserialize_json,
)
from src.common.logger import get_logger
from src.algorithms.variants import VARIANTS
from src.executor.risk_manager import RiskManager
from src.runtime.state import (
    EXECUTOR_STATUS_FILE, FILLS_FILE, ORDER_MAP_FILE,
    append_jsonl, install_graceful_shutdown, load_control, now_iso,
    read_json, read_jsonl, write_json_atomic,
)


ORDERS_LOG = LOGS_DIR / "orders.jsonl"

# Dashboard-launched workers obey the Start/Pause control file.
CONTROLLED_BY_DASHBOARD = os.getenv("TRADING_CONTROL") == "dashboard"
# Interactive Brokers live-account ports; this project is configured for paper trading.
LIVE_PORTS = {4001, 7496}
STATUS_EVERY_S = 2.0


class SignalExecutor:
    def __init__(self):
        self.ib = IB()
        self.contract = Forex(SYMBOL, exchange="IDEALPRO", currency="USD")
        self.risk = RiskManager()
        self.log = get_logger("signal_executor")
        self.consumer = None
        self.connected_client_id: Optional[int] = None
        self.ticker = None
        self._last_status = 0.0
        self._last_reconnect = 0.0
        # Map order IDs to trade IDs and roles so fill tracking survives restarts.
        self.order_map = read_json(ORDER_MAP_FILE, {}) or {}
        self._seen_exec_ids = {
            r["exec_id"] for r in read_jsonl(FILLS_FILE)
            if r.get("type") == "fill" and r.get("exec_id")
        }

    # ---------- Interactive Brokers connection ----------

    def _connect_with_retry(self, max_attempts: int = 5, step: int = 10) -> None:
        base_id = IB_CLIENT_ID_EXECUTOR
        for attempt in range(max_attempts):
            cid = base_id + attempt * step
            try:
                self.ib.connect(IB_HOST, IB_PORT, clientId=cid, timeout=15)
                self.connected_client_id = cid
                self.log.info("Connected to IB Gateway with clientId=%s.", cid)
                return
            except Exception as e:
                self.log.warning(
                    "Connection attempt %s/%s (clientId=%s) failed: %s",
                    attempt + 1,
                    max_attempts,
                    cid,
                    e,
                )
                time.sleep(2)
        raise RuntimeError("Could not connect to IB Gateway after multiple attempts.")

    def _maybe_reconnect(self) -> None:
        """Retry a dropped IB connection every 15 seconds using the same client ID."""
        if self.ib.isConnected() or time.time() - self._last_reconnect < 15:
            return
        self._last_reconnect = time.time()
        try:
            cid = self.connected_client_id or IB_CLIENT_ID_EXECUTOR
            self.ib.connect(IB_HOST, IB_PORT, clientId=cid, timeout=10)
            self.ticker = self.ib.reqMktData(self.contract, "", False, False)
            self.log.info("Reconnected to IB Gateway.")
        except Exception as e:
            self.log.warning("Could not reconnect to IB: %s", e)

    def _get_positions(self) -> List[dict]:
        """Read the current open positions from IB."""
        positions = self.ib.positions()
        return [
            {
                "symbol": (
                    f"{p.contract.symbol}{p.contract.currency}"
                    if p.contract.secType == "CASH" and p.contract.currency
                    else p.contract.symbol
                ),
                "position": float(p.position),
                "avg_cost": float(p.avgCost),
            }
            for p in positions
        ]

    # ---------- Event logging ----------

    def _log_event(self, event: str, signal: dict, extra: Optional[dict] = None) -> None:
        entry = {
            "timestamp": now_iso(),
            "event": event,
            "id_algoritmo": signal.get("id_algoritmo"),
            "symbol": signal.get("symbol"),
            "signal": signal.get("signal"),
            "precio_referencia": signal.get("precio_referencia"),
        }
        if extra:
            entry.update(extra)
        try:
            with open(ORDERS_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            self.log.error("Could not write to orders.jsonl: %s", e)

    # ---------- Executions reported by IB ----------

    def _order_key(self, order_id: int, client_id: Optional[int] = None) -> str:
        if client_id is None:
            client_id = getattr(getattr(self.ib, "client", None), "clientId", 0)
        return f"{client_id}:{order_id}"

    def _register_orders(self, trade_id: str, roles: dict) -> None:
        """Persist order roles before submission because fills can arrive immediately."""
        for order_id, role in roles.items():
            self.order_map[self._order_key(order_id)] = {"trade_id": trade_id, "role": role}
        try:
            write_json_atomic(ORDER_MAP_FILE, self.order_map)
        except Exception as e:
            self.log.error("Could not save open_orders_map.json: %s", e)

    def _record_fill(self, fill) -> None:
        try:
            ex = fill.execution
            if ex.execId in self._seen_exec_ids:
                return
            info = self.order_map.get(self._order_key(ex.orderId, ex.clientId))
            if info is None:
                self.log.info("Ignoring fill without a matching trade (orderId=%s).", ex.orderId)
                return
            self._seen_exec_ids.add(ex.execId)
            ts = getattr(fill, "time", None) or getattr(ex, "time", None)
            if isinstance(ts, datetime):
                ts = (ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
                ts = ts.isoformat().replace("+00:00", "Z")
            append_jsonl(FILLS_FILE, {
                "type": "fill",
                "timestamp": ts or now_iso(),
                "exec_id": ex.execId,
                "order_id": ex.orderId,
                "trade_id": info["trade_id"],
                "role": info["role"],
                "symbol": SYMBOL,
                "side": ex.side,  # IB uses BOT for buy and SLD for sell.
                "shares": float(ex.shares),
                "price": float(ex.price),
            })
            self.log.info(
                "Fill %s %s %s @ %s (trade %s)",
                info["role"].upper(),
                ex.side,
                ex.shares,
                ex.price,
                info["trade_id"],
            )
            report = getattr(fill, "commissionReport", None)
            if report is not None and getattr(report, "commission", 0):
                self._record_commission(report)
        except Exception as e:
            self.log.exception("Could not record IB fill: %s", e)

    def _record_commission(self, report) -> None:
        try:
            append_jsonl(FILLS_FILE, {
                "type": "commission",
                "exec_id": report.execId,
                "commission": float(report.commission),
                "timestamp": now_iso(),
            })
        except Exception as e:
            self.log.exception("Could not record IB commission: %s", e)

    def _on_exec_details(self, trade, fill) -> None:
        self._record_fill(fill)

    def _on_commission(self, trade, fill, report) -> None:
        self._record_commission(report)

    # ---------- Dashboard status heartbeat ----------

    def _write_status(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_status < STATUS_EVERY_S:
            return
        self._last_status = now
        bid = ask = net_liq = None
        try:
            if self.ticker is not None:
                bid = self.ticker.bid if self.ticker.bid and self.ticker.bid > 0 else None
                ask = self.ticker.ask if self.ticker.ask and self.ticker.ask > 0 else None
            for v in self.ib.accountValues():
                if v.tag == "NetLiquidation" and v.currency in ("USD", "BASE"):
                    net_liq = float(v.value)
                    break
        except Exception as e:
            self.log.warning("Could not read IB account values for status: %s", e)
        try:
            positions = self._get_positions() if self.ib.isConnected() else []
        except Exception as e:
            self.log.warning("Could not read IB positions for status: %s", e)
            positions = []
        try:
            write_json_atomic(EXECUTOR_STATUS_FILE, {
                "updated_at": now_iso(),
                "ib_connected": bool(self.ib.isConnected()),
                "accounts": list(self.ib.managedAccounts()) if self.ib.isConnected() else [],
                "client_id": self.connected_client_id,
                "symbol": SYMBOL,
                "bid": bid,
                "ask": ask,
                "net_liquidation": net_liq,
                "positions": positions,
                "allowed_algorithms": self.risk.allowed_algorithms,
                "risk": {
                    "trade_size": self.risk.trade_size,
                    "sl_pips": self.risk.sl_pips,
                    "tp_pips": self.risk.tp_pips,
                    "max_positions": self.risk.max_positions,
                },
            })
        except Exception as e:
            self.log.error("Could not write executor_status.json: %s", e)

    # ---------- Order execution ----------

    def _place_bracket(self, signal: dict) -> None:
        qty = self.risk.trade_size
        action = signal["signal"]  # BUY or SELL.
        ref = float(signal["precio_referencia"])

        sl_price, tp_price = self.risk.compute_bracket_prices(action, ref)

        self.log.info(
            "Submitting %s bracket: quantity=%s reference=%.5f stop=%.5f take-profit=%.5f",
            action,
            qty,
            ref,
            sl_price,
            tp_price,
        )

        reverse_action = "SELL" if action == "BUY" else "BUY"
        trade_id = uuid.uuid4().hex[:12]
        parent_order_id = self.ib.client.getReqId()
        orders = [
            MarketOrder(
                action,
                qty,
                orderId=parent_order_id,
                transmit=False,
            ),
            LimitOrder(
                reverse_action,
                qty,
                tp_price,
                orderId=self.ib.client.getReqId(),
                parentId=parent_order_id,
                transmit=False,
            ),
            StopOrder(
                reverse_action,
                qty,
                sl_price,
                orderId=self.ib.client.getReqId(),
                parentId=parent_order_id,
                transmit=True,
            ),
        ]
        self._register_orders(
            trade_id,
            {orders[0].orderId: "entry", orders[1].orderId: "tp", orders[2].orderId: "sl"},
        )
        trades = [self.ib.placeOrder(self.contract, order) for order in orders]

        parent_trade = trades[0]
        # Allow IB a short time to report order status.
        self.ib.sleep(2)
        inactive_orders = [
            trade.order.orderId
            for trade in trades
            if trade.orderStatus.status == "Inactive"
        ]
        if inactive_orders:
            raise RuntimeError(f"IB marked bracket orders inactive: {inactive_orders}")

        self._log_event(
            event="order_placed",
            signal=signal,
            extra={
                "trade_id": trade_id,
                "action": action,
                "quantity": qty,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "parent_order_id": parent_trade.order.orderId,
                "parent_status": parent_trade.orderStatus.status,
                "filled_price": parent_trade.orderStatus.avgFillPrice,
                "bracket_orders": [
                    {
                        "order_id": trade.order.orderId,
                        "order_type": trade.order.orderType,
                        "action": trade.order.action,
                        "status": trade.orderStatus.status,
                    }
                    for trade in trades
                ],
            },
        )
        self.log.info(
            "Bracket submitted. parent_order_id=%s, status=%s",
            parent_trade.order.orderId,
            parent_trade.orderStatus.status,
        )

    def execute_signal(self, signal: dict) -> bool:
        """Validate a signal and return whether its bracket was submitted."""
        self.log.info("Received signal: %s", signal)
        self._log_event(event="signal_received", signal=signal)

        if CONTROLLED_BY_DASHBOARD:
            control = load_control()
            if not control.get("trading_enabled"):
                self.log.info("Trading is paused in the dashboard; signal ignored.")
                self._log_event(event="ignored_paused", signal=signal,
                                extra={"reason": "Trading is paused."})
                return False
            variant = control.get("variant")
            if variant in VARIANTS:
                self.risk.allowed_algorithms = [VARIANTS[variant]["id_algoritmo"]]

        positions = self._get_positions()
        ok, reason = self.risk.can_execute(signal, positions)
        if not ok:
            self.log.warning("Signal rejected: %s", reason)
            self._log_event(
                event="rejected",
                signal=signal,
                extra={"reason": reason, "positions": positions},
            )
            return False

        try:
            self._place_bracket(signal)
            return True
        except Exception as e:
            self.log.exception("Could not submit order: %s", e)
            self._log_event(
                event="error",
                signal=signal,
                extra={"error": str(e)},
            )
            return False

    # ---------- Main worker loop ----------

    def run(self) -> None:
        create_topic_if_not_exists(TOPIC_SIGNALS)

        install_graceful_shutdown()
        if IB_PORT in LIVE_PORTS:
            raise RuntimeError(
                f"IB_PORT={IB_PORT} is configured for a live account. This project only "
                "supports paper trading (Gateway 4002, TWS 7497)."
            )

        self.log.info("Connecting to IB Gateway.")
        self._connect_with_retry()
        accounts = self.ib.managedAccounts()
        if not all(a.startswith("DU") for a in accounts):  # IB paper account IDs start with DU.
            self.ib.disconnect()
            raise RuntimeError(
                f"Non-paper account detected {accounts}; aborting for safety."
            )
        self.contract = Forex(SYMBOL, exchange="IDEALPRO", currency="USD")
        self.ib.qualifyContracts(self.contract)
        self.log.info("Qualified contract: %s", self.contract)

        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.commissionReportEvent += self._on_commission
        for fill in self.ib.fills():  # Include executions already reported in this IB session.
            self._record_fill(fill)
        try:
            self.ticker = self.ib.reqMktData(self.contract, "", False, False)
        except Exception as e:
            self.log.warning("Market data is unavailable for the status heartbeat: %s", e)

        self.consumer = make_consumer(TOPIC_SIGNALS, group_id="signal_executor_group")
        self.log.info(
            "Order executor started. Accounts=%s. Listening to '%s'. "
            "Allowed algorithms=%s. Quantity=%s, stop=%sp, take-profit=%sp.",
            self.ib.managedAccounts(),
            TOPIC_SIGNALS,
            self.risk.allowed_algorithms,
            self.risk.trade_size,
            self.risk.sl_pips,
            self.risk.tp_pips,
        )

        try:
            while True:
                # Alternate IB and Kafka polling so fills continue to be processed
                # while the Kafka consumer waits for signals.
                self.ib.sleep(0.2)
                self._write_status()
                self._maybe_reconnect()
                msg = self.consumer.poll(timeout=0.8)
                if msg is None:
                    continue
                if msg.error():
                    self.log.error("Kafka error: %s", msg.error())
                    continue
                try:
                    signal = deserialize_json(msg.value())
                    self.execute_signal(signal)
                except Exception as e:
                    self.log.exception("Could not process Kafka message: %s", e)
        except KeyboardInterrupt:
            self.log.info("Stopped by user (Ctrl+C).")
        finally:
            self._write_status(force=True)
            if self.consumer is not None:
                self.consumer.close()
            if self.ib.isConnected():
                self.ib.disconnect()
            self.log.info("Order executor stopped.")