"""
Ejecutor de órdenes: consume signals de Kafka, aplica control de riesgo
y envía órdenes bracket (market + SL + TP) a IB paper account.
"""
import json
import time
from datetime import datetime
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
from src.executor.risk_manager import RiskManager


ORDERS_LOG = LOGS_DIR / "orders.jsonl"


class SignalExecutor:
    def __init__(self):
        self.ib = IB()
        self.contract = Forex(SYMBOL, exchange="IDEALPRO", currency="USD")
        self.risk = RiskManager()
        self.log = get_logger("signal_executor")
        self.consumer = None
        self.connected_client_id: Optional[int] = None

    # ---------- Conexión IB ----------

    def _connect_with_retry(self, max_attempts: int = 5, step: int = 10) -> None:
        base_id = IB_CLIENT_ID_EXECUTOR
        for attempt in range(max_attempts):
            cid = base_id + attempt * step
            try:
                self.ib.connect(IB_HOST, IB_PORT, clientId=cid, timeout=15)
                self.connected_client_id = cid
                self.log.info(f"✅ Conectado a IB Gateway con clientId={cid}")
                return
            except Exception as e:
                self.log.warning(f"Intento {attempt + 1}/{max_attempts} (clientId={cid}) falló: {e}")
                time.sleep(2)
        raise RuntimeError("No se pudo conectar a IB Gateway tras varios intentos.")

    def _get_positions(self) -> List[dict]:
        """Consulta las posiciones abiertas desde IB."""
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

    # ---------- Registro de eventos ----------

    def _log_event(self, event: str, signal: dict, extra: Optional[dict] = None) -> None:
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
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
            self.log.error(f"No se pudo escribir en orders.jsonl: {e}")

    # ---------- Ejecución ----------

    def _place_bracket(self, signal: dict) -> None:
        qty = self.risk.trade_size
        action = signal["signal"]  # BUY o SELL
        ref = float(signal["precio_referencia"])

        sl_price, tp_price = self.risk.compute_bracket_prices(action, ref)

        self.log.info(
            f"📤 Enviando bracket {action} qty={qty} ref={ref:.5f} "
            f"SL={sl_price:.5f} TP={tp_price:.5f}"
        )

        reverse_action = "SELL" if action == "BUY" else "BUY"
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
        trades = [self.ib.placeOrder(self.contract, order) for order in orders]

        parent_trade = trades[0]
        # Espera breve para confirmar aceptación
        self.ib.sleep(2)
        inactive_orders = [
            trade.order.orderId
            for trade in trades
            if trade.orderStatus.status == "Inactive"
        ]
        if inactive_orders:
            raise RuntimeError(
                f"IB dejó órdenes bracket inactivas: {inactive_orders}"
            )

        self._log_event(
            event="order_placed",
            signal=signal,
            extra={
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
            f"✅ Bracket enviado. parent_order_id={parent_trade.order.orderId}, "
            f"status={parent_trade.orderStatus.status}"
        )

    def execute_signal(self, signal: dict) -> bool:
        """Valida y ejecuta una señal; retorna si se envió el bracket."""
        self.log.info(f"📥 Señal recibida: {signal}")
        self._log_event(event="signal_received", signal=signal)

        positions = self._get_positions()
        ok, reason = self.risk.can_execute(signal, positions)
        if not ok:
            self.log.warning(f"🚫 Rechazada: {reason}")
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
            self.log.error(f"❌ Error enviando orden: {e}")
            self._log_event(
                event="error",
                signal=signal,
                extra={"error": str(e)},
            )
            return False

    # ---------- Loop principal ----------

    def run(self) -> None:
        create_topic_if_not_exists(TOPIC_SIGNALS)

        self.log.info("Conectando a IB Gateway...")
        self._connect_with_retry()
        self.contract = Forex(SYMBOL, exchange="IDEALPRO", currency="USD")
        self.ib.qualifyContracts(self.contract)
        self.log.info(f"Contrato calificado: {self.contract}")

        self.consumer = make_consumer(TOPIC_SIGNALS, group_id="signal_executor_group")
        self.log.info(
            f"▶ Ejecutor iniciado. Cuenta={self.ib.managedAccounts()}. "
            f"Escuchando '{TOPIC_SIGNALS}'. "
            f"Algoritmos autorizados: {self.risk.allowed_algorithms}. "
            f"Trade size={self.risk.trade_size} SL={self.risk.sl_pips}p TP={self.risk.tp_pips}p."
        )

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log.error(f"Kafka error: {msg.error()}")
                    continue
                try:
                    signal = deserialize_json(msg.value())
                    self.execute_signal(signal)
                except Exception as e:
                    self.log.error(f"Error procesando mensaje: {e}")
        except KeyboardInterrupt:
            self.log.info("Detenido por el usuario (Ctrl+C).")
        finally:
            if self.consumer is not None:
                self.consumer.close()
            if self.ib.isConnected():
                self.ib.disconnect()
            self.log.info("Ejecutor detenido.")