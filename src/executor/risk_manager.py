"""Validaciones de riesgo y cálculo de precios de órdenes bracket."""
import math
from typing import List, Tuple

from config.settings import (
    RISK_TRADE_SIZE,
    RISK_STOP_LOSS_PIPS,
    RISK_TAKE_PROFIT_PIPS,
    RISK_MAX_POSITIONS,
    EXECUTOR_ALLOWED_ALGORITHMS,
)


# Un pip equivale a 0.0001 en EURUSD y GBPUSD.
PIP_SIZE = 0.0001


class RiskManager:
    """Rechaza señales no autorizadas o riesgosas antes de enviarlas a IB."""

    def __init__(
        self,
        trade_size: int = RISK_TRADE_SIZE,
        sl_pips: float = RISK_STOP_LOSS_PIPS,
        tp_pips: float = RISK_TAKE_PROFIT_PIPS,
        max_positions: int = RISK_MAX_POSITIONS,
        allowed_algorithms: List[str] = EXECUTOR_ALLOWED_ALGORITHMS,
    ):
        self.trade_size = trade_size
        self.sl_pips = sl_pips
        self.tp_pips = tp_pips
        self.max_positions = max_positions
        self.allowed_algorithms = [a.strip() for a in allowed_algorithms if a.strip()]

    def can_execute(
        self,
        signal: dict,
        current_positions: List[dict],
    ) -> Tuple[bool, str]:
        """
        Devuelve ``(True, "ok")`` si se puede ejecutar la señal.
        Devuelve ``(False, motivo)`` si se debe rechazar.

        ``current_positions`` contiene diccionarios con al menos ``symbol``
        y ``position``. Los valores positivos representan posiciones largas
        y los negativos, posiciones cortas.
        """
        # Rechaza identificadores de estrategias no aprobados, incluidas las de prueba.
        if signal.get("id_algoritmo") not in self.allowed_algorithms:
            return False, f"Algorithm '{signal.get('id_algoritmo')}' is not allowed."

        # Los campos obligatorios forman parte del contrato actual de Kafka.
        for field in ("id_algoritmo", "symbol", "signal", "precio_referencia"):
            if field not in signal:
                return False, f"Required field is missing: {field}."

        if signal["signal"] not in ("BUY", "SELL"):
            return False, f"Invalid signal direction: {signal['signal']}."

        reference_price = signal["precio_referencia"]
        if (
            isinstance(reference_price, bool)
            or not isinstance(reference_price, (int, float))
            or not math.isfinite(reference_price)
            or reference_price <= 0
        ):
            return False, f"Invalid reference price: {reference_price}."

        # Cuenta únicamente las posiciones abiertas del símbolo de esta señal.
        symbol = signal["symbol"]
        open_positions = [
            p for p in current_positions
            if p.get("symbol") == symbol and abs(p.get("position", 0)) > 0
        ]
        if len(open_positions) >= self.max_positions:
            return False, (
                f"Maximum open positions reached for {symbol} "
                f"({len(open_positions)}/{self.max_positions})."
            )

        return True, "ok"

    def compute_bracket_prices(
        self, side: str, reference_price: float, pip_size: float = PIP_SIZE
    ) -> Tuple[float, float]:
        """
        Calcula el stop loss y el take profit a partir del precio de referencia.

        - BUY: SL por debajo y TP por encima.
        - SELL: SL por encima y TP por debajo.
        """
        if side == "BUY":
            sl = reference_price - self.sl_pips * pip_size
            tp = reference_price + self.tp_pips * pip_size
        elif side == "SELL":
            sl = reference_price + self.sl_pips * pip_size
            tp = reference_price - self.tp_pips * pip_size
        else:
            raise ValueError(f"Invalid order side: {side}")

        return round(sl, 5), round(tp, 5)