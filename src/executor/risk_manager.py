"""
Gestión de riesgo del ejecutor.
Reglas de negocio puras; sin dependencias de IB ni Kafka.
"""
from typing import List, Tuple

from config.settings import (
    RISK_TRADE_SIZE,
    RISK_STOP_LOSS_PIPS,
    RISK_TAKE_PROFIT_PIPS,
    RISK_MAX_POSITIONS,
    EXECUTOR_ALLOWED_ALGORITHMS,
)


# 1 pip = 0.0001 en EURUSD y GBPUSD
PIP_SIZE = 0.0001


class RiskManager:
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
        Retorna (True, "ok") si la señal puede ejecutarse.
        Retorna (False, razón) si debe rechazarse.

        `current_positions` es una lista de dicts con al menos:
            {"symbol": "EURUSD", "position": 10000.0}
        donde `position` es la cantidad neta (positivo=long, negativo=short).
        """
        # 1. Filtro de algoritmo (ignorar señales de test)
        if signal.get("id_algoritmo") not in self.allowed_algorithms:
            return False, f"algoritmo '{signal.get('id_algoritmo')}' no autorizado"

        # 2. Validar campos requeridos
        for field in ("id_algoritmo", "symbol", "signal", "precio_referencia"):
            if field not in signal:
                return False, f"campo faltante: {field}"

        if signal["signal"] not in ("BUY", "SELL"):
            return False, f"señal inválida: {signal['signal']}"

        if not isinstance(signal["precio_referencia"], (int, float)) or signal["precio_referencia"] <= 0:
            return False, f"precio_referencia inválido: {signal['precio_referencia']}"

        # 3. Máximo de posiciones por símbolo
        symbol = signal["symbol"]
        open_positions = [
            p for p in current_positions
            if p.get("symbol") == symbol and abs(p.get("position", 0)) > 0
        ]
        if len(open_positions) >= self.max_positions:
            return False, (
                f"máximo de posiciones alcanzado para {symbol} "
                f"({len(open_positions)}/{self.max_positions})"
            )

        return True, "ok"

    def compute_bracket_prices(
        self, side: str, reference_price: float, pip_size: float = PIP_SIZE
    ) -> Tuple[float, float]:
        """
        Calcula (stop_loss, take_profit) alrededor del precio de referencia.

        - BUY:  SL por debajo, TP por encima
        - SELL: SL por encima, TP por debajo
        """
        if side == "BUY":
            sl = reference_price - self.sl_pips * pip_size
            tp = reference_price + self.tp_pips * pip_size
        elif side == "SELL":
            sl = reference_price + self.sl_pips * pip_size
            tp = reference_price - self.tp_pips * pip_size
        else:
            raise ValueError(f"side inválido: {side}")

        return round(sl, 5), round(tp, 5)