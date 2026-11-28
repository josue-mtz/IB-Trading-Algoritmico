"""Pruebas unitarias locales de riesgo, filtros y creación de órdenes bracket."""
from types import SimpleNamespace
from unittest.mock import Mock

from src.executor.risk_manager import RiskManager
from src.executor.signal_executor import SignalExecutor


ALLOWED = ["ema_cross_v1"]


def _make_signal(id_algo="ema_cross_v1", symbol="EURUSD", signal="BUY", ref=1.08500):
    return {
        "id_algoritmo": id_algo,
        "symbol": symbol,
        "signal": signal,
        "precio_referencia": ref,
        "timestamp": "2026-01-01T12:00:00Z",
    }


def test_can_execute_valid_buy():
    rm = RiskManager(allowed_algorithms=ALLOWED)
    ok, reason = rm.can_execute(_make_signal(), [])
    assert ok, f"Expected an allowed signal, got: {reason}"
    print("PASS test_can_execute_valid_buy")


def test_reject_test_algorithm():
    rm = RiskManager(allowed_algorithms=ALLOWED)
    sig = _make_signal(id_algo="ema_cross_test_abc123")
    ok, reason = rm.can_execute(sig, [])
    assert not ok
    assert "not allowed" in reason

    executor = SignalExecutor()
    executor.ib = Mock()
    executor.ib.positions.return_value = []
    executor._log_event = Mock()
    executor._place_bracket = Mock()
    assert executor.execute_signal(sig) is False
    executor._place_bracket.assert_not_called()
    assert executor._log_event.call_args_list[-1].kwargs["event"] == "rejected"
    print(f"PASS test_reject_test_algorithm (reason: {reason})")


def test_reject_max_positions():
    rm = RiskManager(allowed_algorithms=ALLOWED, max_positions=1)
    positions = [{"symbol": "EURUSD", "position": 10000.0}]
    ok, reason = rm.can_execute(_make_signal(), positions)
    assert not ok
    assert "Maximum open positions" in reason

    executor = SignalExecutor()
    executor.ib = Mock()
    executor.ib.positions.return_value = [
        SimpleNamespace(
            contract=SimpleNamespace(
                symbol="EUR",
                currency="USD",
                secType="CASH",
            ),
            position=10000.0,
            avgCost=1.09,
        )
    ]
    executor._log_event = Mock()
    executor._place_bracket = Mock()
    assert executor.execute_signal(_make_signal()) is False
    executor._place_bracket.assert_not_called()
    assert executor._log_event.call_args_list[-1].kwargs["event"] == "rejected"
    print(f"PASS test_reject_max_positions (reason: {reason})")


def test_accept_when_position_zero():
    """Una posición con saldo neto cero no cuenta como abierta."""
    rm = RiskManager(allowed_algorithms=ALLOWED)
    positions = [{"symbol": "EURUSD", "position": 0.0}]
    ok, _ = rm.can_execute(_make_signal(), positions)
    assert ok
    print("PASS test_accept_when_position_zero")


def test_reject_different_symbol_open_does_not_block():
    """Una posición abierta en otro símbolo no bloquea EURUSD."""
    rm = RiskManager(allowed_algorithms=ALLOWED)
    positions = [{"symbol": "GBPUSD", "position": 10000.0}]
    ok, _ = rm.can_execute(_make_signal(symbol="EURUSD"), positions)
    assert ok
    print("PASS test_reject_different_symbol_open_does_not_block")


def test_reject_invalid_signal():
    rm = RiskManager(allowed_algorithms=ALLOWED)
    sig = _make_signal()
    sig["signal"] = "HOLD"
    ok, reason = rm.can_execute(sig, [])
    assert not ok
    assert "Invalid signal direction" in reason
    print(f"PASS test_reject_invalid_signal (reason: {reason})")


def test_reject_missing_field():
    rm = RiskManager(allowed_algorithms=ALLOWED)
    sig = _make_signal()
    del sig["precio_referencia"]
    ok, reason = rm.can_execute(sig, [])
    assert not ok
    assert "Required field is missing" in reason
    print(f"PASS test_reject_missing_field (reason: {reason})")


def test_bracket_prices_buy():
    rm = RiskManager(allowed_algorithms=ALLOWED, sl_pips=10, tp_pips=20)
    sl, tp = rm.compute_bracket_prices("BUY", 1.08500)
    # SL = 1.08500 - 0.0010 = 1.08400.
    # TP = 1.08500 + 0.0020 = 1.08700.
    assert abs(sl - 1.08400) < 1e-9, f"sl={sl}"
    assert abs(tp - 1.08700) < 1e-9, f"tp={tp}"
    print(f"PASS test_bracket_prices_buy (SL={sl}, TP={tp})")


def test_bracket_prices_sell():
    rm = RiskManager(allowed_algorithms=ALLOWED, sl_pips=10, tp_pips=20)
    sl, tp = rm.compute_bracket_prices("SELL", 1.08500)
    # SL = 1.08500 + 0.0010 = 1.08600.
    # TP = 1.08500 - 0.0020 = 1.08300.
    assert abs(sl - 1.08600) < 1e-9, f"sl={sl}"
    assert abs(tp - 1.08300) < 1e-9, f"tp={tp}"
    print(f"PASS test_bracket_prices_sell (SL={sl}, TP={tp})")


class _FakeIB:
    def __init__(self):
        self.client = self
        self.next_order_id = 100
        self.orders = []
        self.contracts = []

    def isConnected(self):
        return False

    def getReqId(self):
        order_id = self.next_order_id
        self.next_order_id += 1
        return order_id

    def positions(self):
        return []

    def placeOrder(self, contract, order):
        self.contracts.append(contract)
        self.orders.append(order)
        return SimpleNamespace(
            order=order,
            orderStatus=SimpleNamespace(status="Submitted", avgFillPrice=0.0),
        )

    def sleep(self, seconds):
        pass


def test_execute_signal_builds_bracket_and_reports_failure():
    executor = SignalExecutor()
    fake_ib = _FakeIB()
    executor.ib = fake_ib
    executor._log_event = Mock()

    assert executor.execute_signal(_make_signal(ref=1.085)) is True

    parent, take_profit, stop_loss = fake_ib.orders
    assert parent.orderType == "MKT"
    assert parent.action == "BUY"
    assert parent.totalQuantity == 10000
    assert parent.transmit is False

    assert take_profit.orderType == "LMT"
    assert take_profit.action == "SELL"
    assert take_profit.lmtPrice == 1.087
    assert take_profit.parentId == parent.orderId
    assert take_profit.transmit is False

    assert stop_loss.orderType == "STP"
    assert stop_loss.action == "SELL"
    assert stop_loss.auxPrice == 1.084
    assert stop_loss.parentId == parent.orderId
    assert stop_loss.transmit is True
    event = executor._log_event.call_args.kwargs
    bracket_orders = event["extra"]["bracket_orders"]
    assert [order["order_type"] for order in bracket_orders] == [
        "MKT",
        "LMT",
        "STP",
    ]
    assert [order["order_id"] for order in bracket_orders] == [100, 101, 102]

    failing_executor = SignalExecutor()
    failing_executor._get_positions = Mock(return_value=[])
    failing_executor._log_event = Mock()
    failing_executor._place_bracket = Mock(side_effect=RuntimeError("test failure"))
    assert failing_executor.execute_signal(_make_signal()) is False
    print("PASS test_execute_signal_builds_bracket_and_reports_failure")


def test_execute_signal_uses_signal_symbol():
    executor = SignalExecutor()
    fake_ib = _FakeIB()
    executor.ib = fake_ib
    executor._log_event = Mock()

    assert executor.execute_signal(_make_signal(symbol="GBPUSD", ref=1.2700)) is True
    assert len(fake_ib.contracts) == 3
    assert all(c.pair() == "GBPUSD" for c in fake_ib.contracts)
    print("PASS test_execute_signal_uses_signal_symbol")


if __name__ == "__main__":
    print("Phase 4 unit tests")
    test_can_execute_valid_buy()
    test_reject_test_algorithm()
    test_reject_max_positions()
    test_accept_when_position_zero()
    test_reject_different_symbol_open_does_not_block()
    test_reject_invalid_signal()
    test_reject_missing_field()
    test_bracket_prices_buy()
    test_bracket_prices_sell()
    test_execute_signal_builds_bracket_and_reports_failure()
    test_execute_signal_uses_signal_symbol()
    print("All 11 Phase 4 unit tests passed.")