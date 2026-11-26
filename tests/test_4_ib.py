"""Paper-account integration test that submits one bracket order.

Run this only after the offline checks in ``test_4.py`` pass. This test can
submit a real order to the configured IB paper account.
"""
import math
import time

from ib_insync import IB, Forex

from config.settings import IB_HOST, IB_PORT, IB_CLIENT_ID_EXECUTOR, SYMBOL
from src.executor.signal_executor import SignalExecutor


def _current_buy_reference(ex, timeout=15):
    ticker = ex.ib.reqMktData(ex.contract, "", False, False)
    deadline = time.monotonic() + timeout
    try:
        while True:
            ask = ticker.ask
            if isinstance(ask, (int, float)) and math.isfinite(ask) and ask > 0:
                return float(ask)
            if time.monotonic() >= deadline:
                raise TimeoutError("IB did not provide a valid EURUSD ask price.")
            ex.ib.sleep(0.1)
    finally:
        ex.ib.cancelMktData(ex.contract)


def run_connectivity_check():
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID_EXECUTOR, timeout=15)
    print(f"Connected to IB. Accounts: {ib.managedAccounts()}")
    ib.disconnect()


def submit_paper_bracket_order():
    """Submit a 10,000 EURUSD BUY bracket order to the paper account."""
    ex = SignalExecutor()
    ex._connect_with_retry()

    try:
        if ex.connected_client_id != IB_CLIENT_ID_EXECUTOR:
            raise RuntimeError(
                f"clientId={IB_CLIENT_ID_EXECUTOR} is already in use; "
                "se cancela el test antes de enviar una orden."
            )

        accounts = ex.ib.managedAccounts()
        if IB_PORT not in (4002, 7497) or not any(
            account.startswith("DU") for account in accounts
        ):
            raise RuntimeError(
                "The test was cancelled to avoid submitting outside the paper account. "
                f"Puerto={IB_PORT}, cuentas={accounts}"
            )

        ex.ib.qualifyContracts(ex.contract)

        print("Connected to IB paper. Submitting a BUY bracket...")
        reference_price = _current_buy_reference(ex)
        print(f"Precio ask de EURUSD para el bracket: {reference_price:.5f}")
        signal = {
            "id_algoritmo": "ema_cross_v1",
            "symbol": SYMBOL,
            "signal": "BUY",
            "precio_referencia": reference_price,
            "timestamp": "2026-01-01T00:00:00Z",
        }
        if not ex.execute_signal(signal):
            raise RuntimeError(
                "No se pudo enviar el bracket. Revisa el log del ejecutor."
            )
        print(
            "Bracket order request submitted to IB. "
            "Confirm the order status in TWS/IB Gateway."
        )
    finally:
        if ex.ib.isConnected():
            ex.ib.disconnect()


if __name__ == "__main__":
    print("Phase 4 paper-account integration test")
    run_connectivity_check()
    print()
    print("Siguiente: enviar una orden real a paper. Ctrl+C en 5s para cancelar.")
    try:
        time.sleep(5)
        submit_paper_bracket_order()
    except KeyboardInterrupt:
        print("Cancelado por el usuario.")