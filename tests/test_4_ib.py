"""
Test de integración de Fase 4. Envía UNA orden bracket al paper account.
Esto crea una orden real en tu cuenta demo DUR392579.
Usar solo cuando el test unitario test_4.py ya pasó.
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
                raise TimeoutError("IB no entregó un precio ask válido para EURUSD.")
            ex.ib.sleep(0.1)
    finally:
        ex.ib.cancelMktData(ex.contract)


def test_connectivity():
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID_EXECUTOR, timeout=15)
    print(f"✅ Conectado a IB. Cuentas: {ib.managedAccounts()}")
    ib.disconnect()


def test_paper_bracket_order():
    """Envía una orden BUY bracket de 10.000 EURUSD a la cuenta demo."""
    ex = SignalExecutor()
    ex._connect_with_retry()

    try:
        if ex.connected_client_id != IB_CLIENT_ID_EXECUTOR:
            raise RuntimeError(
                f"El clientId={IB_CLIENT_ID_EXECUTOR} ya está ocupado; "
                "se cancela el test antes de enviar una orden."
            )

        accounts = ex.ib.managedAccounts()
        if IB_PORT not in (4002, 7497) or not any(
            account.startswith("DU") for account in accounts
        ):
            raise RuntimeError(
                f"Se canceló el test para evitar una orden fuera de paper. "
                f"Puerto={IB_PORT}, cuentas={accounts}"
            )

        ex.ib.qualifyContracts(ex.contract)

        print("Conectado a IB paper. Enviando bracket BUY...")
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
            "✅ Solicitud de bracket enviada a IB. "
            "Confirma el estado de las órdenes en TWS/IB Gateway."
        )
    finally:
        if ex.ib.isConnected():
            ex.ib.disconnect()


if __name__ == "__main__":
    print(" Test de integración Fase 4 ")
    test_connectivity()
    print()
    print("Siguiente: enviar una orden real a paper. Ctrl+C en 5s para cancelar.")
    try:
        time.sleep(5)
        test_paper_bracket_order()
    except KeyboardInterrupt:
        print("Cancelado por el usuario.")