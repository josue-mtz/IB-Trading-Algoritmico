import asyncio
from datetime import datetime
from ib_insync import IB, Forex, util
from config.settings import (
    IB_HOST, IB_PORT, IB_CLIENT_ID_PRODUCER, TOPIC_TICKS, SYMBOL,
)
from src.common.kafka_utils import make_producer, serialize_json, create_topic_if_not_exists
from src.common.logger import get_logger

log = get_logger("ib_tick_producer")

class IBTickProducer:
    def __init__(self):
        self.ib = IB()
        self.producer = make_producer()
        self.contract = Forex(SYMBOL)  # EURUSD, exchange IDEALPRO por defecto
        self.running = False
        self.tick_count = 0

    def _on_pending_tickers(self, tickers):
        """Callback que se ejecuta en cada actualización de precio."""
        for ticker in tickers:
            # En Forex, el 'last' puede ser NaN; usamos bid/ask
            bid = ticker.bid if ticker.bid and ticker.bid > 0 else None
            ask = ticker.ask if ticker.ask and ticker.ask > 0 else None

            if bid is None or ask is None or bid >= ask:
                continue  # tick inválido, lo saltamos

            tick_data = {
                "symbol": SYMBOL,
                "bid": round(bid, 5),
                "ask": round(ask, 5),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

            try:
                self.producer.produce(
                    TOPIC_TICKS,
                    key=SYMBOL.encode("utf-8"),
                    value=serialize_json(tick_data),
                )
                self.producer.poll(0)  # sirve callbacks sin bloquear
                self.tick_count += 1
                log.info(f"Tick #{self.tick_count} -> {tick_data}")
            except Exception as e:
                log.error(f"Error publicando tick: {e}")

    async def _connect_with_retry(self, max_attempts=5, step=10):
        """Intenta conectar probando clientIds desplazados si el original está ocupado."""
        base_id = IB_CLIENT_ID_PRODUCER
        for attempt in range(max_attempts):
            cid = base_id + attempt * step  # 1, 11, 21, ...
            try:
                await self.ib.connectAsync(IB_HOST, IB_PORT, clientId=cid)
                log.info(f"Conectado con clientId={cid}")
                return
            except Exception as e:
                log.warning(f"Intento {attempt+1}/{max_attempts} (clientId={cid}) falló: {e}")
                await asyncio.sleep(2)
        raise RuntimeError("No se pudo conectar a IB Gateway tras varios intentos.")

    async def _connect_and_subscribe(self):
        """Conecta a IB Gateway y se suscribe al contrato Forex."""
        await self._connect_with_retry()
        await self.ib.qualifyContractsAsync(self.contract)
        log.info(f"Contrato calificado: {self.contract}")
        self.ib.pendingTickersEvent += self._on_pending_tickers
        self.ib.reqMktData(self.contract, "", False, False)
        log.info(f"Suscrito a datos de mercado para {SYMBOL}. Esperando ticks...")

    def run(self):
        """Bucle principal: conecta, se suscribe y mantiene la sesión viva."""
        # Crea el tópico antes de empezar a publicar
        create_topic_if_not_exists(TOPIC_TICKS)

        self.running = True
        try:
            asyncio.get_event_loop().run_until_complete(self._connect_and_subscribe())
            # Mantiene el programa vivo escuchando ticks
            self.ib.run()
        except KeyboardInterrupt:
            log.info("Detenido por el usuario (Ctrl+C).")
        finally:
            self._shutdown()

    def _shutdown(self):
        self.running = False
        log.info(f"Cerrando. Total de ticks publicados: {self.tick_count}")
        if self.ib.isConnected():
            self.ib.disconnect()
        self.producer.flush(timeout=5)
        log.info("Productor detenido.")