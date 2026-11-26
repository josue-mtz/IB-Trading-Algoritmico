"""Publica en Kafka las cotizaciones bid y ask de EUR/USD recibidas de IB."""
import asyncio
from datetime import datetime, timezone
from ib_insync import IB, Forex, util
from config.settings import IB_HOST, IB_PORT, IB_CLIENT_ID_PRODUCER, TOPIC_TICKS, SYMBOL
from src.common.kafka_utils import make_producer, serialize_json, create_topic_if_not_exists
from src.common.logger import get_logger

log = get_logger("ib_tick_producer")


class IBTickProducer:
    """Mantiene una suscripción de datos de IB y publica cotizaciones válidas."""

    def __init__(self):
        self.ib = IB()
        self.producer = make_producer()
        self.contract = Forex(SYMBOL)
        self.running = False
        self.tick_count = 0

    def _on_pending_tickers(self, tickers):
        """Publica cotizaciones válidas cada vez que IB informa cambios."""
        for ticker in tickers:
            # Forex podría no proporcionar el último precio; en su lugar se usa bid y ask.
            bid = ticker.bid if ticker.bid and ticker.bid > 0 else None
            ask = ticker.ask if ticker.ask and ticker.ask > 0 else None

            if bid is None or ask is None or bid >= ask:
                continue

            tick_data = {
                "symbol": SYMBOL,
                "bid": round(bid, 5),
                "ask": round(ask, 5),
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }

            try:
                self.producer.produce(
                    TOPIC_TICKS,
                    key=SYMBOL.encode("utf-8"),
                    value=serialize_json(tick_data),
                )
                self.producer.poll(0)  # Atiende confirmaciones de entrega sin bloquear.
                self.tick_count += 1
                log.info("Published tick #%s: %s", self.tick_count, tick_data)
            except Exception as e:
                log.exception("Could not publish tick: %s", e)

    async def _connect_with_retry(self, max_attempts=5, step=10):
        """Prueba otros ID de cliente si el ID configurado ya está en uso."""
        base_id = IB_CLIENT_ID_PRODUCER
        for attempt in range(max_attempts):
            cid = base_id + attempt * step
            try:
                await self.ib.connectAsync(IB_HOST, IB_PORT, clientId=cid)
                log.info("Connected to IB Gateway with clientId=%s.", cid)
                return
            except Exception as e:
                log.warning(
                    "Connection attempt %s/%s (clientId=%s) failed: %s",
                    attempt + 1,
                    max_attempts,
                    cid,
                    e,
                )
                await asyncio.sleep(2)
        raise RuntimeError("Could not connect to IB Gateway after multiple attempts.")

    async def _connect_and_subscribe(self):
        """Se conecta a IB Gateway y se suscribe al par de divisas configurado."""
        await self._connect_with_retry()
        await self.ib.qualifyContractsAsync(self.contract)
        log.info("Qualified contract: %s", self.contract)
        self.ib.pendingTickersEvent += self._on_pending_tickers
        self.ib.reqMktData(self.contract, "", False, False)
        log.info("Subscribed to %s market data. Waiting for quotes.", SYMBOL)

    def run(self):
        """Crea el tópico Kafka, se conecta a IB y ejecuta el proceso hasta interrumpirse."""
        create_topic_if_not_exists(TOPIC_TICKS)

        self.running = True
        try:
            asyncio.get_event_loop().run_until_complete(self._connect_and_subscribe())
            # Mantiene el proceso activo mientras espera ticks.
            self.ib.run()
        except KeyboardInterrupt:
            log.info("Stopped by user (Ctrl+C).")
        finally:
            self._shutdown()

    def _shutdown(self):
        self.running = False
        log.info("Shutting down after publishing %s ticks.", self.tick_count)
        if self.ib.isConnected():
            self.ib.disconnect()
        self.producer.flush(timeout=5)
        log.info("Market-data producer stopped.")