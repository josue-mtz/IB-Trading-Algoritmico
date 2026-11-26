"""
Descarga (o carga) datos históricos EURUSD M1.
Primario: IB reqHistoricalData (límite paper: 30 días M1).
Fallback: CSV manual en data/historical/EURUSD_M1.csv (formato HistData).
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
from ib_insync import IB, Forex, util

from config.settings import IB_HOST, IB_PORT, SYMBOL, BACKTEST_DAYS, DATA_DIR
from src.common.logger import get_logger

log = get_logger("backtest_downloader")

CSV_PATH = DATA_DIR / f"{SYMBOL}_M1.csv"
REQUIRED_COLS = ["symbol", "inicio", "open", "high", "low", "close", "num_ticks"]
MAX_CHUNK_DAYS = 7


def has_local_data() -> bool:
    return CSV_PATH.exists()


def load_local_data() -> pd.DataFrame:
    log.info(f"Cargando datos locales: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    df["inicio"] = pd.to_datetime(df["inicio"], utc=True)
    df = df.sort_values("inicio").reset_index(drop=True)
    return df


def download_from_ib(days: int = BACKTEST_DAYS, client_id: int = 99) -> pd.DataFrame:
    """Descarga velas M1 vía IB. Retorna DataFrame con esquema unificado."""
    if days < 1:
        raise ValueError("days debe ser al menos 1.")

    log.info(f"Conectando a IB para descargar {days} días de M1...")
    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=client_id, timeout=30)
    try:
        contract = Forex(SYMBOL, exchange="IDEALPRO", currency="USD")
        ib.qualifyContracts(contract)
        end_time = datetime.now(timezone.utc)
        remaining_days = days
        bars = []
        while remaining_days > 0:
            chunk_days = min(MAX_CHUNK_DAYS, remaining_days)
            chunk = ib.reqHistoricalData(
                contract,
                endDateTime=end_time,
                durationStr=f"{chunk_days} D",
                barSizeSetting="1 min",
                whatToShow="MIDPOINT",
                useRTH=False,
                formatDate=1,
                timeout=90,
            )
            if not chunk:
                raise RuntimeError(
                    f"IB no devolvió datos para el bloque de {chunk_days} "
                    f"días que termina en {end_time.isoformat()}."
                )
            bars.extend(chunk)
            log.info(
                f"Bloque de {chunk_days} días: {len(chunk)} velas; "
                f"{chunk[0].date} → {chunk[-1].date}"
            )
            end_time -= timedelta(days=chunk_days)
            remaining_days -= chunk_days

        df = util.df(bars)
        df = df.rename(columns={"date": "inicio", "volume": "num_ticks"})
        df["inicio"] = pd.to_datetime(df["inicio"], utc=True)
        df["symbol"] = SYMBOL
        # num_ticks como proxy de volumen si no viene
        if "num_ticks" not in df.columns:
            df["num_ticks"] = 0
        df = (
            df[REQUIRED_COLS]
            .drop_duplicates(subset=["inicio"], keep="last")
            .sort_values("inicio")
            .reset_index(drop=True)
        )
        log.info(f"Descargadas {len(df)} velas: {df['inicio'].min()} → {df['inicio'].max()}")
        return df
    finally:
        ib.disconnect()


def save_data(df: pd.DataFrame) -> None:
    df.to_csv(CSV_PATH, index=False)
    log.info(f"Datos guardados en {CSV_PATH}")


def get_or_download(days: int = BACKTEST_DAYS, force: bool = False) -> pd.DataFrame:
    """Usa datos locales si existen, si no descarga."""
    if has_local_data() and not force:
        return load_local_data()
    df = download_from_ib(days=days)
    save_data(df)
    return df