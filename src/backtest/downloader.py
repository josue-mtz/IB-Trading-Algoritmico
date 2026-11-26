"""Load or download historical one-minute foreign-exchange candles.

Interactive Brokers is the primary source. A local CSV is reused when
available, avoiding a network request on repeat backtests.
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
    """Return whether the configured historical CSV is already present."""
    return CSV_PATH.exists()


def load_local_data() -> pd.DataFrame:
    """Read, normalize, and sort the local historical CSV."""
    log.info("Loading local historical data: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    df["inicio"] = pd.to_datetime(df["inicio"], utc=True)
    df = df.sort_values("inicio").reset_index(drop=True)
    return df


def download_from_ib(days: int = BACKTEST_DAYS, client_id: int = 99) -> pd.DataFrame:
    """Download one-minute bars from IB in chunks and return the shared schema."""
    if days < 1:
        raise ValueError("days must be at least 1.")

    log.info("Connecting to IB to download %s days of one-minute bars.", days)
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
                    f"IB returned no data for the {chunk_days}-day block ending "
                    f"at {end_time.isoformat()}."
                )
            bars.extend(chunk)
            log.info(
                f"{chunk_days}-day block: {len(chunk)} bars; "
                f"{chunk[0].date} → {chunk[-1].date}"
            )
            end_time -= timedelta(days=chunk_days)
            remaining_days -= chunk_days

        df = util.df(bars)
        df = df.rename(columns={"date": "inicio", "volume": "num_ticks"})
        df["inicio"] = pd.to_datetime(df["inicio"], utc=True)
        df["symbol"] = SYMBOL
        # Use the provider's volume field as the tick-count proxy when available.
        if "num_ticks" not in df.columns:
            df["num_ticks"] = 0
        df = (
            df[REQUIRED_COLS]
            .drop_duplicates(subset=["inicio"], keep="last")
            .sort_values("inicio")
            .reset_index(drop=True)
        )
        log.info(
            "Downloaded %s bars: %s to %s",
            len(df),
            df["inicio"].min(),
            df["inicio"].max(),
        )
        return df
    finally:
        ib.disconnect()


def save_data(df: pd.DataFrame) -> None:
    """Save historical bars in the configured CSV location."""
    df.to_csv(CSV_PATH, index=False)
    log.info("Historical data saved to %s", CSV_PATH)


def get_or_download(days: int = BACKTEST_DAYS, force: bool = False) -> pd.DataFrame:
    """Reuse the local CSV or download and save a fresh copy."""
    if has_local_data() and not force:
        return load_local_data()
    df = download_from_ib(days=days)
    save_data(df)
    return df