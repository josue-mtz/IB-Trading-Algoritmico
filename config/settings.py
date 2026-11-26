"""Central application settings loaded from environment variables and ``.env``.

Defaults are safe for local development and paper trading. External message
fields and existing Kafka topic names are kept compatible with earlier runs.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# IB Gateway
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", 4002))
IB_CLIENT_ID_PRODUCER = int(os.getenv("IB_CLIENT_ID_PRODUCER", 1))
IB_CLIENT_ID_EXECUTOR = int(os.getenv("IB_CLIENT_ID_EXECUTOR", 2))
IB_ACCOUNT = os.getenv("IB_ACCOUNT")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TICKS = os.getenv("TOPIC_TICKS", "ticks")
TOPIC_CANDLES = os.getenv(
    "TOPIC_CANDLES",
    os.getenv("TOPIC_VELAS", "velas"),
)
# Backward-compatible constant and environment-variable default.
TOPIC_VELAS = TOPIC_CANDLES
TOPIC_SIGNALS = os.getenv("TOPIC_SIGNALS", "signals")

# Trading
SYMBOL = os.getenv("SYMBOL", "EURUSD")
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", 60))

# Directories
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Strategy parameters
EMA_FAST = int(os.getenv("EMA_FAST", 9))
EMA_SLOW = int(os.getenv("EMA_SLOW", 21))

# Strategy filters
USE_RSI_FILTER = os.getenv("USE_RSI_FILTER", "false").lower() == "true"
RSI_PERIOD = int(os.getenv("RSI_PERIOD", 14))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", 70))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", 30))
USE_TREND_FILTER = os.getenv("USE_TREND_FILTER", "false").lower() == "true"
EMA_TREND_PERIOD = int(os.getenv("EMA_TREND_PERIOD", 50))

# Order executor and risk controls
RISK_TRADE_SIZE = int(os.getenv("RISK_TRADE_SIZE", 10000))
RISK_STOP_LOSS_PIPS = float(os.getenv("RISK_STOP_LOSS_PIPS", 10))
RISK_TAKE_PROFIT_PIPS = float(os.getenv("RISK_TAKE_PROFIT_PIPS", 20))
RISK_MAX_POSITIONS = int(os.getenv("RISK_MAX_POSITIONS", 1))
EXECUTOR_ALLOWED_ALGORITHMS = os.getenv(
    "EXECUTOR_ALLOWED_ALGORITHMS", "ema_cross_v1"
).split(",")

# Backtest settings
BACKTEST_DAYS = int(os.getenv("BACKTEST_DAYS", 30))
BACKTEST_SPREAD_PIPS = float(os.getenv("BACKTEST_SPREAD_PIPS", 2.0))
BACKTEST_SLIPPAGE_PIPS = float(os.getenv("BACKTEST_SLIPPAGE_PIPS", 0.5))
BACKTEST_SL_PIPS = float(os.getenv("BACKTEST_SL_PIPS", 10))
BACKTEST_TP_PIPS = float(os.getenv("BACKTEST_TP_PIPS", 20))

DATA_DIR = BASE_DIR / "data" / "historical"
REPORTS_DIR = BASE_DIR / "reports"
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Runtime and dashboard ----------
# Shared state directory used by the dashboard, strategy, and order executor.
RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", BASE_DIR / "data" / "runtime"))
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", 8050))
# When true, the dashboard also starts run_producer.py (IB ticks -> Kafka).
# Set to false if the producer is already running in another terminal.
DASHBOARD_MANAGE_PRODUCER = os.getenv("DASHBOARD_MANAGE_PRODUCER", "true").lower() == "true"
# Time zone used to group daily profit and loss (system time zone by default).
DASHBOARD_TZ = os.getenv("DASHBOARD_TZ", "")
# Default dashboard strategy (see src/algorithms/variants.py).
DEFAULT_VARIANT = os.getenv("DEFAULT_VARIANT", "v3")
