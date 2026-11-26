import os
from pathlib import Path
from dotenv import load_dotenv

# Carga el .env desde la raíz del proyecto
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
TOPIC_VELAS = os.getenv("TOPIC_VELAS", "velas")
TOPIC_SIGNALS = os.getenv("TOPIC_SIGNALS", "signals")

# Trading
SYMBOL = os.getenv("SYMBOL", "EURUSD")
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", 60))

# Directorios
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Parámetros del algoritmo
EMA_FAST = int(os.getenv("EMA_FAST", 9))
EMA_SLOW = int(os.getenv("EMA_SLOW", 21))

# Ejecutor y control de riesgo
RISK_TRADE_SIZE = int(os.getenv("RISK_TRADE_SIZE", 10000))
RISK_STOP_LOSS_PIPS = float(os.getenv("RISK_STOP_LOSS_PIPS", 10))
RISK_TAKE_PROFIT_PIPS = float(os.getenv("RISK_TAKE_PROFIT_PIPS", 20))
RISK_MAX_POSITIONS = int(os.getenv("RISK_MAX_POSITIONS", 1))
EXECUTOR_ALLOWED_ALGORITHMS = os.getenv(
    "EXECUTOR_ALLOWED_ALGORITHMS", "ema_cross_v1"
).split(",")

# Backtest
BACKTEST_DAYS = int(os.getenv("BACKTEST_DAYS", 30))
BACKTEST_SPREAD_PIPS = float(os.getenv("BACKTEST_SPREAD_PIPS", 2.0))
BACKTEST_SLIPPAGE_PIPS = float(os.getenv("BACKTEST_SLIPPAGE_PIPS", 0.5))
BACKTEST_SL_PIPS = float(os.getenv("BACKTEST_SL_PIPS", 10))
BACKTEST_TP_PIPS = float(os.getenv("BACKTEST_TP_PIPS", 20))

DATA_DIR = BASE_DIR / "data" / "historical"
REPORTS_DIR = BASE_DIR / "reports"
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)