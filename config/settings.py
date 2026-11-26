"""La configuración central de la aplicación se carga desde las variables de entorno y el archivo ``.env``.

Los valores predeterminados son seguros para el desarrollo local y las simulaciones de operaciones, los campos de mensajes externos y los nombres de temas de Kafka existentes se mantienen compatibles con ejecuciones anteriores.
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
# Constante y valor predeterminado de variable de entorno con compatibilidad con versiones anteriores
TOPIC_VELAS = TOPIC_CANDLES
TOPIC_SIGNALS = os.getenv("TOPIC_SIGNALS", "signals")

# Trading
SYMBOL = os.getenv("SYMBOL", "EURUSD")
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", 60))

# Directorios
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Parámetros de la estrategia
EMA_FAST = int(os.getenv("EMA_FAST", 9))
EMA_SLOW = int(os.getenv("EMA_SLOW", 21))

# Filtros de la estrategia
USE_RSI_FILTER = os.getenv("USE_RSI_FILTER", "false").lower() == "true"
RSI_PERIOD = int(os.getenv("RSI_PERIOD", 14))
RSI_OVERBOUGHT = float(os.getenv("RSI_OVERBOUGHT", 70))
RSI_OVERSOLD = float(os.getenv("RSI_OVERSOLD", 30))
USE_TREND_FILTER = os.getenv("USE_TREND_FILTER", "false").lower() == "true"
EMA_TREND_PERIOD = int(os.getenv("EMA_TREND_PERIOD", 50))

# Ejecutor de órdenes y controles de riesgo
RISK_TRADE_SIZE = int(os.getenv("RISK_TRADE_SIZE", 10000))
RISK_STOP_LOSS_PIPS = float(os.getenv("RISK_STOP_LOSS_PIPS", 10))
RISK_TAKE_PROFIT_PIPS = float(os.getenv("RISK_TAKE_PROFIT_PIPS", 20))
RISK_MAX_POSITIONS = int(os.getenv("RISK_MAX_POSITIONS", 1))
EXECUTOR_ALLOWED_ALGORITHMS = os.getenv(
    "EXECUTOR_ALLOWED_ALGORITHMS", "ema_cross_v1"
).split(",")

# Backtest ajustes
BACKTEST_DAYS = int(os.getenv("BACKTEST_DAYS", 30))
BACKTEST_SPREAD_PIPS = float(os.getenv("BACKTEST_SPREAD_PIPS", 2.0))
BACKTEST_SLIPPAGE_PIPS = float(os.getenv("BACKTEST_SLIPPAGE_PIPS", 0.5))
BACKTEST_SL_PIPS = float(os.getenv("BACKTEST_SL_PIPS", 10))
BACKTEST_TP_PIPS = float(os.getenv("BACKTEST_TP_PIPS", 20))

DATA_DIR = BASE_DIR / "data" / "historical"
REPORTS_DIR = BASE_DIR / "reports"
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Entorno de ejecución y panel de control
# Directorio de estado compartido utilizado por el panel de control, la estrategia y el ejecutor de órdenes
RUNTIME_DIR = Path(os.getenv("RUNTIME_DIR", BASE_DIR / "data" / "runtime"))
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", 8050))
# Si es true, el panel de control también inicia run_producer.py (ticks de IB -> Kafka)
# Establécelo en false si el productor ya se está ejecutando en otra terminal
DASHBOARD_MANAGE_PRODUCER = os.getenv("DASHBOARD_MANAGE_PRODUCER", "true").lower() == "true"
# Zona horaria utilizada para agrupar las ganancias y pérdidas diarias (por defecto, la zona horaria del sistema)
DASHBOARD_TZ = os.getenv("DASHBOARD_TZ", "")
# Estrategia de dashboard (véase src/algorithms/variants.py)
DEFAULT_VARIANT = os.getenv("DEFAULT_VARIANT", "v3")
