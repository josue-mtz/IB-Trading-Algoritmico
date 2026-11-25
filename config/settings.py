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