"""
Estado compartido entre dashboard, algoritmo y ejecutor.

Todo vive en archivos dentro de RUNTIME_DIR (escritura atómica) para que los
procesos sean independientes entre sí y sobrevivan a reinicios del dashboard.

  control.json        -> lo escribe el dashboard (botón Iniciar/Pausar, variante)
  executor_status.json-> heartbeat del ejecutor (IB conectado, precio, posiciones)
  algo_status.json    -> heartbeat del algoritmo (última vela, EMAs, calentamiento)
  fills.jsonl         -> ejecuciones reales reportadas por IB (entradas/salidas)
  open_orders_map.json-> orderId -> (trade_id, rol) para casar entradas con salidas
  workers.json        -> PIDs de los procesos lanzados por el dashboard
"""
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

from config.settings import RUNTIME_DIR, DEFAULT_VARIANT

CONTROL_FILE = RUNTIME_DIR / "control.json"
EXECUTOR_STATUS_FILE = RUNTIME_DIR / "executor_status.json"
ALGO_STATUS_FILE = RUNTIME_DIR / "algo_status.json"
FILLS_FILE = RUNTIME_DIR / "fills.jsonl"
ORDER_MAP_FILE = RUNTIME_DIR / "open_orders_map.json"
WORKERS_FILE = RUNTIME_DIR / "workers.json"


# ---------- Utilidades de archivo ----------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().isoformat().replace("+00:00", "Z")


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    """Parsea timestamps ISO (con o sin 'Z'). Naive => UTC."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def write_json_atomic(path: Path, data: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, default=str), encoding="utf-8")
    # En Windows os.replace falla si un lector tiene el archivo abierto: reintentar.
    for attempt in range(10):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.05 * (attempt + 1))
    os.replace(tmp, path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def append_jsonl(path: Path, entry: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")


def read_jsonl(path: Path) -> List[dict]:
    """Lee un .jsonl ignorando líneas corruptas (p. ej. una escritura a medias)."""
    rows: List[dict] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return rows


def install_graceful_shutdown() -> None:
    """Convierte SIGTERM en KeyboardInterrupt para que los `finally` de los workers
    cierren Kafka e IB limpiamente cuando el dashboard los detiene."""
    def _handler(signum, frame):
        raise KeyboardInterrupt()
    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError):
        pass  # no estamos en el hilo principal


# ---------- Control (botón Iniciar / Pausar) ----------

def _default_control() -> dict:
    return {
        "trading_enabled": False,
        "variant": DEFAULT_VARIANT,
        "first_started_at": None,
        "resumed_at": None,
        "accumulated_active_s": 0.0,
        "updated_at": None,
    }


def load_control() -> dict:
    control = _default_control()
    stored = read_json(CONTROL_FILE, {})
    if isinstance(stored, dict):
        control.update(stored)
    return control


def save_control(control: dict) -> dict:
    control["updated_at"] = now_iso()
    write_json_atomic(CONTROL_FILE, control)
    return control


def is_trading_enabled() -> bool:
    return bool(load_control().get("trading_enabled"))


def set_trading_enabled(enabled: bool) -> dict:
    """Activa/pausa el trading acumulando el tiempo activo (para 'tiempo corriendo')."""
    control = load_control()
    now = utcnow()
    if enabled and not control["trading_enabled"]:
        control["resumed_at"] = now.isoformat()
        control["first_started_at"] = control["first_started_at"] or now.isoformat()
    elif not enabled and control["trading_enabled"]:
        resumed = parse_iso(control.get("resumed_at"))
        if resumed:
            control["accumulated_active_s"] += max(0.0, (now - resumed).total_seconds())
        control["resumed_at"] = None
    control["trading_enabled"] = bool(enabled)
    return save_control(control)


def set_variant(variant: str) -> dict:
    control = load_control()
    control["variant"] = variant
    return save_control(control)


def active_seconds(control: Optional[dict] = None, now: Optional[datetime] = None) -> float:
    """Segundos totales con el trading activado (excluye pausas)."""
    control = control or load_control()
    total = float(control.get("accumulated_active_s") or 0.0)
    if control.get("trading_enabled"):
        resumed = parse_iso(control.get("resumed_at"))
        if resumed:
            total += max(0.0, ((now or utcnow()) - resumed).total_seconds())
    return total
