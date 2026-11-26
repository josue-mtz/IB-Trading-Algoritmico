"""Inicia, detiene e informa el estado de los procesos controlados por el panel.

Los ID de proceso se guardan para que el panel pueda volver a reconocer
procesos que sigan activos después de reiniciar el servidor web.
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from config.settings import BASE_DIR, LOGS_DIR, RUNTIME_DIR
from src.algorithms.variants import VARIANTS
from src.runtime.state import WORKERS_FILE, now_iso, read_json, write_json_atomic

IS_WINDOWS = os.name == "nt"


def pid_alive(pid: int) -> bool:
    if not pid:
        return False
    if IS_WINDOWS:
        # En Windows, os.kill(pid, 0) puede terminar el proceso; se usa tasklist.
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True,
        ).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class ProcessManager:
    def __init__(self, base_dir: Path = BASE_DIR, runtime_dir: Path = RUNTIME_DIR,
                 workers_file: Path = WORKERS_FILE):
        self.base_dir = Path(base_dir)
        self.runtime_dir = Path(runtime_dir)
        self.workers_file = Path(workers_file)
        self._procs: Dict[str, subprocess.Popen] = {}

    # ---------- Comandos de procesos ----------

    def command(self, name: str, variant: str) -> List[str]:
        py = [sys.executable, "-u"]
        if name == "producer":
            return py + ["run_producer.py"]
        if name == "executor":
            return py + ["run_executor.py"]
        if name == "algorithm":
            return py + ["run_algorithms.py", "--variant", variant]
        raise ValueError(f"Unknown worker: {name}")

    # ---------- Estado de los procesos ----------

    def _load(self) -> dict:
        data = read_json(self.workers_file, {})
        return data if isinstance(data, dict) else {}

    def _save(self, data: dict) -> None:
        write_json_atomic(self.workers_file, data)

    def is_alive(self, name: str) -> bool:
        proc = self._procs.get(name)
        if proc is not None:
            return proc.poll() is None  # poll() también recoge los procesos secundarios terminados.
        info = self._load().get(name)
        return bool(info) and pid_alive(int(info.get("pid", 0)))

    def status(self) -> Dict[str, dict]:
        stored = self._load()
        out = {}
        for name in ("producer", "algorithm", "executor"):
            info = stored.get(name) or {}
            out[name] = {
                "alive": self.is_alive(name),
                "pid": info.get("pid"),
                "started_at": info.get("started_at"),
                "variant": info.get("variant"),
            }
        return out

    # ---------- Acciones de los procesos ----------

    def start(self, name: str, variant: str) -> bool:
        """Inicia el proceso si no está activo y devuelve si se inició."""
        if name == "algorithm" and variant not in VARIANTS:
            raise ValueError(f"Unknown strategy variant: {variant}")
        if self.is_alive(name):
            return False
        LOGS_DIR.mkdir(exist_ok=True)
        out = open(LOGS_DIR / f"worker_{name}.out", "a", encoding="utf-8")
        env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1",
                   TRADING_CONTROL="dashboard")  # El ejecutor lee controles del panel solo en este modo.
        kwargs = {}
        if IS_WINDOWS:
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True  # Crea un grupo para poder detener todos sus procesos descendientes.
        proc = subprocess.Popen(
            self.command(name, variant), cwd=str(self.base_dir), env=env,
            stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, **kwargs,
        )
        self._procs[name] = proc
        data = self._load()
        data[name] = {"pid": proc.pid, "started_at": now_iso(), "variant": variant}
        self._save(data)
        return True

    def stop(self, name: str, timeout: float = 8.0) -> bool:
        """Intenta detener el proceso de forma ordenada y lo fuerza si es necesario."""
        if not self.is_alive(name):
            self._forget(name)
            return False
        info = self._load().get(name) or {}
        proc = self._procs.get(name)
        pid = proc.pid if proc is not None else int(info.get("pid", 0))
        self._terminate(pid, force=False)
        deadline = time.time() + timeout
        while time.time() < deadline and self.is_alive(name):
            time.sleep(0.2)
        if self.is_alive(name):
            self._terminate(pid, force=True)
            time.sleep(0.3)
        self._forget(name)
        return True

    def _terminate(self, pid: int, force: bool) -> None:
        if IS_WINDOWS:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True)
            return
        sig = signal.SIGKILL if force else signal.SIGTERM
        try:
            os.killpg(pid, sig)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                pass

    def _forget(self, name: str) -> None:
        self._procs.pop(name, None)
        data = self._load()
        if name in data:
            del data[name]
            self._save(data)

    def start_all(self, variant: str, manage_producer: bool = True) -> List[str]:
        """Inicia los procesos faltantes y reinicia la estrategia si cambia la variante."""
        started = []
        current = self.status()["algorithm"]
        if current["alive"] and current["variant"] != variant:
            self.stop("algorithm")
        names = (["producer"] if manage_producer else []) + ["algorithm", "executor"]
        for name in names:
            if self.start(name, variant):
                started.append(name)
        return started

    def stop_all(self) -> List[str]:
        return [n for n in ("executor", "algorithm", "producer") if self.stop(n)]
