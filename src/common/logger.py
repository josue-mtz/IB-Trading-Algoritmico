"""Configura el registro uniforme en consola y archivos UTF-8 para la aplicación."""
import logging
import sys
from config.settings import LOGS_DIR


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger con nombre y un controlador para consola y otro para archivo."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # La salida de consola aparece en la terminal de cada proceso.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # El archivo de registro sigue disponible cuando termina el proceso.
    file_handler = logging.FileHandler(LOGS_DIR / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger