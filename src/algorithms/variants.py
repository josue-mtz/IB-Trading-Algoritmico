"""Variantes del algoritmo EMA Cross (sin dependencias pesadas; las usa también el dashboard)."""

VARIANTS = {
    "v1": {"id_algoritmo": "ema_cross_v1", "label": "EMA Cross base"},
    "v2": {"use_rsi_filter": True, "id_algoritmo": "ema_cross_rsi", "label": "EMA Cross + RSI"},
    "v3": {"use_trend_filter": True, "id_algoritmo": "ema_cross_trend", "label": "EMA Cross + Tendencia"},
    "v4": {"use_rsi_filter": True, "use_trend_filter": True,
           "id_algoritmo": "ema_cross_rsi_trend", "label": "EMA Cross + RSI + Tendencia"},
}


def algo_kwargs(variant: str) -> dict:
    """kwargs para EMACross(...) (sin la etiqueta de UI)."""
    return {k: v for k, v in VARIANTS[variant].items() if k != "label"}
