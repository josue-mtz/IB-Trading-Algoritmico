"""Configuraciones de estrategias EMA compartidas por el ejecutor y el panel."""

VARIANTS = {
    "v1": {"id_algoritmo": "ema_cross_v1", "label": "Base EMA crossover"},
    "v2": {"use_rsi_filter": True, "id_algoritmo": "ema_cross_rsi", "label": "EMA crossover + RSI"},
    "v3": {"use_trend_filter": True, "id_algoritmo": "ema_cross_trend", "label": "EMA crossover + trend filter"},
    "v4": {"use_rsi_filter": True, "use_trend_filter": True,
           "id_algoritmo": "ema_cross_rsi_trend", "label": "EMA crossover + RSI + trend filter"},
}


def algo_kwargs(variant: str) -> dict:
    """Devuelve los argumentos de ``EMACross`` sin incluir la etiqueta visible."""
    return {k: v for k, v in VARIANTS[variant].items() if k != "label"}
