"""Inicia una estrategia de cruce de EMA y publica sus señales en Kafka."""
import argparse
from src.algorithms.ema_cross import EMACross
from src.algorithms.variants import VARIANTS, algo_kwargs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consume candle messages and publish EMA strategy signals."
    )
    parser.add_argument("--variant", default="v1", choices=list(VARIANTS))
    args = parser.parse_args()
    algorithm = EMACross(**algo_kwargs(args.variant))
    algorithm.run()
