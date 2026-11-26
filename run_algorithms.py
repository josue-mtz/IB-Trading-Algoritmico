import argparse
from src.algorithms.ema_cross import EMACross
from src.algorithms.variants import VARIANTS, algo_kwargs

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--variant", default="v1", choices=list(VARIANTS))
    args = p.parse_args()
    algo = EMACross(**algo_kwargs(args.variant))
    algo.run()
