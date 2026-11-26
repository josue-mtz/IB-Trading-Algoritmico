"""Inicia el panel de control local de trading.

Usa ``--demo`` para mostrar datos sintéticos sin conectarse a IB ni a Kafka.
"""
import argparse
import os
import sys
import tempfile
import webbrowser
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(
        description="Start the local paper-trading dashboard."
    )
    p.add_argument(
        "--demo",
        action="store_true",
        help="use synthetic data without IB or Kafka",
    )
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--no-open", action="store_true", help="do not open a browser")
    args = p.parse_args()

    demo_dir = None
    if args.demo:
        # Se establece antes de importar config.settings, que lee RUNTIME_DIR al cargarse.
        demo_dir = Path(tempfile.mkdtemp(prefix="fx_dashboard_demo_"))
        os.environ["RUNTIME_DIR"] = str(demo_dir)

    from config.settings import DASHBOARD_HOST, DASHBOARD_PORT, LOGS_DIR
    from dashboard.server import DashboardApp, serve

    host = args.host or DASHBOARD_HOST
    port = args.port or DASHBOARD_PORT
    if host not in ("127.0.0.1", "localhost"):
        sys.exit(
            "For safety, the dashboard only listens on 127.0.0.1 "
            "because it can control an order executor."
        )

    if args.demo:
        from dashboard.demo import DemoProcessManager, seed_demo
        orders_log = demo_dir / "orders.jsonl"
        seed_demo(orders_log)
        app = DashboardApp(pm=DemoProcessManager(), orders_log=orders_log, demo=True)
    else:
        app = DashboardApp(orders_log=LOGS_DIR / "orders.jsonl")

    httpd = serve(app, host, port)
    url = f"http://127.0.0.1:{port}"
    print(
        f"Dashboard at {url}{' (DEMO MODE)' if args.demo else ''}. "
        "Press Ctrl+C to stop.",
        flush=True,
    )
    print(
        "Closing the dashboard does not stop workers; use the Stop workers button.",
        flush=True,
    )
    if not args.no_open:
        webbrowser.open(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
