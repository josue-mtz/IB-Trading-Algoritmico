"""
Dashboard de trading: python run_dashboard.py [--demo] [--port 8050] [--no-open]

  --demo   datos sintéticos en una carpeta temporal (no necesita IB ni Kafka, no lanza workers)
"""
import argparse
import os
import sys
import tempfile
import webbrowser
from pathlib import Path


def main() -> None:
    p = argparse.ArgumentParser(description="Dashboard de trading algorítmico (demo IB)")
    p.add_argument("--demo", action="store_true", help="datos sintéticos, sin IB ni Kafka")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    p.add_argument("--no-open", action="store_true", help="no abrir el navegador")
    args = p.parse_args()

    demo_dir = None
    if args.demo:
        # Debe fijarse ANTES de importar config.settings (RUNTIME_DIR se lee al importar).
        demo_dir = Path(tempfile.mkdtemp(prefix="fx_dashboard_demo_"))
        os.environ["RUNTIME_DIR"] = str(demo_dir)

    from config.settings import DASHBOARD_HOST, DASHBOARD_PORT, LOGS_DIR
    from dashboard.server import DashboardApp, serve

    host = args.host or DASHBOARD_HOST
    port = args.port or DASHBOARD_PORT
    if host not in ("127.0.0.1", "localhost"):
        sys.exit("Por seguridad el dashboard solo escucha en 127.0.0.1 (controla un ejecutor de órdenes).")

    if args.demo:
        from dashboard.demo import DemoProcessManager, seed_demo
        orders_log = demo_dir / "orders.jsonl"
        seed_demo(orders_log)
        app = DashboardApp(pm=DemoProcessManager(), orders_log=orders_log, demo=True)
    else:
        app = DashboardApp(orders_log=LOGS_DIR / "orders.jsonl")

    httpd = serve(app, host, port)
    url = f"http://127.0.0.1:{port}"
    print(f"Dashboard en {url}{'  (MODO DEMO)' if args.demo else ''}  — Ctrl+C para salir", flush=True)
    print("Nota: cerrar el dashboard NO detiene los workers; usa el botón 'Apagar workers'.", flush=True)
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
