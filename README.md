# FX Algorithmic Trading

An educational, paper-trading project for EUR/USD. It connects Interactive
Brokers (IB), Kafka, Spark, EMA-based strategies, risk checks, an order
executor, historical backtests, and a local dashboard.

The project is intended for learning and controlled paper-account experiments.
It does not promise profits, and its strategies have not been established as
profitable. Do not treat backtest or paper results as a recommendation to trade
real money.

## What the application does

The live pipeline has four main stages:

1. **Market-data producer** reads bid/ask updates from IB and publishes tick
   messages to Kafka topic `ticks`.
2. **Candle processor** uses Spark to group ticks into time windows and publishes
   OHLC candle messages to Kafka topic `velas`.
3. **Strategy** reads candles, calculates EMA signals (and optional filters),
   then publishes signals to Kafka topic `signals`.
4. **Order executor** validates each signal, applies position limits, and can
   submit a bracket order to the configured IB account.

The dashboard displays runtime status, market data, signals, trades, and
account information. Its demo mode uses synthetic data and does not connect to
IB or Kafka.

## Main components

| Component | Purpose |
| --- | --- |
| `run_producer.py` | Start the IB-to-Kafka tick producer. |
| `run_candles.py` | Start Spark processing of Kafka ticks into candles. |
| `run_algorithms.py` | Run an EMA strategy variant and publish signals. |
| `run_executor.py` | Consume signals and apply risk checks before submitting orders. |
| `run_dashboard.py` | Start the local dashboard; `--demo` uses synthetic data. |
| `run_backtest.py` | Run a historical backtest and save metrics, trades, and a chart. |
| `run_ga_backtest.py` | Compare V3 with a genetic parameter search using walk-forward training. |
| `src/algorithms/` | Strategy, indicator, and variant implementations. |
| `src/spark/` | Tick-to-candle transformation and streaming job. |
| `src/executor/` | Signal validation, risk management, and IB bracket orders. |
| `src/runtime/` | Dashboard state, worker management, and trade ledger. |
| `tests/` | Offline unit tests and explicitly opt-in service integration tests. |
| `data/historical/` | Local historical candle CSV files. |
| `reports/` | Generated backtest and optimization reports. |

Some Spanish module names and message fields remain as compatibility
interfaces. In particular, Kafka topic `velas` and fields such as `inicio`,
`num_ticks`, `id_algoritmo`, and `precio_referencia` are retained so existing
consumers and stored data continue to work. New commands and source modules use
English names; the former `run_velas.py`, `run_consumer_velas.py`,
`base_algoritmo.py`, `indicadores.py`, and `ticks_to_velas.py` paths remain
compatibility wrappers.

## Requirements

- Windows 10 or 11.
- Python **3.10** for this tested setup. Keep Spark's driver and Python worker
  on the same Python minor version.
- Java 17 or newer for a local PySpark installation.
- Docker Desktop with Docker Compose for Kafka and the containerized Spark job.
- IB Gateway or Trader Workstation (TWS), configured for a **paper account**,
  only for live market data or IB integration tests.

The warning about `winutils.exe` on Windows can appear with Spark. It is often
non-fatal for local transformations. The Python worker version mismatch is not
safe to ignore: use the same Python version for the driver and worker.

## Windows installation

Open PowerShell in the project directory.

If the required tools are not installed, Windows Package Manager can install
Python 3.10, Java 17, and Docker Desktop:

```powershell
winget install --exact --id Python.Python.3.10
winget install --exact --id EclipseAdoptium.Temurin.17.JDK
winget install --exact --id Docker.DockerDesktop
```

Restart PowerShell after installation and start Docker Desktop. If you use
Conda, you can skip the standalone Python installation. Install IB Gateway or
TWS from Interactive Brokers separately and enable its paper-account API;
broker software and account setup are not installed by this project.

### Option A: Conda

```powershell
conda create -n IB python=3.10 -y
conda activate IB
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

### Option B: Python virtual environment

Install Python 3.10 first, then run:

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

If PowerShell blocks activation, either enable script execution for the current
user according to your organization's policy, or skip activation and run
`.venv\Scripts\python.exe -m pip install -r requirements.txt` and
`.venv\Scripts\python.exe <script.py>` explicitly.

Edit `.env` only with your own local settings. Do not commit it or share its
contents. `.env.example` contains placeholders and safe local defaults; it has
no account credentials.

### Check the Python environment

```powershell
python --version
python -c "import sys, pyspark; print(sys.executable); print(pyspark.__version__)"
```

Use the activated environment's `python -m ...` form rather than an unrelated
`pyspark` executable on `PATH`. `run_candles.py` sets PySpark's Python
interpreter to the same executable that starts the driver.

## Start Kafka and Spark

Start Docker Desktop, then from the repository root:

```powershell
docker compose up -d kafka
docker compose ps
docker compose up --build -d spark-velas
docker compose logs -f spark-velas
```

The Compose service keeps its historic `spark-velas` key so existing
installations update in place; its application code and entry point are in
English. It connects to Kafka over the Kafka container's network. Wait until
its log says `Streaming started`. Stop the services with
`Ctrl+C` in the logs view, then:

```powershell
docker compose down
```

To run Spark natively instead of in Docker, make sure Java is available and use
the same activated Python 3.10 environment:

```powershell
python run_candles.py
```

Do not run both Spark candle processors at the same time against the same
checkpoint. The checkpoint directory is `checkpoints/velas`; it preserves
stream offsets across restarts.

## Run the live paper pipeline

Before starting, configure IB Gateway/TWS for paper trading and confirm that
the API is enabled and reachable on the configured host and port. The default
paper port is `4002`. Keep each process in a separate PowerShell terminal,
with the same Python environment activated.

Start the services in this order:

**Terminal 1 — market data**

```powershell
python run_producer.py
```

**Terminal 2 — ticks to candles**

Use either the Docker Spark service above or, instead, run:

```powershell
python run_candles.py
```

**Terminal 3 — strategy**

```powershell
python run_algorithms.py --variant v3
```

Available variants are `v1`, `v2`, `v3`, and `v4`. Their filter settings are
defined in `src/algorithms/variants.py`.

**Terminal 4 — order executor**

```powershell
python run_executor.py
```

**Terminal 5 — inspect signals**

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic signals --from-beginning
```

To inspect candles instead, replace `signals` with `velas`. A candle is only
published after its Spark window closes. FX market hours, quiet periods, and
window timing affect when data appears. A strategy may correctly emit no
signal for a long time; do not generate trades just to make the console active.

Stop each foreground process with `Ctrl+C`. Stopping the strategy or executor
does not automatically close orders already held by IB. Verify any open paper
orders in TWS/IB Gateway.

### Safety defaults

The executor uses an allowlist (`EXECUTOR_ALLOWED_ALGORITHMS`), a maximum
position count, a fixed order size, and stop-loss/take-profit prices. It also
refuses known live-account ports and requires a paper account. Review
`config/settings.py` and your local `.env` before starting it. These safeguards
do not replace supervision, and they do not make real-money trading safe.

The dashboard can start and stop workers when run in live mode:

```powershell
python run_dashboard.py
```

It listens on localhost by default. Keep it local because its controls can
manage the order executor. Closing the dashboard window does not stop running
workers; use its **Stop workers** control or stop the worker terminals.

For a dashboard preview without IB or Kafka:

```powershell
python run_dashboard.py --demo
```

## Tests

Run tests from the project root with the intended Python environment.

### Offline tests

These do not require IB or Kafka:

```powershell
python -m tests.test_3
python -m tests.test_4
python -m tests.test_5
python -m tests.test_6
python -m tests.test_genetic_optimizer
```

`tests.test_2` exercises Spark locally. It does not need Kafka, but it does need
a working Java installation:

```powershell
python -m tests.test_2
```

### Integration checks

These need their external service running:

```powershell
python -m tests.test_1
python -m tests.test_3_kafka
```

The IB paper-account test can submit a **real paper order** (a parent market
order with take-profit and stop-loss children). Run it only when you intend to
test order submission, IB Gateway/TWS is connected to paper, and you are
watching the account:

```powershell
python -m tests.test_4_ib
```

The script checks its configured paper port and account before sending. It is
not an offline unit test. Avoid running broad test discovery unless you have
reviewed the integration tests and their external side effects.

## Historical backtests and reports

Run the selected EMA strategy on historical one-minute bars:

```powershell
python run_backtest.py --variant v3
```

If `data/historical/EURUSD_M1.csv` is missing, the downloader requests historical
data from IB and saves the result locally. This requires a working IB connection
and data permissions. To request a fresh download when a local CSV exists:

```powershell
python run_backtest.py --variant v3 --force-download
```

Reports are written under a new `reports/backtest_*` directory and include
`metrics.json`, `trades_is.csv`, `trades_oos.csv`, and `equity_curve.png`.
The backtest splits the dataset into in-sample (IS) and out-of-sample (OOS)
periods; a historical split is not a guarantee of future performance.

### Genetic parameter search

Compare V3 against a genetic search over EMA, RSI threshold, stop-loss, and
take-profit parameters:

```powershell
python run_ga_backtest.py --population-size 20 --generations 8 --seed 42
```

This requires a local historical dataset with at least 20,000 candles. The
optimizer selects candidates using walk-forward in-sample folds, then evaluates
the selected candidate on the held-out period. The current held-out data has
been inspected in earlier experiments, so its results are exploratory rather
than independent confirmation. The generated `genetic_comparison_*` directory
contains `metrics.json`, `comparison.csv`, OOS trade files, and an equity
chart. Do not choose a live strategy based solely on this optimization.

In the recorded experiment on 30,146 historical candles, V3 returned
approximately **-$5.50 across 40 OOS trades**, while the genetic candidate
returned approximately **-$113 across 32 OOS trades**. The candidate was not
selected. Because this holdout had been inspected in earlier comparisons, these
numbers are exploratory and are not an independent performance estimate.

## Configuration reference

Settings are read from `.env` when present; defaults are defined in
`config/settings.py`.

| Setting | Default | Description |
| --- | --- | --- |
| `IB_HOST` | `127.0.0.1` | IB Gateway/TWS API host. |
| `IB_PORT` | `4002` | Paper API port. |
| `IB_CLIENT_ID_PRODUCER` | `1` | Client ID used by market-data producer. |
| `IB_CLIENT_ID_EXECUTOR` | `2` | Client ID used by order executor. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address. |
| `TOPIC_TICKS` | `ticks` | Raw market-data topic. |
| `TOPIC_CANDLES` | `velas` | Candle topic; the legacy `TOPIC_VELAS` name remains supported. |
| `TOPIC_SIGNALS` | `signals` | Strategy signal topic. |
| `SYMBOL` | `EURUSD` | Forex instrument. |
| `WINDOW_SECONDS` | `60` | Spark candle-window length. |
| `EMA_FAST`, `EMA_SLOW` | `9`, `21` | Default EMA periods. |
| `USE_RSI_FILTER` | `false` | Enable the RSI strategy filter. |
| `USE_TREND_FILTER` | `false` | Enable the long-term EMA trend filter. |
| `RISK_TRADE_SIZE` | `10000` | Order quantity in base currency units. |
| `RISK_STOP_LOSS_PIPS`, `RISK_TAKE_PROFIT_PIPS` | `10`, `20` | Bracket distances in pips. |
| `RISK_MAX_POSITIONS` | `1` | Maximum open positions per symbol. |
| `EXECUTOR_ALLOWED_ALGORITHMS` | `ema_cross_v1` | Comma-separated strategy IDs accepted by executor. |
| `BACKTEST_DAYS` | `30` | Days requested when downloading historical bars. |
| `DASHBOARD_HOST`, `DASHBOARD_PORT` | `127.0.0.1`, `8050` | Local dashboard address. |

`DASHBOARD_MANAGE_PRODUCER` defaults to `true`, so the dashboard can launch the
IB producer when its Start action is used. Set it to `false` if the producer is
already running in a separate terminal.

## Data contracts and compatibility

Kafka candle messages contain `symbol`, `inicio`, `open`, `high`, `low`,
`close`, and `num_ticks`. Strategy signal messages contain `id_algoritmo`,
`symbol`, `signal`, `precio_referencia`, and `timestamp`. These field names and
the existing topic name `velas` are kept stable for existing producers and
consumers.

## Troubleshooting

### PySpark says `Could not find valid SPARK_HOME`

When using the pip-installed package, use the project's Python environment and
start the job with:

```powershell
python run_candles.py
```

The script uses the current Python executable for both Spark's driver and
worker. The Docker-based Spark service is another supported option.

### `PYTHON_VERSION_MISMATCH`

Spark's Python worker and driver must have the same minor version. Check
`python --version` and interpreter paths, activate the intended Python 3.10
environment, then use `python run_candles.py`. Clear conflicting
`PYSPARK_PYTHON` or `PYSPARK_DRIVER_PYTHON` overrides from the current shell if
they point to another interpreter.

### Kafka is running but a test receives no message

Confirm broker reachability and topic names, then check producer delivery
errors and consumer group offsets. A producer's “published” log alone does not
prove that a consumer in another group has read the message. Run the isolated
round-trip test with Kafka available:

```powershell
python -m tests.test_1
```

### No candle or strategy signal appears

Check that the IB producer, Kafka, and Spark job are all running, and inspect
the `ticks`, `velas`, and `signals` topics independently. A candle appears
after a time window closes. The strategy also requires indicator warm-up and
only emits on a crossover; no signal may be the expected result.

### `winutils.exe` warning on Windows

Spark may warn that Hadoop's `winutils.exe` is missing. For local Spark
transformations this warning may be non-fatal. If Spark stops with an actual
Hadoop-related exception, prefer the containerized Spark service or install a
compatible Hadoop Windows setup for your Spark version.

## Generated and local-only files

`.env`, logs, local market data, reports, Python caches, and Spark checkpoints
are excluded from version control. Preserve reports you need to keep outside
the ignored directories or adjust the ignore policy intentionally; never add
credentials to source control.
