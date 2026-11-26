# FX Trading Algorítmico

Proyecto educativo de trading algorítmico en cuenta **paper (simulada)** de
Interactive Brokers (IBKR) para el par EUR/USD. Integra Kafka, Spark, estrategias
basadas en EMA, control de riesgo, un ejecutor de órdenes, backtesting histórico
y un dashboard local.

El proyecto está diseñado para **aprendizaje y experimentación controlada en
cuenta demo**, este no promete ganancias, y las estrategias implementadas **no han
sido validadas como rentables** y los resultados de backtest y paper trading no
deben interpretarse como recomendación para operar con dinero real.

---

## Índice

1. [Qué hace la aplicación](#qué-hace-la-aplicación)
2. [Componentes principales](#componentes-principales)
3. [Requisitos](#requisitos)
4. [Instalación en Windows](#instalación-en-windows)
5. [Configuración de la cuenta paper de IBKR](#configuración-de-la-cuenta-paper-de-ibkr)
6. [Arrancar Kafka y Spark](#arrancar-kafka-y-spark)
7. [Ejecutar el pipeline en vivo](#ejecutar-el-pipeline-en-vivo)
8. [Defaults de seguridad](#defaults-de-seguridad)
9. [Tests](#tests)
10. [Backtesting histórico](#backtesting-histórico)
11. [Búsqueda genética de parámetros](#búsqueda-genética-de-parámetros)
12. [Resultados obtenidos](#resultados-obtenidos)
13. [Análisis y limitaciones](#análisis-y-limitaciones)
14. [Referencia de configuración](#referencia-de-configuración)
15. [Contratos de datos](#contratos-de-datos)
16. [Troubleshooting](#troubleshooting)
17. [Archivos generados y locales](#archivos-generados-y-locales)

---

## Qué hace la aplicación

El pipeline en vivo tiene cuatro etapas principales:

1. **Productor de datos de mercado**: lee actualizaciones bid/ask desde IB y
   publica ticks al tópico `ticks` de Kafka.
2. **Procesador de velas**: usa Spark para agrupar ticks en ventanas de tiempo
   y publica velas OHLC al tópico `velas` de Kafka.
3. **Estrategia**: consume velas, calcula señales con EMA (y filtros opcionales
   de RSI y tendencia), y publica señales al tópico `signals`.
4. **Ejecutor de órdenes**: valida cada señal, aplica límites de posición, y
   puede enviar una orden bracket (market + take-profit + stop-loss) a la
   cuenta paper de IB configurada.

El dashboard muestra estado en vivo, datos de mercado, señales, trades e
información de la cuenta pero su modo `--demo` usa datos sintéticos y no se conecta
a IB ni a Kafka.

---

## Componentes principales

| Componente | Propósito |
| --- | --- |
| `run_producer.py` | Arranca el productor de ticks IB → Kafka. |
| `run_candles.py` | Arranca el job de Spark que convierte ticks en velas. |
| `run_algorithms.py` | Ejecuta una variante de estrategia EMA y publica señales. |
| `run_executor.py` | Consume señales, aplica control de riesgo y envía órdenes. |
| `run_dashboard.py` | Arranca el dashboard local; `--demo` usa datos sintéticos. |
| `run_backtest.py` | Ejecuta un backtest histórico y guarda métricas, trades y gráfico. |
| `run_ga_backtest.py` | Compara V3 contra una búsqueda genética con walk-forward. |
| `src/algorithms/` | Implementaciones de estrategias, indicadores y variantes. |
| `src/spark/` | Transformación tick→vela y job de streaming. |
| `src/executor/` | Validación de señales, control de riesgo y órdenes bracket a IB. |
| `src/runtime/` | Estado del dashboard, gestión de workers y ledger de trades. |
| `tests/` | Tests unitarios offline y tests de integración opt-in. |
| `data/historical/` | CSVs locales con velas históricas. |
| `reports/` | Reportes generados de backtest y optimización. |

**Compatibilidad**: algunos nombres de módulos y campos en español se conservan
como interfaces por compatibilidad (tópico `velas`, campos `inicio`, `num_ticks`,
`id_algoritmo`, `precio_referencia`), para los comandos y módulos nuevos usan nombres
en inglés.\
Los antiguos `run_velas.py`, `run_consumer_velas.py`,
`base_algoritmo.py`, `indicadores.py` y `ticks_to_velas.py` siguen existiendo
como wrappers de compatibilidad.

---

## Requisitos

- **Windows 10 u 11**.
- **Python 3.10** (versión validada; el driver y worker de Spark deben usar
  exactamente la misma versión menor).
- **Java 17 o superior** para PySpark local.
- **Docker Desktop con Docker Compose** para Kafka y el job Spark contenedorizado.
- **IB Gateway o Trader Workstation (TWS)**, configurados para **cuenta paper**,
  solo si vas a usar datos en vivo o los tests de integración con IB.

Los warnings sobre `winutils.exe` en Windows son usuales con Spark y suelen
ser no fatales para transformaciones locales, en los errores de
`PYTHON_VERSION_MISMATCH` **sí son graves**: driver y worker deben coincidir.

---

## Instalación en Windows

Abre PowerShell en la raíz del proyecto.

### Instalación de herramientas base (si faltan)

```powershell
winget install --exact --id Python.Python.3.10
winget install --exact --id EclipseAdoptium.Temurin.17.JDK
winget install --exact --id Docker.DockerDesktop
```

Reinicia PowerShell y abre Docker Desktop.

### Opción A: Conda

```powershell
conda create -n IB python=3.10 -y
conda activate IB
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

### Opción B: Virtualenv

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Si PowerShell bloquea la activación, ejecuta explícitamente
`.venv\Scripts\python.exe <script.py>`.

Edita `.env` solo con tus valores locales. **Nunca lo subas a Git ni compartas
su contenido**. `.env.example` contiene placeholders seguros y no tiene
credenciales.

### Verificar el entorno Python

```powershell
python --version
python -c "import sys, pyspark; print(sys.executable); print(pyspark.__version__)"
```

Usa siempre `python -m ...` del entorno activado, no un `pyspark` ajeno en el
`PATH`.

---

## Configuración de la cuenta paper de IBKR

Este paso es **independiente del proyecto** y debe hacerse una sola vez.

### 1. Crear la cuenta paper

1. Regístrate en IBKR y accede al [Client Portal](https://www.interactivebrokers.com/portal).
2. En la página principal, busca **"Try Our Platforms with a Simulated Account"**
   y pulsa **"Create Paper Account"**.
3. Alternativamente: menú lateral → **Settings** → **Paper Trading Account**.
4. Define un usuario y contraseña **exclusivos** para la cuenta simulada.
5. IBKR notifica por correo cuando la cuenta está lista (usualmente < 24 h).
   El número de cuenta demo empieza con `DU`.

### 2. Instalar IB Gateway (o TWS)

- **IB Gateway** (recomendado para automatización): ligero, sin interfaz de
  trading completa.
  Descarga: https://www.interactivebrokers.com/en/index.php?f=16457
  Puerto **paper**: **`4002`**.
- **TWS** (alternativa): plataforma completa.
  Descarga: https://www.interactivebrokers.com/en/index.php?f=14099
  Puerto **paper**: **`7497`**.

### 3. Habilitar la API

1. Abre IB Gateway o TWS e inicia sesión **con las credenciales de la cuenta
   paper** (no las de la cuenta real).
2. Ve a `File → Global Configuration → API → Settings`.
3. Marca:
   - ✅ **Enable ActiveX and Socket Clients**.
   - ⬜ **Read-Only API** (desmarcado; si lo dejas marcado, no podrás enviar
     órdenes).
   - ✅ **Allow connections from localhost only**.
4. Verifica el **Socket Port**:
   - IB Gateway (paper) → `4002`.
   - TWS (paper) → `7497`.
5. Pulsa **OK** y **reinicia** IB Gateway/TWS.

### 4. Verificar la conexión desde Python

```powershell
python -c "from ib_insync import IB; ib = IB(); ib.connect('127.0.0.1', 4002, clientId=99); print('Cuentas:', ib.managedAccounts()); ib.disconnect()"
```

Debe imprimir algo como `Cuentas: ['DUxxxxxxxx']` pero si falla, revisa el paso 3.

### 5. Configurar `.env` con tu cuenta

```ini
IB_HOST=127.0.0.1
IB_PORT=4002
IB_CLIENT_ID_PRODUCER=1
IB_CLIENT_ID_EXECUTOR=2
IB_ACCOUNT=DUxxxxxxxx
```

### 6. Consideraciones importantes

- **clientId único por proceso**: productor usa `1`, ejecutor usa `2`, si
  reinicias espera ~30-60 s antes de reconectar (IB bloquea el clientId por un
  rato tras desconexión abrupta).
- **Permisos de Forex**: en TWS, verifica `File → Account → Trading Permissions`.
  Las cuentas paper pueden requerir habilitación manual de Forex.
- **Horario del mercado**: Forex abre domingo 22:00 UTC y cierra viernes
  22:00 UTC. Fuera de ese horario no llegarán ticks.
- **Tamaño mínimo de orden**: IBKR puede rechazar órdenes < 20,000 unidades
  en paper en algunos instrumentos. Si ocurre, sube `RISK_TRADE_SIZE` en `.env`.

---

## Arrancar Kafka y Spark

Con Docker Desktop abierto, desde la raíz del proyecto:

```powershell
docker compose up -d kafka
docker compose ps
docker compose up --build -d spark-velas
docker compose logs -f spark-velas
```

Espera hasta que el log muestre `Streaming started`. Detén el job con `Ctrl+C`
en el visor de logs y luego:

```powershell
docker compose down
```

### Alternativa: Spark nativo (sin Docker)

Con Java 17 disponible y el entorno Python 3.10 activado:

```powershell
python run_candles.py
```

**No ejecutes ambos procesadores de velas simultáneamente** contra el mismo
checkpoint (`checkpoints/velas`), por que El checkpoint conserva offsets de stream
entre reinicios.

---

## Ejecutar el pipeline en vivo

Antes de arrancar, **confirma** que IB Gateway/TWS está conectado a **paper**
y que la API es alcanzable ya que cada proceso va en **una terminal independiente**,
con el entorno Python activado.

### Terminal 1 — Datos de mercado (IB → Kafka)

```powershell
python run_producer.py
```

### Terminal 2 — Ticks a velas (Spark)

Usa el servicio Docker (recomendado):

```powershell
docker compose up -d spark-velas
```

O bien, si prefieres Spark nativo:

```powershell
python run_candles.py
```

### Terminal 3 — Estrategia

```powershell
python run_algorithms.py --variant v3
```

Variantes disponibles: `v1`, `v2`, `v3`, `v4`. Sus filtros están definidos en
`src/algorithms/variants.py`.

### Terminal 4 — Ejecutor de órdenes

```powershell
python run_executor.py
```

### Terminal 5 — Inspeccionar señales

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic signals --from-beginning
```

Para inspeccionar velas, cambia `signals` por `velas`. Una vela solo se publica
**después de que su ventana de Spark se cierra** (agrega ~30 s de watermark).

**Detener cada proceso**: `Ctrl+C` en su terminal. Detener la estrategia o el
ejecutor **no cierra órdenes abiertas en IB**: verifícalas en TWS/IB Gateway.

---

## Defaults de seguridad

El ejecutor aplica:

- **Allowlist** de algoritmos (`EXECUTOR_ALLOWED_ALGORITHMS`): solo acepta
  señales cuyos IDs estén en la lista (default: `ema_cross_v1`).
- **Límite de posiciones** por símbolo (`RISK_MAX_POSITIONS`, default `1`).
- **Tamaño fijo** de orden (`RISK_TRADE_SIZE`, default `10,000` unidades).
- **Stop-loss / take-profit** fijos (`RISK_STOP_LOSS_PIPS=10`,
  `RISK_TAKE_PROFIT_PIPS=20`).
- **Rechazo de puertos live conocidos** y **requerimiento de cuenta paper**.

**Estos controles no sustituyen supervisión humana.** Revisa
`config/settings.py` y tu `.env` antes de arrancar.

### Dashboard en modo live

```powershell
python run_dashboard.py
```

Escucha en localhost por defecto (`127.0.0.1:8050`). **No lo expongas a la red
pública**: sus controles pueden iniciar/detener el ejecutor, al cerrar la ventana
del navegador **no detiene los workers**; usa el botón **Stop workers** o
detén las terminales.

### Dashboard en modo demo (sin IB/Kafka)

```powershell
python run_dashboard.py --demo
```

---

## Tests

Desde la raíz del proyecto con el entorno correcto activado.

### Tests offline (no requieren IB ni Kafka)

```powershell
python -m tests.test_3
python -m tests.test_4
python -m tests.test_5
python -m tests.test_6
python -m tests.test_genetic_optimizer
```

`tests.test_2` ejercita Spark localmente, no requiere Kafka, pero sí Java:

```powershell
python -m tests.test_2
```

### Tests de integración (requieren servicios externos)

```powershell
python -m tests.test_1
python -m tests.test_3_kafka
```

### Test de envío de órdenes a IB paper

⚠️ **Envía una orden real a la cuenta paper**. Ejecútalo solo si estás
supervisando la cuenta y TWS/IB Gateway está conectado a paper:

```powershell
python -m tests.test_4_ib
```

El script verifica que el puerto y la cuenta sean paper antes de enviar, pero **no
es un test offline**, solo evita ejecuciones automáticas de todo `tests/` sin revisar
los side-effects.

---

## Backtesting histórico

Ejecuta una variante sobre datos históricos M1:

```powershell
python run_backtest.py --variant v3
```

Si `data/historical/EURUSD_M1.csv` no existe, el downloader solicita datos a IB
y los guarda localmente, este requiere conexión IB activa y permisos de datos para
forzar una descarga nueva:

```powershell
python run_backtest.py --variant v3 --force-download
```

Los reportes se escriben en `reports/backtest_<ts>_<variant>/` e incluyen:

- `metrics.json`: métricas completas (IS y OOS).
- `trades_is.csv` / `trades_oos.csv`: cada trade con entrada, salida, SL, TP,
  pnl_pips, pnl_usd, exit_reason.
- `equity_curve.png`: gráfico IS vs OOS.

### Reglas metodológicas del backtest

- Split temporal **70% in-sample (IS) / 30% out-of-sample (OOS)**.
- **Sin look-ahead**: la señal se genera al cierre de la vela `i`; la entrada
  se ejecuta en la **apertura** de la vela `i+1`.
- **Spread de 2 pips** y **slippage de 0.5 pips** aplicados **en contra** en
  cada entrada y salida.
- Si SL y TP se tocan en la misma vela, **se asume SL** (conservador).
- Capital inicial: **$10,000 USD** por corrida.
- Tamaño: **10,000 unidades** de EUR (micro-lote).

### Comparativa de las 4 variantes en una sola pasada

```powershell
python run_variants.py
```

Carga el dataset una vez y corre V1–V4 sobre el mismo IS/OOS. Genera
`reports/variants_comparison_<ts>/comparison.{csv,json}`.

---

## Búsqueda genética de parámetros

Compara V3 contra una búsqueda genética sobre parámetros de EMA, RSI, SL y TP:

```powershell
python run_ga_backtest.py --population-size 20 --generations 8 --seed 42
```

Requiere dataset local ≥ 20,000 velas M1, con el optimizador:

- Selecciona candidatos con **walk-forward** sobre pliegues in-sample.
- Evalúa el candidato seleccionado en el periodo **held-out**.
- Calcula **Deflated Sharpe Ratio (DSR)** para penalizar el número de pruebas.
- Genera `reports/genetic_comparison_<ts>/` con `metrics.json`,
  `comparison.csv`, trades OOS y gráfico de equity.

**Limitación metodológica importante**: el held-out actual ya fue inspeccionado
en experimentos anteriores para los resultados son **exploratorios** y **no
constituyen confirmación independiente**.\
No selecciones una estrategia para
live basándote solo en esta optimización.

---

## Resultados obtenidos

Todos los resultados a continuación provienen de:
- Dataset: **EURUSD M1**, ~**30,146 velas** descargadas vía IB.
- Split: **~21,102 velas IS** / **~9,044 velas OOS**.
- Costos aplicados: **spread 2 pips + slippage 0.5 pips**.
- Capital inicial: **$10,000 USD**.

### Comparativa de variantes (split 70/30)

| Variante | Algoritmo | IS Trades | IS PF | IS WR | IS Sharpe | IS MaxDD | IS PnL | OOS Trades | OOS PF | OOS WR | OOS Sharpe | OOS MaxDD | OOS PnL |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **V1** | `ema_cross_v1` | 101 | 0.7322 | 26.7% | -6.3631 | 3.1% | -$197.50 | 44 | 0.7297 | 27.3% | -5.2624 | 1.2% | -$86.50 |
| **V2** | `ema_cross_rsi` | 100 | 0.7423 | 27.0% | -4.9103 | 3.4% | -$187.50 | 44 | 0.7297 | 27.3% | -5.2624 | 1.2% | -$86.50 |
| **V3** | `ema_cross_trend` | 98 | 0.8239 | 29.6% | -4.3174 | 1.7% | -$121.50 | **39** | **1.1405** | **35.9%** | **+2.3291** | **0.7%** | **+$34.50** |
| **V4** | `ema_cross_rsi_trend` | 96 | 0.8066 | 29.2% | -4.5409 | 1.8% | -$131.50 | 39 | 1.1405 | 35.9% | +2.3291 | 0.7% | +$34.50 |

**Observaciones**:

- **V1 y V2** pierden consistentemente en IS y OOS, el filtro RSI (V2) reduce
  pérdida IS pero no cambia nada en OOS.
- **V3 (filtro de tendencia EMA 50)** es la única variante con OOS positivo:
  **PF 1.14, WR 35.9%, Sharpe +2.33, PnL +$34.50** sobre 39 trades.
- **V4 (RSI + tendencia)** es idéntica a V3 en OOS (el filtro RSI no agrega
  información en presencia del filtro de tendencia).

### Búsqueda genética (walk-forward + held-out)

| Estrategia | Segmento | Trades | WR | PF | Expectancy | Sharpe | MaxDD | PnL | DSR |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **V3_baseline** | IS_walk_forward | 49 | 34.69% | 0.9578 | -$0.28 | -0.8111 | 0.85% | -$13.50 | 0.443 |
| **V3_baseline** | OOS | 40 | 32.50% | 0.9793 | -$0.14 | -0.3467 | 0.65% | -$5.50 | 0.477 |
| **GA_optimized** | IS_walk_forward | 43 | 79.07% | **1.5247** | +$2.72 | **+8.298** | 0.30% | +$117.00 | **0.0** |
| **GA_optimized** | OOS | 32 | 65.62% | 0.6502 | -$3.53 | **-7.8433** | 1.52% | **-$113.00** | **0.0** |

**El GA encontró parámetros `fast=5, slow=14, sl_pips=30, tp_pips=10` que
dan Sharpe +8.30 en entrenamiento y -7.84 en validación, con DSR=0.0.**
Esto es **el ejemplo de libro de overfitting**: el optimizador memoriza el
pasado y fracasa en el futuro.

**Nota metodológica**: los números de `V3_baseline` en esta tabla difieren de
la sección anterior porque `run_ga_backtest.py` usa **walk-forward con
múltiples pliegues**, mientras que `run_variants.py` usa **un único split
70/30**. Ambos son honestos; miden cosas distintas.

### Veredicto del proyecto

- **No hay evidencia estadísticamente significativa de edge** en ninguna
  variante con los datos actuales.
- **V3 es la única con OOS positivo**, pero el tamaño muestral (39 trades, ~9
  días) es **insuficiente** para confirmar edge real.
- **El GA fue rechazado correctamente** gracias al DSR (0.0). Esto demuestra
  rigor metodológico: el sistema sabe **no adoptar** una estrategia
  sobreajustada.

---

## Análisis y limitaciones

### Por qué el Sharpe OOS de V3 (+2.33) no es concluyente

El error estándar aproximado de un Sharpe anualizado con N años de datos es:

```
SE(Sharpe) ≈ √((1 + Sharpe²/2) / N_años)
```

Para V3 OOS: N = 9 días ≈ 0.036 años, Sharpe = 2.33:

```
SE ≈ √((1 + 2.33²/2) / 0.036) ≈ 10.15
IC 95% = 2.33 ± 2 × 10.15 = [-17.97, +22.63]
```

El intervalo de confianza al 95% incluye valores **negativos y positivos
extremos**. En términos prácticos: *con 9 días de datos, no sabemos si V3 gana
o pierde.* Para reducir el SE a ~0.5 (mínimo razonable para inferencia), se
necesitarían **~4 años de velas M1**.

### Por qué el GA falló como se predijo

- Optimizó sobre 30 días de datos con ~20-40 trades efectivos por pliegue.
- Encontró parámetros con SL=30 pips y TP=10 pips: ratio riesgo/beneficio 1:0.33,
  matemáticamente suicida, pero con WR=79% en backtest.
- El DSR = 0.0 fue la **señal de rechazo automática** (penalización por haber
  probado muchos candidatos).

### Qué se necesitaría para continuar honestamente

1. **Más historia**: mínimo 1-2 años de M1 (Dukascopy o HistData gratuito) para
   tener suficiente potencia estadística.
2. **Test cruzado en GBPUSD**: si V3 gana también en GBPUSD OOS con Sharpe
   positivo y PF > 1.1, gana credibilidad. Si pierde, confirma regime luck.
3. **Análisis por régimen**: separar resultados por tendencia vs rango para
   entender **cuándo** funciona V3 y cuándo no.
4. **Costos más realistas**: añadir comisión IB, swap overnight, y modelar
   gap de fin de semana.

Todo lo anterior está **fuera del alcance de este proyecto** y se propone como
trabajo futuro.

---

## Referencia de configuración

Settings leídos desde `.env` (con defaults en `config/settings.py`).

| Setting | Default | Descripción |
| --- | --- | --- |
| `IB_HOST` | `127.0.0.1` | Host de IB Gateway/TWS. |
| `IB_PORT` | `4002` | Puerto paper. |
| `IB_CLIENT_ID_PRODUCER` | `1` | Client ID del productor de datos. |
| `IB_CLIENT_ID_EXECUTOR` | `2` | Client ID del ejecutor de órdenes. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Dirección del broker Kafka. |
| `TOPIC_TICKS` | `ticks` | Tópico de datos crudos. |
| `TOPIC_CANDLES` | `velas` | Tópico de velas (legacy `TOPIC_VELAS` soportado). |
| `TOPIC_SIGNALS` | `signals` | Tópico de señales. |
| `SYMBOL` | `EURUSD` | Instrumento Forex. |
| `WINDOW_SECONDS` | `60` | Longitud de ventana en Spark. |
| `EMA_FAST`, `EMA_SLOW` | `9`, `21` | Períodos EMA por defecto. |
| `USE_RSI_FILTER` | `false` | Habilita filtro RSI. |
| `USE_TREND_FILTER` | `false` | Habilita filtro de tendencia EMA larga. |
| `RSI_PERIOD`, `RSI_OVERBOUGHT`, `RSI_OVERSOLD` | `14`, `70`, `30` | Parámetros del RSI. |
| `EMA_TREND_PERIOD` | `50` | Período de la EMA de tendencia. |
| `RISK_TRADE_SIZE` | `10000` | Cantidad en unidades base. |
| `RISK_STOP_LOSS_PIPS`, `RISK_TAKE_PROFIT_PIPS` | `10`, `20` | Distancias bracket en pips. |
| `RISK_MAX_POSITIONS` | `1` | Máximo de posiciones por símbolo. |
| `EXECUTOR_ALLOWED_ALGORITHMS` | `ema_cross_v1` | IDs aceptados por el ejecutor (CSV). |
| `BACKTEST_DAYS` | `30` | Días solicitados a IB al descargar. |
| `BACKTEST_SPREAD_PIPS`, `BACKTEST_SLIPPAGE_PIPS` | `2.0`, `0.5` | Costos del backtest. |
| `BACKTEST_SL_PIPS`, `BACKTEST_TP_PIPS` | `10`, `20` | SL/TP del backtest. |
| `DASHBOARD_HOST`, `DASHBOARD_PORT` | `127.0.0.1`, `8050` | Dirección del dashboard. |
| `DASHBOARD_MANAGE_PRODUCER` | `true` | Permite al dashboard lanzar el productor IB. |

---

## Contratos de datos

### Velas (tópico `velas`)

```json
{
  "symbol": "EURUSD",
  "inicio": "2026-11-25T20:51:00.000Z",
  "open": 1.097665,
  "high": 1.097685,
  "low": 1.097630,
  "close": 1.097685,
  "num_ticks": 430
}
```

### Señales (tópico `signals`)

```json
{
  "id_algoritmo": "ema_cross_trend",
  "symbol": "EURUSD",
  "signal": "BUY",
  "precio_referencia": 1.097685,
  "timestamp": "2026-11-25T20:52:14.123456Z"
}
```

### Ticks (tópico `ticks`)

```json
{
  "symbol": "EURUSD",
  "bid": 1.097685,
  "ask": 1.097695,
  "timestamp": "2026-11-25T20:52:14.043526Z"
}
```

Estos nombres de campo y el tópico `velas` se mantienen estables por
compatibilidad con consumidores existentes.

---

## Troubleshooting

### PySpark: `Could not find valid SPARK_HOME`

Usa el entorno Python del proyecto y arranca el job con:

```powershell
python run_candles.py
```

El script usa el ejecutable de Python actual para driver y worker. La
alternativa Docker también es válida.

### `PYTHON_VERSION_MISMATCH`

Driver y worker de Spark deben usar la **misma versión menor de Python**.
Verifica `python --version`, activa el entorno 3.10 correcto y usa
`python run_candles.py`, limpia overrides conflictivos de `PYSPARK_PYTHON` y
`PYSPARK_DRIVER_PYTHON` si apuntan a otro intérprete.

### Kafka corriendo pero un test no recibe mensaje

Verifica alcanzabilidad del broker y nombres de tópicos, revisa errores de
entrega del productor y offsets del consumer group. Un log de "publicado" no
garantiza que otro consumer group haya leído el mensaje.\
Test aislado:

```powershell
python -m tests.test_1
```

### No aparece ninguna vela o señal

Verifica que **producer IB, Kafka y Spark job estén todos corriendo**, inspecciona
`ticks`, `velas` y `signals` de forma independiente, que una vela aparecezca **después**
de que la ventana cierra.\
La estrategia requiere warm-up de indicadores y solo
emite en un cruce; **no emitir ninguna señal es un resultado esperado** durante
periodos largos.

### Warning `winutils.exe` en Windows

Spark puede advertir que falta `winutils.exe` para transformaciones locales es
usualmente **no fatal**, si Spark falla con excepción Hadoop real, prefiere el
servicio Spark contenedorizado o instala un Hadoop Windows compatible con tu
versión de Spark.

### IB rechaza la conexión con `clientId already in use`

El `clientId` quedó bloqueado por una desconexión previa abrupta espera
30-60 s y reintenta, el código ya reintenta con `clientId` desplazado
(`1, 11, 21...` para productor; `2, 12, 22...` para ejecutor).

### La orden queda en `Submitted` sin llenarse

Normal en demo los market orders se llenan en 1-2 s casi siempre si tarda,
verifica que el contrato sea `IDEALPRO` y que el mercado Forex esté abierto
(domingo 22:00 UTC a viernes 22:00 UTC).

---

## Archivos generados y locales

Estos archivos están excluidos de control de versiones (`.gitignore`):

- `.env` (contiene credenciales).
- `logs/` (logs runtime).
- `data/historical/` (CSVs de velas).
- `reports/` (backtests y comparativas).
- `checkpoints/` (checkpoints de Spark Streaming).
- `__pycache__/`, `.venv/`, `venv/`.

**Preserva manualmente** cualquier reporte que necesites conservar fuera de
`reports/`. **Nunca** agregues credenciales al repositorio.

---

## Estado del proyecto

- ✅ Pipeline completo funcional (ticks → velas → señales → órdenes).
- ✅ 4 variantes de estrategia implementadas y comparadas.
- ✅ Backtesting honesto con spread/slippage, split temporal y sin look-ahead.
- ✅ Búsqueda genética con walk-forward y Deflated Sharpe Ratio.
- ✅ Dashboard local con modo live y demo.
- ✅ Tests unitarios y de integración.
- ⚠️ **Ninguna estrategia ha demostrado edge estadísticamente significativo**
  con los datos actuales (30 días M1). El proyecto es un **framework listo
  para recibir estrategias futuras**, no un sistema rentable hoy.

---

## Licencia y uso

Proyecto educativo. **No usar con dinero real** sin validación independiente
extensa, más historia, y supervisión profesional, los autores no se hacen
responsables de pérdidas derivadas de su uso.
```

---

