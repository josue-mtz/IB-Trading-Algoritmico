# FX Trading Algorítmico

Sistema de **trading algorítmico** que opera el par **EUR/USD** en una **cuenta demo
(paper) de Interactive Brokers**. Es decir: un programa que mira el precio del
euro contra el dólar, decide solo cuándo comprar o vender, y manda las órdenes a
una cuenta **con dinero ficticio**.

> **Aviso importante.** Es un proyecto educativo. Ninguna estrategia ha demostrado
> ser rentable (más abajo se explica por qué). **No lo uses con dinero real.**

---

## Índice

1. [¿Cómo funciona? (explicación sencilla)](#cómo-funciona-explicación-sencilla)
2. [¿Qué pidió el proyecto y dónde está cada cosa?](#qué-pidió-el-proyecto-y-dónde-está-cada-cosa)
3. [Qué necesitas instalar](#qué-necesitas-instalar)
4. [Instalación paso a paso (Windows)](#instalación-paso-a-paso-windows)
5. [Preparar la cuenta demo de Interactive Brokers](#preparar-la-cuenta-demo-de-interactive-brokers)
6. [Cómo usarlo](#cómo-usarlo)
7. [El dashboard (panel visual)](#el-dashboard-panel-visual)
8. [Seguridad: qué protege al sistema](#seguridad-qué-protege-al-sistema)
9. [Backtesting: probar con datos del pasado](#backtesting-probar-con-datos-del-pasado)
10. [Algoritmo genético](#algoritmo-genético)
11. [Resultados](#resultados)
12. [Limitaciones (lo que hay que decir con honestidad)](#limitaciones-lo-que-hay-que-decir-con-honestidad)
13. [Estructura del proyecto](#estructura-del-proyecto)
14. [Formato de los mensajes](#formato-de-los-mensajes)
15. [Configuración (archivo `.env`)](#configuración-archivo-env)
16. [Pruebas automáticas](#pruebas-automáticas)
17. [Preguntas frecuentes y solución de problemas](#preguntas-frecuentes-y-solución-de-problemas)
18. [Glosario](#glosario)

---

## ¿Cómo funciona? (explicación sencilla)

Imagina una fábrica con cinco estaciones. Cada una hace una sola cosa y le pasa
el resultado a la siguiente por medio de **Kafka**, que funciona como un servicio
de mensajería con "buzones" (llamados *tópicos*).

```
 Interactive Brokers        Kafka            Spark             Kafka
 (precios en vivo)   ──►  tópico "ticks" ──► (arma velas) ──► tópico "velas"
                                                                    │
                                                                    ▼
 Cuenta demo de IB  ◄──  Ejecutor de   ◄── tópico "signals" ◄── Algoritmo
 (compra / vende)        órdenes + riesgo      (BUY / SELL)      (cruce de EMAs)
```

1. **Productor** (`run_producer.py`): se conecta a Interactive Brokers y publica
   cada cambio de precio (*tick*) en el buzón `ticks`.
2. **Spark** (`run_candles.py`): junta los ticks de cada minuto en una **vela**
   (precio de apertura, máximo, mínimo y cierre) y la publica en el buzón `velas`.
3. **Algoritmo** (`run_algorithms.py`): lee las velas, calcula dos promedios
   móviles (EMAs) y cuando se cruzan publica una señal **BUY** (comprar) o
   **SELL** (vender) en el buzón `signals`.
4. **Ejecutor** (`run_executor.py`): lee las señales, revisa las reglas de riesgo
   y, si todo está bien, manda la orden a la cuenta demo con su protección
   (stop-loss y take-profit).
5. **Dashboard** (`run_dashboard.py`): una página web local para ver cómo va todo
   y para **iniciar o pausar** el sistema con un botón.

### La estrategia en una frase

Se calculan dos promedios del precio: uno **rápido** (últimos 9 minutos) y uno
**lento** (últimos 21). Cuando el rápido **cruza hacia arriba** al lento, el precio
está tomando fuerza y se **compra**; cuando cruza **hacia abajo**, se **vende**.
Hay cuatro variantes (v1 a v4) que agregan filtros para evitar señales falsas
(ver [Resultados](#resultados)).

---

## ¿Qué pidió el proyecto y dónde está cada cosa?

| Requisito del proyecto | Dónde está |
| --- | --- |
| Usar una cuenta **demo** de Interactive Brokers con **Forex** (EURUSD) | `config/settings.py`, `src/producer/`, `src/executor/` |
| IB envía eventos al tópico `ticks` | `src/producer/ib_tick_producer.py` |
| Spark Structured Streaming convierte ticks en velas de 1 minuto y las publica en `velas` | `src/spark/ticks_to_candles.py` |
| Cada algoritmo es un proceso que consume velas y publica **BUY/SELL** en `signals` | `src/algorithms/` |
| Un ejecutor consume `signals`, aplica control de riesgo y envía órdenes a IB demo | `src/executor/` |
| Formato unificado de mensajes (para que otros equipos puedan conectar sus algoritmos) | [Formato de los mensajes](#formato-de-los-mensajes) |
| *Deseable:* backtesting | `run_backtest.py`, `run_variants.py`, `src/backtest/` |
| *Opcional:* algoritmo genético | `run_ga_backtest.py`, `src/backtest/genetic_optimizer.py` |
| Interfaz visual con historial, métricas y botón de iniciar/pausar | `run_dashboard.py`, `dashboard/` |

> **Para que otro equipo use su algoritmo en esta plataforma:** basta con que
> publique mensajes en `signals` con el formato de abajo, y que su `id_algoritmo`
> esté en `EXECUTOR_ALLOWED_ALGORITHMS` (archivo `.env`). Por seguridad, el
> ejecutor rechaza algoritmos que no estén en esa lista.

---

## Qué necesitas instalar

- **Windows 10 u 11**.
- **Python 3.10** (Spark exige que su "driver" y su "worker" usen la misma versión).
- **Java 17 o superior** (lo necesita Spark si lo corres sin Docker).
- **Docker Desktop** (para Kafka y, opcionalmente, Spark).
- **IB Gateway** o **Trader Workstation (TWS)** de Interactive Brokers, con sesión
  en la **cuenta demo** (solo para datos y órdenes en vivo).

## Instalación paso a paso (Windows)

Abre **PowerShell** dentro de la carpeta del proyecto.

**1. Instalar las herramientas** (omite las que ya tengas):

```powershell
winget install --exact --id Python.Python.3.10
winget install --exact --id EclipseAdoptium.Temurin.17.JDK
winget install --exact --id Docker.DockerDesktop
```

Cierra y vuelve a abrir PowerShell, y abre Docker Desktop.

**2. Crear el entorno de Python e instalar las librerías** (elige una opción):

```powershell
# Opción A: Conda
conda create -n IB python=3.10 -y
conda activate IB
python -m pip install -r requirements.txt

# Opción B: entorno virtual
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

**3. Crear tu archivo de configuración:**

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Abre `.env` con el Bloc de notas y revisa los valores (explicados en
[Configuración](#configuración-archivo-env)). **Nunca subas `.env` a GitHub.**

---

## Preparar la cuenta demo de Interactive Brokers

Esto se hace **una sola vez** y es independiente del código.

1. **Crear la cuenta demo:** entra al [Client Portal](https://www.interactivebrokers.com/portal)
   → *Settings* → *Paper Trading Account*. El número de cuenta demo **empieza con `DU`**.
2. **Instalar IB Gateway** (recomendado) o TWS. Puertos de la cuenta demo:
   **4002** (Gateway) o **7497** (TWS).
3. **Activar la API:** abre IB Gateway con tu usuario **demo** y entra a
   `File → Global Configuration → API → Settings`:
   - ✅ Marca **Enable ActiveX and Socket Clients**.
   - ⬜ **Desmarca Read-Only API** (si está marcada, no se podrán enviar órdenes).
   - ✅ Marca **Allow connections from localhost only**.
   - Confirma que el **Socket Port** sea 4002 (o 7497 en TWS), y reinicia IB Gateway.
4. **Probar la conexión:**

   ```powershell
   python -c "from ib_insync import IB; ib = IB(); ib.connect('127.0.0.1', 4002, clientId=99); print('Cuentas:', ib.managedAccounts()); ib.disconnect()"
   ```

   Debe mostrar algo como `Cuentas: ['DUxxxxxxx']`.

**Detalles a tener en cuenta**

- **Horario:** el mercado Forex abre el domingo ~22:00 UTC y cierra el viernes
  ~22:00 UTC. Fuera de ese horario **no llegan precios**, y es normal que no haya señales.
- **Permisos de Forex:** en algunas cuentas demo hay que habilitarlos a mano.
- **`clientId`:** cada proceso usa uno distinto (productor 1, ejecutor 2). Si IB dice que
  está ocupado, espera 30–60 segundos; el código también reintenta con otro número.

---

## Cómo usarlo

### Opción fácil: todo desde el dashboard

1. Abre **Docker Desktop** y levanta Kafka y Spark:

   ```powershell
   docker compose up -d kafka
   docker compose up --build -d spark-velas
   ```

2. Abre **IB Gateway** con la cuenta demo.
3. Abre el dashboard y pulsa **▶ Iniciar**:

   ```powershell
   python run_dashboard.py
   ```

El botón arranca por ti el productor, el algoritmo y el ejecutor.

### Opción manual: una terminal por proceso

Con el entorno de Python activado, abre una terminal para cada uno:

| Terminal | Comando | Qué hace |
| --- | --- | --- |
| 1 | `python run_producer.py` | IB → Kafka (ticks) |
| 2 | `docker compose up -d spark-velas` *(o `python run_candles.py`)* | ticks → velas |
| 3 | `python run_algorithms.py --variant v3` | velas → señales |
| 4 | `python run_executor.py` | señales → órdenes |

> **No corras dos procesadores de velas a la vez** (Docker y `run_candles.py`): comparten
> el mismo *checkpoint* y se estorban.

Para ver los mensajes que pasan por Kafka:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic signals --from-beginning
```

Cambia `signals` por `velas` o `ticks` para ver los otros buzones. Una vela
aparece **después** de que termina su minuto (más ~30 segundos de espera de Spark).

**Para detener** un proceso usa `Ctrl+C` en su terminal. Detener el algoritmo o el
ejecutor **no cierra las posiciones abiertas** en IB: se cierran solas al llegar a su
stop-loss o take-profit, o puedes cerrarlas a mano en IB Gateway.

---

## El dashboard (panel visual)

```powershell
python run_dashboard.py          # datos reales
python run_dashboard.py --demo   # datos de ejemplo (no necesita IB ni Kafka)
```

Se abre en `http://127.0.0.1:8050`. Muestra:

- **Botón Iniciar / Pausar** y el estado del sistema (OPERANDO, PAUSADO, DETENIDO).
  - **Iniciar** arranca los procesos que falten y activa el trading.
  - **Pausar** deja de abrir operaciones nuevas, pero los procesos siguen activos
    (así no hay que esperar de nuevo el calentamiento de las EMAs). Las posiciones
    abiertas siguen protegidas con su stop-loss y take-profit en IB.
  - **Apagar procesos** (abajo a la derecha) detiene productor, algoritmo y ejecutor.
- **Hoy:** ganancia o pérdida del día, operaciones, porcentaje de aciertos y la posición abierta.
- **Acumulado desde que corre:** ganancia total, promedio por día, *profit factor*,
  máxima caída (*drawdown*) y tiempo que lleva activo.
- **Gráficas:** ganancia acumulada, ganancia/pérdida por día y el precio con las EMAs en vivo.
- **Tabla de operaciones** (como en las plataformas de criptomonedas): hora, compra o
  venta, precios de entrada y salida, pips, ganancia en USD y si cerró por TP o SL.
  La pestaña **Señales y eventos** muestra cada señal y qué hizo el ejecutor con ella.
- **Estado de servicios:** Kafka, IB Gateway, los tres procesos y la hora de la última vela.

Cerrar la pestaña del navegador **no** detiene los procesos.

---

## Seguridad: qué protege al sistema

- **Solo cuenta demo:** el ejecutor se niega a arrancar si el puerto de IB es el de
  una cuenta real (4001 o 7496) o si la cuenta no empieza con `DU`.
- **Lista de algoritmos autorizados:** solo se ejecutan señales de algoritmos permitidos.
- **Una posición a la vez** por símbolo (`RISK_MAX_POSITIONS=1`).
- **Tamaño fijo** por orden (10,000 unidades) con **stop-loss de 10 pips** y
  **take-profit de 20 pips**.
- **El dashboard solo escucha en tu computadora** (`127.0.0.1`) y exige un token
  para cada acción, para que ninguna otra página web pueda darle órdenes.

Estos controles no sustituyen la supervisión humana.

---

## Backtesting: probar con datos del pasado

Sirve para ver cómo **habría** funcionado una estrategia antes de usarla.

```powershell
python run_backtest.py --variant v3        # una variante
python run_variants.py                     # compara v1, v2, v3 y v4
python run_backtest.py --variant v3 --force-download   # vuelve a bajar datos de IB
```

Los datos (`data/historical/EURUSD_M1.csv`) se descargan de IB la primera vez.
Los reportes quedan en `reports/` con métricas, lista de operaciones y gráfica.

**Reglas para que el resultado sea honesto:**

- Los datos se dividen en **70 % entrenamiento (IS)** y **30 % validación (OOS)**; la
  estrategia se evalúa en datos que no "vio".
- **Sin mirar el futuro:** la señal sale al cerrar una vela y la orden entra en la
  **apertura de la siguiente**.
- Se descuenta un **spread de 2 pips** y **0.5 pips de slippage** en cada operación.
- Si en la misma vela se tocan el stop-loss y el take-profit, se asume que **perdió**.
- Capital inicial simulado: $10,000 USD; tamaño: 10,000 unidades.

---

## Algoritmo genético

Es una técnica inspirada en la evolución: se prueban muchas combinaciones de
parámetros (periodos de las EMAs, RSI, stop-loss y take-profit), se "cruzan" las
mejores y se repite varias generaciones.

```powershell
python run_ga_backtest.py --population-size 20 --generations 8 --seed 42
```

Usa **walk-forward** (entrena en un tramo y valida en el siguiente) y calcula el
**Deflated Sharpe Ratio (DSR)**, que castiga el haber probado demasiadas
combinaciones. Resultado: ver [Resultados](#resultados).

---

## Resultados

Datos: EUR/USD de 1 minuto, **30,146 velas** (~30 días), con spread y slippage incluidos.

### Comparación de las cuatro variantes

| Variante | Qué agrega | IS operaciones | IS PF | IS P&L | OOS operaciones | OOS PF | OOS Sharpe | OOS P&L |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **V1** `ema_cross_v1` | Solo el cruce de EMAs | 101 | 0.73 | −$197.50 | 44 | 0.73 | −5.26 | −$86.50 |
| **V2** `ema_cross_rsi` | + filtro RSI | 100 | 0.74 | −$187.50 | 44 | 0.73 | −5.26 | −$86.50 |
| **V3** `ema_cross_trend` | + filtro de tendencia (EMA 50) | 98 | 0.82 | −$121.50 | 39 | **1.14** | **+2.33** | **+$34.50** |
| **V4** `ema_cross_rsi_trend` | + RSI y tendencia | 96 | 0.81 | −$131.50 | 39 | 1.14 | +2.33 | +$34.50 |

*PF = profit factor (ganancias ÷ pérdidas; mayor a 1 es ganar). IS = entrenamiento, OOS = validación.*

- **V3 es la mejor**, pero solo en validación; en entrenamiento también pierde.
- **V4 = V3 en validación:** el filtro RSI no cambió nada cuando ya hay filtro de tendencia.

### Algoritmo genético vs. V3 (walk-forward)

| Estrategia | Tramo | Operaciones | PF | Sharpe | P&L | DSR |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| V3 base | Entrenamiento | 49 | 0.96 | −0.81 | −$13.50 | 0.44 |
| V3 base | Validación | 40 | 0.98 | −0.35 | −$5.50 | 0.48 |
| Genético | Entrenamiento | 43 | 1.52 | +8.30 | +$117.00 | 0.00 |
| Genético | Validación | 32 | 0.65 | **−7.84** | **−$113.00** | 0.00 |

El genético se veía genial en entrenamiento y **fracasó en validación**: encontró
parámetros "memorizados" del pasado (*overfitting*). El DSR de 0.0 lo detectó y
**se descartó**. **V3 sigue siendo la mejor opción.**

---

## Limitaciones (lo que hay que decir con honestidad)

1. **Pocos datos.** Con ~9 días de validación y 39 operaciones, el Sharpe de +2.33 de V3
   tiene un margen de error enorme (el intervalo de confianza al 95 % va de ≈ −18 a
   ≈ +23). En la práctica, **no sabemos si V3 gana o pierde**. Se necesitarían años de datos.
2. **Comisiones.** El backtest no cobra comisión, pero IB cobra aproximadamente
   **$2 USD por orden** en la cuenta demo (≈ $4 por operación completa, equivalente a
   ~4 pips con 10,000 unidades). Eso puede borrar una ventaja pequeña.
3. **El precio de la orden no es exactamente el de la señal.** El stop-loss y el
   take-profit se calculan con el precio de la última vela, pero la orden se llena un poco
   después al precio del mercado, así que las distancias reales pueden variar un par de pips.
4. **Una posición a la vez.** Si llega una señal contraria con una posición abierta, se
   ignora (la posición se cierra por su stop-loss o take-profit). El backtest funciona igual.
5. **Solo EUR/USD** probado. Faltaría validar en GBP/USD y por tipo de mercado
   (con tendencia o lateral).

Para continuar: más historia (1–2 años), probar en GBP/USD, incluir comisiones en el
backtest y probar otras estrategias.

---

## Estructura del proyecto

Los nombres de archivos están en inglés; los comentarios y mensajes, en español.

```
run_producer.py          Arranca el productor de ticks (IB → Kafka)
run_candles.py           Arranca Spark: ticks → velas
run_candle_consumer.py   Muestra en pantalla las velas que llegan a Kafka
run_algorithms.py        Arranca un algoritmo (--variant v1|v2|v3|v4)
run_executor.py          Arranca el ejecutor de órdenes
run_dashboard.py         Abre el dashboard (--demo para datos de ejemplo)
run_backtest.py          Backtest de una variante
run_variants.py          Compara las 4 variantes
run_ga_backtest.py       Algoritmo genético vs. V3
requirements.txt         Librerías de Python
docker-compose.yml       Kafka y Spark en Docker
Dockerfile.spark         Imagen de Docker para el job de Spark
.env.example             Plantilla de configuración (cópiala como .env)

config/settings.py       Lee la configuración del .env

src/
  producer/ib_tick_producer.py     Productor de ticks
  spark/ticks_to_candles.py        Convierte ticks en velas
  algorithms/
    base_algorithm.py              Clase base de todo algoritmo (Kafka, calentamiento)
    ema_cross.py                   Estrategia de cruce de EMAs con filtros
    indicators.py                  Cálculo de EMA, RSI y detección de cruces
    variants.py                    Definición de las variantes v1–v4
  executor/
    risk_manager.py                Reglas de riesgo
    signal_executor.py             Envía órdenes a IB y registra ejecuciones
  backtest/
    downloader.py                  Descarga historia de IB
    engine.py                      Simulador de operaciones
    metrics.py                     Métricas (Sharpe, drawdown, DSR, ...)
    genetic_optimizer.py           Algoritmo genético con walk-forward
  runtime/                         Soporte del dashboard: estado, procesos y cálculo de P&L
  common/                          Utilidades de Kafka y registro de logs

dashboard/               Servidor local y página web del panel (index.html, server.py, demo.py)
tests/                   Pruebas automáticas
data/historical/         Velas históricas (CSV)
reports/                 Reportes de backtests
logs/                    Registros de ejecución (se generan solos)
```

**¿Por qué hay un solo `indicators.py`?** Antes existían copias con nombre en español
(`indicadores.py`, `base_algoritmo.py`, `ticks_to_velas.py`, `run_velas.py`,
`run_consumer_velas.py`) que solo redirigían al archivo en inglés. Se eliminaron para
no tener duplicados. Lo único que se conserva en español, a propósito, son el tópico
`velas` y los campos de los mensajes (`inicio`, `num_ticks`, `id_algoritmo`,
`precio_referencia`), porque así lo define el formato acordado entre equipos.

---

## Formato de los mensajes

Este formato es común para todos los equipos.

**`ticks`** — cada cambio de precio

```json
{"symbol": "EURUSD", "bid": 1.097685, "ask": 1.097695, "timestamp": "2026-11-25T20:52:14.043526Z"}
```

**`velas`** — resumen de un minuto

```json
{"symbol": "EURUSD", "inicio": "2026-11-25T20:51:00.000Z", "open": 1.097665,
 "high": 1.097685, "low": 1.097630, "close": 1.097685, "num_ticks": 430}
```

**`signals`** — decisión del algoritmo

```json
{"id_algoritmo": "ema_cross_trend", "symbol": "EURUSD", "signal": "BUY",
 "precio_referencia": 1.097685, "timestamp": "2026-11-25T20:52:14.123456Z"}
```

`symbol` va siempre en mayúsculas y sin barra (`EURUSD`). `signal` es `BUY` o `SELL`.

---

## Configuración (archivo `.env`)

Los valores por defecto están en `config/settings.py`; el archivo `.env.example`
explica cada línea. Los más importantes:

| Variable | Valor por defecto | Para qué sirve |
| --- | --- | --- |
| `IB_PORT` | `4002` | Puerto de IB Gateway demo (7497 si usas TWS). |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Dirección de Kafka. |
| `SYMBOL` | `EURUSD` | Par de divisas. |
| `EMA_FAST` / `EMA_SLOW` | `9` / `21` | Periodos de las EMAs. |
| `EMA_TREND_PERIOD` | `50` | EMA del filtro de tendencia. |
| `RISK_TRADE_SIZE` | `10000` | Unidades por orden. |
| `RISK_STOP_LOSS_PIPS` / `RISK_TAKE_PROFIT_PIPS` | `10` / `20` | Protección de cada orden. |
| `RISK_MAX_POSITIONS` | `1` | Posiciones abiertas a la vez. |
| `EXECUTOR_ALLOWED_ALGORITHMS` | `ema_cross_v1` | Algoritmos autorizados (separados por coma). |
| `BACKTEST_SPREAD_PIPS` / `BACKTEST_SLIPPAGE_PIPS` | `2.0` / `0.5` | Costos simulados del backtest. |
| `DASHBOARD_PORT` | `8050` | Puerto del dashboard. |
| `DASHBOARD_MANAGE_PRODUCER` | `true` | Si el botón Iniciar también arranca el productor. |
| `DEFAULT_VARIANT` | `v3` | Estrategia con la que abre el dashboard. |

---

## Pruebas automáticas

Con el entorno activado, desde la carpeta del proyecto:

```powershell
# No necesitan IB ni Kafka (todas juntas)
python -m pytest tests/test_3.py tests/test_4.py tests/test_5.py tests/test_6.py tests/test_genetic_optimizer.py

# Necesita Java (usa Spark local)
python -m tests.test_2

# Necesitan Kafka corriendo
python -m tests.test_1
python -m tests.test_3_kafka
```

⚠️ `python -m tests.test_4_ib` **envía una orden real a la cuenta demo**. Ejecútalo solo
con IB Gateway abierto en la cuenta demo y mirando la pantalla.

---

## Preguntas frecuentes y solución de problemas

### En "Señales y eventos" veo muchas señales "Rechazadas". ¿Algo está mal?

No: es el control de riesgo funcionando. Cada señal aparece en **dos renglones**
(la señal recibida y lo que se hizo con ella). Los motivos más comunes:

- **"Máximo de posiciones abiertas alcanzado (1/1)":** ya hay una operación abierta y
  solo se permite una a la vez. Cuando esa operación cierre (por stop-loss o take-profit),
  se atenderá la siguiente señal.
- **"El algoritmo '…' no está autorizado":** la señal viene de un algoritmo distinto al
  seleccionado en el dashboard. Suele significar que **quedó un algoritmo viejo corriendo**
  (por ejemplo, v4 mientras eliges v3). Pulsa **Apagar procesos** y luego **Iniciar**.
- **"Ignorada (pausa)":** el trading estaba en pausa.

### No aparece ninguna vela o señal

Revisa que estén corriendo **IB Gateway, Kafka, Spark y el productor**. Recuerda que el
algoritmo necesita unas **52 velas de calentamiento** (~1 hora) la primera vez, y que
solo emite señal cuando hay un **cruce**: pasar horas sin señales es normal. Si el
mercado Forex está cerrado (fin de semana), tampoco llegan precios.

### `PYTHON_VERSION_MISMATCH` (Spark)

El driver y el worker de Spark deben usar **la misma versión de Python**. Activa el
entorno de Python 3.10 y usa `python run_candles.py`, o usa Docker.

### Aviso `winutils.exe` en Windows

Es un aviso habitual de Spark en Windows y normalmente **no es grave**. Si Spark falla de
verdad, usa la versión en Docker.

### IB dice `clientId already in use`

El número quedó ocupado por una desconexión brusca. Espera 30–60 segundos; el código
reintenta solo con otro número.

### La orden se queda en `Submitted`

Con la cuenta demo se suele llenar en 1–2 segundos. Si tarda, revisa que el mercado
Forex esté abierto y que el contrato sea `IDEALPRO`.

### Archivos que NO se suben a GitHub

Por el `.gitignore`: `.env` (datos personales), `logs/`, `data/`, `reports/`,
`checkpoints/`, `__pycache__/` y los entornos virtuales.

---

## Glosario

- **Forex:** mercado de compra y venta de divisas.
- **EUR/USD:** cuántos dólares cuesta un euro.
- **Bid / Ask:** precio al que te compran / precio al que te venden.
- **Tick:** cada cambio de precio.
- **Vela (candlestick):** resumen de un periodo: apertura (*open*), máximo (*high*),
  mínimo (*low*) y cierre (*close*).
- **EMA:** promedio móvil exponencial; suaviza el precio dándole más peso a lo reciente.
- **RSI:** indicador que mide si el precio subió o bajó "demasiado rápido".
- **Pip:** la unidad mínima de movimiento (0.0001 en EUR/USD). Con 10,000 unidades, 1 pip ≈ 1 USD.
- **Stop-loss / Take-profit:** órdenes automáticas que cierran la operación para limitar la
  pérdida / asegurar la ganancia.
- **Orden bracket:** una orden de entrada con su stop-loss y su take-profit ya incluidos.
- **Cuenta demo (paper):** cuenta con dinero ficticio.
- **Kafka / tópico:** servicio de mensajería entre programas / cada "buzón" de mensajes.
- **Spark:** motor que procesa datos en tiempo real.
- **Backtest:** probar una estrategia con datos del pasado.
- **IS / OOS:** datos de entrenamiento (*in-sample*) / de validación (*out-of-sample*).
- **Profit factor:** ganancias totales ÷ pérdidas totales.
- **Sharpe:** ganancia ajustada por el riesgo asumido; más alto es mejor.
- **Drawdown:** la mayor caída desde un punto máximo.
- **Overfitting:** ajustar tanto una estrategia al pasado que deja de funcionar en el futuro.
- **Walk-forward:** entrenar en un tramo de datos y validar en el siguiente, avanzando.
- **DSR:** Sharpe corregido por el número de pruebas realizadas.

---

## Licencia y uso

Proyecto educativo. **No usar con dinero real** sin una validación independiente
extensa, más historia de datos y supervisión profesional. Los autores no se hacen
responsables de pérdidas derivadas de su uso.
