from __future__ import annotations

"""
fase4_consumer.py  —  Consumer
================================
Lee el stream de eventos en tiempo real, calcula KPIs por ventana
y predice la demanda usando el modelo XGBoost entrenado en fase 3.

Simula el comportamiento de un consumer de Kafka + Spark Streaming:
- Lee eventos nuevos del topic cada INTERVALO_SEGUNDOS
- Acumula el estado global (inventario y ventas por SKU)
- Calcula KPIs de la ventana actual
- Predice demanda de la proxima semana con XGBoost
- Imprime un dashboard en consola
- Guarda los resultados en outputs/streaming/

Uso (en una segunda terminal mientras corre el producer):
    python src/fase4_consumer.py

Entradas:
    outputs/streaming/stream_topic.jsonl   <- topic simulado
    outputs/models/model_xgboost.pkl       <- modelo entrenado

Salidas:
    outputs/streaming/kpis_tiempo_real.csv
    outputs/streaming/alertas.csv
    outputs/streaming/predicciones_stream.csv
"""

import json
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# Columnas del parquet necesarias para el estado inicial
COLS_PARQUET = [
    'TIPO_TRANSACCION', 'FECHA', 'PAIS', 'CODIGO_SKU',
    'FAMILIA', 'MODELO', 'MARCA', 'TIER', 'ESTADO',
    'CODIGO_BODEGA', 'VALUE', 'PRECIO_UNITARIO',
]


# ============================================================
# CONFIGURACION
# ============================================================

BASE_DIR       = Path(__file__).resolve().parents[1]
OUTPUT_DIR     = BASE_DIR / 'outputs' / 'streaming'
TOPIC_FILE     = OUTPUT_DIR / 'stream_topic.jsonl'
MODEL_FILE     = BASE_DIR / 'outputs' / 'models' / 'model_xgboost.pkl'
DATA_FILE      = BASE_DIR / 'data' / 'processed' / 'datos_limpios.parquet'

# Minimo de semanas de historial para hacer prediccion
MIN_SEMANAS_HISTORIAL = 4

# Cada cuantos segundos procesa una ventana de eventos
INTERVALO_SEGUNDOS = 3

# Cuantos eventos procesar por ventana
TAMANO_VENTANA = 20

# Umbral de alerta de stockout (dias)
ALERTA_DOH_MINIMO = 7

# Cuantas ventanas procesar antes de terminar (None = infinito)
MAX_VENTANAS = None

# Semanas sin movimiento para considerar un SKU inactivo
SEMANAS_SIN_MOVIMIENTO = 4

# Cobertura objetivo en semanas (stock saludable)
COBERTURA_OBJETIVO_SEMANAS = 4

# Lead time en ventanas del stream — tránsito llega en 5 ventanas (~15 segundos en simulación)
LEAD_TIME_VENTANAS = 5


# ============================================================
# ESTADO GLOBAL
# ============================================================
# Simula el estado que Spark Streaming mantiene entre micro-batches

class EstadoStream:
    def __init__(self):
        # Stock actual por SKU x Pais
        self.stock: dict[str, int] = defaultdict(int)

        # Ventas acumuladas por SKU x Pais
        self.ventas: dict[str, int] = defaultdict(int)

        # Historial de demanda por SKU (ultimas 13 semanas) para los lags
        self.historial: dict[str, list[int]] = defaultdict(list)

        # Precios por SKU
        self.precios: dict[str, float] = defaultdict(float)

        # Metadata de SKUs
        self.metadata: dict[str, dict] = {}

        # KPIs historicos de ventanas anteriores
        self.kpis_historico: list[dict] = []

        # Alertas generadas
        self.alertas: list[dict] = []

        # Predicciones generadas
        self.predicciones: list[dict] = []

        # Ordenes de reposicion sugeridas
        self.ordenes: list[dict] = []

        # Transitos en camino: {clave_sku: [(ventana_llegada, cantidad), ...]}
        self.transitos_en_camino: dict[str, list[tuple]] = defaultdict(list)

        # Registro de SKUs sin movimiento
        self.skus_inactivos: dict[str, int] = {}

        # Ventas por SKU en la ventana actual (para detectar inactividad)
        self.ventanas_sin_venta: dict[str, int] = defaultdict(int)

        # Lineas ya procesadas del topic
        self.lineas_procesadas = 0

        # Numero de ventana actual
        self.n_ventana = 0

    def clave(self, pais: str, sku: str) -> str:
        return f'{pais}_{sku}'

    def procesar_evento(self, evento: dict) -> None:
        clave = self.clave(evento['pais'], evento['codigo_sku'])

        # Guardar metadata del SKU
        if clave not in self.metadata:
            self.metadata[clave] = {
                'pais'    : evento['pais'],
                'sku'     : evento['codigo_sku'],
                'familia' : evento.get('familia', ''),
                'modelo'  : evento.get('modelo', ''),
                'marca'   : evento.get('marca', ''),
                'tier'    : evento.get('tier', ''),
                'estado'  : evento.get('estado', 'ACTIVE'),
            }
        else:
            # Actualizar estado si llega en el evento (puede cambiar de ciclo de vida)
            if evento.get('estado'):
                self.metadata[clave]['estado'] = evento['estado']
            # Actualizar modelo y familia si estaban vacíos
            if not self.metadata[clave].get('modelo') and evento.get('modelo'):
                self.metadata[clave]['modelo'] = evento['modelo']
            if not self.metadata[clave].get('familia') and evento.get('familia'):
                self.metadata[clave]['familia'] = evento['familia']

        tipo = evento['tipo_evento']
        val  = evento['value']

        if tipo == 'INVENTORY':
            # Snapshot: reemplaza el stock actual
            self.stock[clave] = val

        elif tipo == 'SALES':
            # Acumular ventas y descontar del stock
            self.ventas[clave] += val
            self.stock[clave]   = max(0, self.stock[clave] - val)
            self.historial[clave].append(val)

            # Guardar precio
            if evento.get('precio_unitario', 0) > 0:
                self.precios[clave] = evento['precio_unitario']


# ============================================================
# ESTADO INICIAL DESDE EL PARQUET
# ============================================================

def cargar_estado_inicial(estado: EstadoStream) -> None:
    # Carga el ultimo snapshot de inventario y el historial de demanda
    # del parquet para que el consumer arranque con datos reales.
    # Se procesa por partes para no saturar la memoria.

    if not DATA_FILE.exists():
        print(f'Parquet no encontrado en {DATA_FILE}. Arrancando sin estado inicial.')
        return

    print('Cargando estado inicial desde parquet...')

    # -- Stock inicial --------------------------------------------------------
    # Leer solo inventario, quedarse con el ultimo snapshot por SKU x Bodega
    df_inv = pd.read_parquet(DATA_FILE, columns=[
        'TIPO_TRANSACCION', 'FECHA', 'PAIS', 'CODIGO_SKU', 'CODIGO_BODEGA', 'VALUE'
    ])
    df_inv['TIPO_TRANSACCION'] = df_inv['TIPO_TRANSACCION'].astype(str)
    df_inv['PAIS']             = df_inv['PAIS'].astype(str)
    df_inv['FECHA']            = pd.to_datetime(df_inv['FECHA'])

    inventario = df_inv[df_inv['TIPO_TRANSACCION'] == 'INVENTORY']

    ultimo_inv = (
        inventario
        .sort_values('FECHA')
        .groupby(['PAIS', 'CODIGO_SKU', 'CODIGO_BODEGA'], as_index=False)
        .last()
    )
    # Consolidar por SKU x Pais sumando bodegas
    stock_por_sku = (
        ultimo_inv
        .groupby(['PAIS', 'CODIGO_SKU'])['VALUE']
        .sum()
        .reset_index()
    )
    for _, row in stock_por_sku.iterrows():
        clave = estado.clave(row['PAIS'], row['CODIGO_SKU'])
        estado.stock[clave] = int(row['VALUE'])

    del df_inv, inventario, ultimo_inv
    print(f'  Stock inicial cargado: {len(stock_por_sku):,} SKUs')

    # -- Historial de demanda -------------------------------------------------
    # Leer solo ventas de SKUs ACTIVE con las columnas minimas
    df_ven = pd.read_parquet(DATA_FILE, columns=[
        'TIPO_TRANSACCION', 'FECHA', 'PAIS', 'CODIGO_SKU',
        'ESTADO', 'VALUE', 'PRECIO_UNITARIO',
        'FAMILIA', 'MODELO', 'MARCA', 'TIER',
    ])
    df_ven['TIPO_TRANSACCION'] = df_ven['TIPO_TRANSACCION'].astype(str)
    df_ven['ESTADO']           = df_ven['ESTADO'].astype(str).str.upper().str.strip()
    df_ven['PAIS']             = df_ven['PAIS'].astype(str)
    df_ven['FECHA']            = pd.to_datetime(df_ven['FECHA'])

    ventas = df_ven[
        (df_ven['TIPO_TRANSACCION'] == 'SALES') &
        (df_ven['ESTADO'] != 'EOL')
    ].copy()

    ventas['YEAR_WEEK'] = ventas['FECHA'].dt.to_period('W').dt.start_time

    demanda_semanal = (
        ventas
        .groupby(['PAIS', 'CODIGO_SKU', 'YEAR_WEEK'])['VALUE']
        .sum()
        .reset_index()
        .sort_values(['PAIS', 'CODIGO_SKU', 'YEAR_WEEK'])
    )
    for (pais, sku), grp in demanda_semanal.groupby(['PAIS', 'CODIGO_SKU']):
        clave = estado.clave(pais, sku)
        estado.historial[clave] = grp['VALUE'].tail(13).tolist()

    n_hist = demanda_semanal.groupby(['PAIS', 'CODIGO_SKU']).ngroups
    print(f'  Historial cargado: {n_hist:,} SKUs')

    # -- Precios --------------------------------------------------------------
    precios = (
        ventas
        .groupby(['PAIS', 'CODIGO_SKU'])['PRECIO_UNITARIO']
        .median()
        .reset_index()
    )
    for _, row in precios.iterrows():
        clave = estado.clave(row['PAIS'], row['CODIGO_SKU'])
        estado.precios[clave] = float(row['PRECIO_UNITARIO'])

    # -- Metadata -------------------------------------------------------------
    meta = (
        df_ven[['PAIS', 'CODIGO_SKU', 'FAMILIA', 'MODELO', 'MARCA', 'TIER', 'ESTADO']]
        .drop_duplicates(subset=['PAIS', 'CODIGO_SKU'])
        .reset_index(drop=True)
    )
    for _, row in meta.iterrows():
        clave = estado.clave(row['PAIS'], row['CODIGO_SKU'])
        estado.metadata[clave] = {
            'pais'    : row['PAIS'],
            'sku'     : row['CODIGO_SKU'],
            'familia' : str(row.get('FAMILIA', '')),
            'modelo'  : str(row.get('MODELO', '')),
            'marca'   : str(row.get('MARCA', '')),
            'tier'    : str(row.get('TIER', '')),
            'estado'  : str(row.get('ESTADO', '')),
        }

    del df_ven, ventas, demanda_semanal
    print('  Estado inicial listo.')


# ============================================================
# LECTURA DEL TOPIC
# ============================================================

def leer_eventos_nuevos(estado: EstadoStream) -> list[dict]:
    if not TOPIC_FILE.exists():
        return []

    with open(TOPIC_FILE, 'r', encoding='utf-8') as f:
        todas_las_lineas = f.readlines()

    nuevas = todas_las_lineas[estado.lineas_procesadas:]
    eventos = []
    for linea in nuevas[:TAMANO_VENTANA]:
        linea = linea.strip()
        if linea:
            try:
                eventos.append(json.loads(linea))
            except json.JSONDecodeError:
                continue

    estado.lineas_procesadas += len(eventos)
    return eventos


# ============================================================
# CALCULO DE KPIs
# ============================================================

def calcular_kpis_ventana(estado: EstadoStream, timestamp: str) -> pd.DataFrame:
    filas = []

    for clave, stock in estado.stock.items():
        ventas = estado.ventas.get(clave, 0)
        meta   = estado.metadata.get(clave, {})
        precio = estado.precios.get(clave, 0)
        hist   = estado.historial.get(clave, [])

        # DOH: usar promedio semanal historico como referencia de demanda
        # Es mas estable que las ventas acumuladas del stream (que son pocas
        # al inicio). Si ya hay ventas en el stream usamos el maximo entre
        # ambas para ser conservadores.
        dem_hist_semanal  = np.mean(hist[-4:]) if len(hist) >= 2 else 0
        dem_stream_diaria = ventas / 30 if ventas > 0 else 0
        dem_hist_diaria   = dem_hist_semanal / 7 if dem_hist_semanal > 0 else 0

        # Tomar la demanda diaria mas alta como referencia (mas conservador)
        dem_diaria = max(dem_hist_diaria, dem_stream_diaria)
        doh        = round(stock / dem_diaria, 1) if dem_diaria > 0 else 999

        # Sell-through sobre el disponible total
        disponible   = ventas + stock
        sell_through = round(ventas / disponible, 3) if disponible > 0 else 0

        # Ingreso acumulado
        ingreso = ventas * precio

        filas.append({
            'timestamp'      : timestamp,
            'ventana'        : estado.n_ventana,
            'pais'           : meta.get('pais', ''),
            'codigo_sku'     : meta.get('sku', ''),
            'familia'        : meta.get('familia', ''),
            'marca'          : meta.get('marca', ''),
            'tier'           : meta.get('tier', ''),
            'stock_actual'   : stock,
            'ventas_acum'    : ventas,
            'dem_semanal_ref': round(dem_hist_semanal, 1),
            'doh'            : doh,
            'sell_through'   : sell_through,
            'ingreso_acum'   : round(ingreso, 2),
            'precio_prom'    : round(precio, 2),
        })

    if not filas:
        return pd.DataFrame()

    return pd.DataFrame(filas).sort_values('ventas_acum', ascending=False)


def detectar_alertas(kpis: pd.DataFrame, estado: EstadoStream) -> list[dict]:
    alertas = []

    for _, row in kpis.iterrows():
        clave   = estado.clave(row['pais'], row['codigo_sku'])
        meta    = estado.metadata.get(clave, {})

        # SKUs EOL no generan alertas — ya no tienen demanda futura
        if str(meta.get('estado', '')).upper() == 'EOL':
            continue

        hist    = estado.historial.get(clave, [])
        prom    = np.mean(hist) if hist else 0
        std     = np.std(hist)  if hist else 0

        # -- STOCKOUT: stock en cero con ventas activas ----------------------
        if row['stock_actual'] == 0 and row['ventas_acum'] > 0:
            alertas.append({
                'timestamp'  : row['timestamp'],
                'ventana'    : row['ventana'],
                'tipo_alerta': 'STOCKOUT',
                'pais'       : row['pais'],
                'codigo_sku' : row['codigo_sku'],
                'marca'      : row['marca'],
                'modelo'     : row.get('modelo', estado.metadata.get(estado.clave(row['pais'], row['codigo_sku']), {}).get('modelo', '')),
                'detalle'    : f'Stock = 0 con {row["ventas_acum"]} uds vendidas',
            })

        # -- DOH BAJO: menos de ALERTA_DOH_MINIMO dias de cobertura ----------
        elif 0 < row['doh'] < ALERTA_DOH_MINIMO:
            alertas.append({
                'timestamp'  : row['timestamp'],
                'ventana'    : row['ventana'],
                'tipo_alerta': 'DOH_BAJO',
                'pais'       : row['pais'],
                'codigo_sku' : row['codigo_sku'],
                'marca'      : row['marca'],
                'modelo'     : row.get('modelo', estado.metadata.get(estado.clave(row['pais'], row['codigo_sku']), {}).get('modelo', '')),
                'detalle'    : f'DOH = {row["doh"]} dias (umbral: {ALERTA_DOH_MINIMO})',
            })

        # -- SOBRESTOCK: DOH mayor a 90 dias con demanda real ----------------
        # También detectar sobrestock anticipado: stock + tránsitos >> demanda esperada
        dem_ref = row.get('dem_semanal_ref', 0)
        transitos_clave = sum(q for _, q in estado.transitos_en_camino.get(clave, []))
        stock_total_futuro = row['stock_actual'] + transitos_clave
        cobertura_futura = stock_total_futuro / (dem_ref / 7) if dem_ref > 0 else 999

        if row['doh'] > 90 and row['stock_actual'] > 0 and dem_ref >= 1:
            alertas.append({
                'timestamp'  : row['timestamp'],
                'ventana'    : row['ventana'],
                'tipo_alerta': 'SOBRESTOCK',
                'pais'       : row['pais'],
                'codigo_sku' : row['codigo_sku'],
                'marca'      : row['marca'],
                'modelo'     : row.get('modelo', estado.metadata.get(estado.clave(row['pais'], row['codigo_sku']), {}).get('modelo', '')),
                'detalle'    : f'DOH = {row["doh"]:.0f} dias | dem. semanal ref: {dem_ref:.1f} uds',
            })
        elif cobertura_futura > 120 and transitos_clave > 0 and dem_ref >= 1:
            # Sobrestock anticipado: el tránsito en camino generará exceso
            alertas.append({
                'timestamp'  : row['timestamp'],
                'ventana'    : row['ventana'],
                'tipo_alerta': 'SOBRESTOCK',
                'pais'       : row['pais'],
                'codigo_sku' : row['codigo_sku'],
                'marca'      : row['marca'],
                'modelo'     : row.get('modelo', estado.metadata.get(estado.clave(row['pais'], row['codigo_sku']), {}).get('modelo', '')),
                'detalle'    : f'Cobertura futura {cobertura_futura:.0f} dias (stock+tránsito={stock_total_futuro} uds)',
            })

        # -- PICO DE DEMANDA: ventas > promedio + 2 desviaciones -------------
        if prom > 0 and std > 0 and row['ventas_acum'] > (prom + 2 * std):
            alertas.append({
                'timestamp'  : row['timestamp'],
                'ventana'    : row['ventana'],
                'tipo_alerta': 'PICO_DEMANDA',
                'pais'       : row['pais'],
                'codigo_sku' : row['codigo_sku'],
                'marca'      : row['marca'],
                'modelo'     : row.get('modelo', estado.metadata.get(estado.clave(row['pais'], row['codigo_sku']), {}).get('modelo', '')),
                'detalle'    : (
                    f'Ventas {row["ventas_acum"]:.0f} uds -- '
                    f'promedio historico {prom:.1f} uds '
                    f'(+{((row["ventas_acum"]-prom)/prom*100):.0f}%)'
                ),
            })

    # -- COBERTURA INSUFICIENTE: prediccion supera el stock actual -----------
    # Se genera una vez por ventana para los SKUs sin cobertura
    for pred in estado.predicciones:
        if pred.get('ventana') != kpis['ventana'].iloc[0]:
            continue
        if not pred['cobertura_ok'] and pred['stock_actual'] > 0:
            alertas.append({
                'timestamp'  : pred['timestamp'],
                'ventana'    : pred['ventana'],
                'tipo_alerta': 'COBERTURA_INSUFICIENTE',
                'pais'       : pred['pais'],
                'codigo_sku' : pred['codigo_sku'],
                'marca'      : pred['marca'],
                'modelo'     : pred.get('modelo', ''),
                'detalle'    : (
                    f'Stock {pred["stock_actual"]} uds -- '
                    f'demanda predicha {pred["demanda_pred_7d"]:.1f} uds proxima semana'
                ),
            })

    return alertas


# ============================================================
# PREDICCION CON XGBOOST
# ============================================================

def predecir_demanda(
    kpis:   pd.DataFrame,
    estado: EstadoStream,
    modelo,
    timestamp: str,
) -> list[dict]:
    if modelo is None or kpis.empty:
        return []

    predicciones = []
    ahora        = datetime.now()

    for _, row in kpis.head(20).iterrows():  # top 20 SKUs con mas ventas
        clave    = estado.clave(row['pais'], row['codigo_sku'])
        meta     = estado.metadata.get(clave, {})
        estado_sku = str(meta.get('estado', '')).upper()

        # EOL: no predecir — ya no hay demanda futura
        if estado_sku == 'EOL':
            continue

        hist     = estado.historial.get(clave, [])
        fuente_pred = 'propio'

        if len(hist) < MIN_SEMANAS_HISTORIAL:
            # SKU NEW o ACTIVE sin historial suficiente
            # Buscar sustituto para usar su historial como proxy
            sust = buscar_sustituto(
                pais   = row['pais'],
                sku    = row['codigo_sku'],
                marca  = row['marca'],
                tier   = row['tier'],
                estado = estado,
                familia= row['familia'],
            )
            if sust is None:
                continue
            clave_sust = estado.clave(row['pais'], sust['sku_sustituto'])
            hist       = estado.historial.get(clave_sust, [])
            if len(hist) < MIN_SEMANAS_HISTORIAL:
                continue
            fuente_pred = 'sustituto'

        # Construir el vector de features igual que en el notebook
        semana_ano = ahora.isocalendar().week
        mes        = ahora.month

        features = {
            'SEMANA_ANO'     : semana_ano,
            'MES'            : mes,
            'TRIMESTRE'      : (mes - 1) // 3 + 1,
            'ANO'            : ahora.year,
            'SEMANA_SIN'     : np.sin(2 * np.pi * semana_ano / 52),
            'SEMANA_COS'     : np.cos(2 * np.pi * semana_ano / 52),
            'MES_SIN'        : np.sin(2 * np.pi * mes / 12),
            'MES_COS'        : np.cos(2 * np.pi * mes / 12),
            'LAG_1W'         : hist[-1] if len(hist) >= 1 else 0,
            'LAG_2W'         : hist[-2] if len(hist) >= 2 else 0,
            'LAG_4W'         : hist[-4] if len(hist) >= 4 else hist[0],
            'LAG_8W'         : hist[-8] if len(hist) >= 8 else hist[0],
            'LAG_13W'        : hist[-13] if len(hist) >= 13 else hist[0],
            'ROLLING_4W_MEAN': np.mean(hist[-4:]),
            'ROLLING_4W_STD' : np.std(hist[-4:]) if len(hist) >= 2 else 0,
            'ROLLING_8W_MEAN': np.mean(hist[-8:]),
            'PRECIO_PROM'    : row['precio_prom'],
            'COSTO_PROM'     : row['precio_prom'] * 0.55,
            'NUM_TRANSAC'    : max(1, len(hist)),
            'PAIS_ENC'       : hash(row['pais']) % 10,
            'FAMILIA_ENC'    : hash(row['familia']) % 50,
            'MARCA_ENC'      : hash(row['marca']) % 20,
        }

        X = pd.DataFrame([features])

        try:
            pred = float(modelo.predict(X)[0])
            pred = max(0, round(pred, 1))
        except Exception:
            continue

        predicciones.append({
            'timestamp'       : timestamp,
            'ventana'         : estado.n_ventana,
            'pais'            : row['pais'],
            'codigo_sku'      : row['codigo_sku'],
            'modelo'          : meta.get('modelo', ''),
            'marca'           : row['marca'],
            'tier'            : row['tier'],
            'estado_sku'      : estado_sku,
            'fuente_pred'     : fuente_pred,
            'demanda_pred_7d' : pred,
            'stock_actual'    : row['stock_actual'],
            'cobertura_ok'    : row['stock_actual'] >= pred,
        })

    return predicciones


# ============================================================
# SUSTITUTOS Y ORDENES DE REPOSICION
# ============================================================

def buscar_sustituto(
    pais:     str,
    sku:      str,
    marca:    str,
    tier:     str,
    estado:   EstadoStream,
    familia:  str = '',
) -> dict | None:
    # Busca un SKU del mismo pais+marca que sirva como referencia de demanda
    # para un SKU nuevo sin historial de ventas.
    #
    # Nivel 1 (mas especifico): mismo pais + marca + familia + tier
    # Nivel 2 (fallback):       mismo pais + marca + tier  (sin importar familia)
    #
    # No filtra por estado EOL: cualquier SKU con historial sirve como
    # referencia de demanda independientemente de si sigue activo.

    def _buscar(require_familia: bool) -> dict | None:
        candidatos = []

        for clave, meta in estado.metadata.items():
            if meta.get('pais')  != pais:  continue
            if meta.get('sku')   == sku:   continue
            if meta.get('marca') != marca: continue
            if meta.get('tier')  != tier:  continue
            if require_familia and familia and meta.get('familia') != familia:
                continue

            hist = estado.historial.get(clave, [])
            dem  = np.mean(hist[-4:]) if len(hist) >= 4 else (
                   np.mean(hist)      if len(hist) >= 1 else 0)

            if dem > 0:
                candidatos.append({
                    'sku_sustituto'  : meta['sku'],
                    'modelo'         : meta.get('modelo', ''),
                    'marca'          : meta.get('marca', ''),
                    'familia'        : meta.get('familia', ''),
                    'tier'           : meta.get('tier', ''),
                    'nivel_busqueda' : 1 if require_familia else 2,
                    'demanda_semanal': round(dem, 1),
                })

        if not candidatos:
            return None

        # Preferir el modelo más reciente: ordenar por nombre de modelo
        # descendente (ej. iPhone 15 > iPhone 14) y luego por demanda
        candidatos.sort(
            key=lambda x: (x['modelo'], x['demanda_semanal']),
            reverse=True
        )
        return candidatos[0]

    # Nivel 1: con familia
    resultado = _buscar(require_familia=True)
    if resultado:
        return resultado

    # Nivel 2: sin familia (solo marca + tier + pais)
    return _buscar(require_familia=False)


def calcular_cantidad_reponer(
    stock_actual:    int,
    demanda_semanal: float,
    stock_en_transito: int = 0,
) -> int:
    # Calcula cuántas unidades pedir considerando:
    # 1. El stock actual se consume durante el lead time
    # 2. El stock en tránsito ya está comprometido
    # 3. La cantidad pedida debe cubrir COBERTURA_OBJETIVO_SEMANAS sin generar sobrestock
    #
    # Lógica:
    #   consumo_lead_time = demanda_semanal * (LEAD_TIME_VENTANAS / ventanas_por_semana)
    #   stock_al_llegar   = stock_actual - consumo_lead_time (lo que quedará cuando llegue)
    #   stock_total_futuro = stock_al_llegar + stock_en_transito + cantidad_a_pedir
    #   stock_objetivo    = demanda_semanal * COBERTURA_OBJETIVO_SEMANAS (sin sobrestock)
    #   cantidad_a_pedir  = stock_objetivo - stock_al_llegar - stock_en_transito

    if demanda_semanal <= 0:
        return 0

    # Ventanas por semana (con 3s por ventana y ~21600s en un día hábil)
    ventanas_por_semana = 7 * 24 * 3600 / INTERVALO_SEGUNDOS  # ventanas reales
    consumo_lead_time   = demanda_semanal * (LEAD_TIME_VENTANAS / ventanas_por_semana)
    consumo_lead_time   = max(1, consumo_lead_time)  # mínimo 1 unidad

    stock_al_llegar = max(0, stock_actual - consumo_lead_time)
    stock_disponible = stock_al_llegar + stock_en_transito

    # Stock objetivo: cobertura deseada sin sobrestock
    stock_objetivo = demanda_semanal * COBERTURA_OBJETIVO_SEMANAS

    # Solo pedir si el stock futuro no cubre el objetivo
    cantidad = max(0, int(stock_objetivo - stock_disponible))

    # Cap: no pedir más del doble del objetivo para evitar sobrestock
    cantidad = min(cantidad, int(stock_objetivo * 2))

    return cantidad


def procesar_transitos_llegados(estado: EstadoStream, n_ventana: int) -> list[dict]:
    # Revisa si algun transito programado llega en esta ventana
    # y lo suma al stock
    llegadas = []

    for clave, lista in estado.transitos_en_camino.items():
        por_llegar = []
        for ventana_llegada, cantidad in lista:
            if n_ventana >= ventana_llegada:
                # El transito llego — sumar al stock
                estado.stock[clave] = estado.stock.get(clave, 0) + cantidad
                meta = estado.metadata.get(clave, {})
                llegadas.append({
                    'clave'    : clave,
                    'pais'     : meta.get('pais', ''),
                    'sku'      : meta.get('sku', ''),
                    'marca'    : meta.get('marca', ''),
                    'cantidad' : cantidad,
                    'stock_nuevo': estado.stock[clave],
                })
            else:
                por_llegar.append((ventana_llegada, cantidad))
        estado.transitos_en_camino[clave] = por_llegar

    return llegadas


def generar_ordenes_reposicion(
    alertas:      list[dict],
    predicciones: list[dict],
    estado:       EstadoStream,
    timestamp:    str,
    n_ventana:    int,
) -> list[dict]:
    ordenes = []

    # -- Tránsitos por stockout o cobertura insuficiente ---------------------
    skus_reponer = set()
    for a in alertas:
        if a['tipo_alerta'] in ('STOCKOUT', 'COBERTURA_INSUFICIENTE', 'DOH_BAJO'):
            skus_reponer.add((a['pais'], a['codigo_sku']))

    for pais, sku in skus_reponer:
        clave = estado.clave(pais, sku)
        meta  = estado.metadata.get(clave, {})

        # Evitar generar doble orden si ya hay un transito en camino
        if estado.transitos_en_camino.get(clave):
            continue

        # Demanda semanal: desde prediccion o desde historial
        pred_sku = next(
            (p for p in predicciones
             if p['pais'] == pais and p['codigo_sku'] == sku),
            None
        )
        if pred_sku:
            dem_semanal = pred_sku['demanda_pred_7d']
        else:
            hist        = estado.historial.get(clave, [])
            dem_semanal = np.mean(hist[-4:]) if len(hist) >= 4 else 1.0

        stock_actual      = estado.stock.get(clave, 0)
        # Sumar tránsitos ya en camino para no sobreordenar
        transitos_camino  = sum(q for _, q in estado.transitos_en_camino.get(clave, []))
        cantidad          = calcular_cantidad_reponer(stock_actual, dem_semanal, transitos_camino)

        if cantidad <= 0:
            continue

        # Programar la llegada del transito
        ventana_llegada = n_ventana + LEAD_TIME_VENTANAS
        estado.transitos_en_camino[clave].append((ventana_llegada, cantidad))

        ordenes.append({
            'timestamp'        : timestamp,
            'ventana'          : n_ventana,
            'tipo_orden'       : 'TRANSITO_SUGERIDO',
            'pais'             : pais,
            'codigo_sku'       : sku,
            'modelo'           : meta.get('modelo', ''),
            'marca'            : meta.get('marca', ''),
            'tier'             : meta.get('tier', ''),
            'stock_actual'     : stock_actual,
            'demanda_semanal'  : round(dem_semanal, 1),
            'cantidad_sugerida': cantidad,
            'cobertura_actual_dias': round(stock_actual / dem_semanal * 7, 1) if dem_semanal > 0 else 0,
            'cobertura_objetivo_dias': COBERTURA_OBJETIVO_SEMANAS * 7,
            'ventana_llegada'  : ventana_llegada,
            'motivo'           : 'Cobertura por debajo del objetivo de 4 semanas',
        })

    # -- Sustitutos para SKUs sin movimiento ---------------------------------
    for clave, n_ventanas in estado.ventanas_sin_venta.items():
        if n_ventanas < SEMANAS_SIN_MOVIMIENTO:
            continue

        meta  = estado.metadata.get(clave, {})
        pais  = meta.get('pais', '')
        sku   = meta.get('sku', '')
        marca = meta.get('marca', '')
        tier  = meta.get('tier', '')
        familia = meta.get('familia', '')
        estado_sku = str(meta.get('estado', '')).upper()

        if not sku:
            continue

        # EOL: no sugerir órdenes ni sustitutos
        if estado_sku == 'EOL':
            continue

        # Solo sugerir una vez (cuando alcanza exactamente el umbral)
        if n_ventanas != SEMANAS_SIN_MOVIMIENTO:
            continue

        sustituto = buscar_sustituto(pais, sku, marca, tier, estado, familia)

        ordenes.append({
            'timestamp'        : timestamp,
            'ventana'          : n_ventana,
            'tipo_orden'       : 'SUSTITUTO_SUGERIDO',
            'pais'             : pais,
            'codigo_sku'       : sku,
            'modelo'           : meta.get('modelo', ''),
            'marca'            : marca,
            'tier'             : tier,
            'stock_actual'     : estado.stock.get(clave, 0),
            'demanda_semanal'  : 0,
            'cantidad_sugerida': 0,
            'cobertura_actual_dias': 0,
            'cobertura_objetivo_dias': 0,
            'ventana_llegada'  : 0,
            'motivo'           : f'{n_ventanas} ventanas sin movimiento',
            'sku_sustituto'    : sustituto['sku_sustituto']   if sustituto else 'SIN_SUSTITUTO',
            'modelo_sustituto' : sustituto['modelo']          if sustituto else '',
            'familia_sustituto': sustituto.get('familia', '') if sustituto else '',
            'nivel_busqueda'   : sustituto.get('nivel_busqueda', 0) if sustituto else 0,
            'dem_sustituto'    : sustituto['demanda_semanal'] if sustituto else 0,
        })

    return ordenes


def actualizar_inactividad(kpis: pd.DataFrame, estado: EstadoStream) -> None:
    # Incrementar contador de ventanas sin venta por SKU
    skus_con_venta = set(
        estado.clave(row['pais'], row['codigo_sku'])
        for _, row in kpis.iterrows()
        if row['ventas_acum'] > 0
    )

    for clave in list(estado.metadata.keys()):
        if clave in skus_con_venta:
            # Resetear si volvio a vender
            estado.ventanas_sin_venta[clave] = 0
        else:
            estado.ventanas_sin_venta[clave] += 1


# ============================================================
# DASHBOARD EN CONSOLA
# ============================================================

def imprimir_dashboard(
    kpis:        pd.DataFrame,
    alertas:     list[dict],
    predicciones: list[dict],
    eventos_nuevos: int,
    estado:      EstadoStream,
) -> None:
    ts = datetime.now().strftime('%H:%M:%S')

    print()
    print('=' * 65)
    print(f'  VENTANA {estado.n_ventana:03d}  |  {ts}  |  {eventos_nuevos} eventos nuevos')
    print('=' * 65)

    if kpis.empty:
        print('  Sin datos aun...')
        return

    # -- Resumen de la ventana ------------------------------------------------
    total_skus    = len(kpis)
    skus_activos  = (kpis['ventas_acum'] > 0).sum()
    skus_stockout = (kpis['stock_actual'] == 0).sum()
    ventas_tot    = kpis['ventas_acum'].sum()
    ingreso_tot   = kpis['ingreso_acum'].sum()
    doh_prom      = kpis[kpis['doh'] < 999]['doh'].mean()

    print(f'  SKUs monitoreados : {total_skus}')
    print(f'  SKUs con ventas   : {skus_activos}')
    print(f'  SKUs en stockout  : {skus_stockout}')
    print(f'  Ventas acumuladas : {ventas_tot:,.0f} uds')
    print(f'  Ingresos acumul.  : USD {ingreso_tot:,.2f}')
    print(f'  DOH promedio      : {doh_prom:.1f} dias' if not np.isnan(doh_prom) else '  DOH promedio      : --')

    # -- Top 5 SKUs por ventas ------------------------------------------------
    print()
    print('  Top 5 SKUs por ventas:')
    print(f'  {"SKU":<12} {"MARCA":<12} {"STOCK":>6} {"VENTAS":>7} {"DOH":>6} {"S.THRU":>7}')
    print('  ' + '-' * 55)
    for _, row in kpis.head(5).iterrows():
        doh_str = f'{row["doh"]:.0f}d' if row['doh'] < 999 else '--'
        print(
            f'  {row["codigo_sku"]:<12} '
            f'{str(row["marca"])[:10]:<12} '
            f'{row["stock_actual"]:>6,} '
            f'{row["ventas_acum"]:>7,} '
            f'{doh_str:>6} '
            f'{row["sell_through"]:>7.1%}'
        )

    # -- Alertas agrupadas por tipo ------------------------------------------
    if alertas:
        print()
        # Agrupar por tipo para que el dashboard sea mas legible
        por_tipo = {}
        for a in alertas:
            por_tipo.setdefault(a['tipo_alerta'], []).append(a)

        ICONOS = {
            'STOCKOUT'              : '!! STOCKOUT',
            'DOH_BAJO'              : '!  DOH BAJO',
            'SOBRESTOCK'            : 'i  SOBRESTOCK',
            'PICO_DEMANDA'          : '** PICO DEMANDA',
            'COBERTURA_INSUFICIENTE': '!  COBERTURA',
        }

        print(f'  ALERTAS ({len(alertas)}):')
        for tipo, lista in por_tipo.items():
            etiqueta = ICONOS.get(tipo, tipo)
            for a in lista[:3]:
                print(f'  [{etiqueta}] {a["pais"]} | {a["codigo_sku"]} | {a["detalle"]}')

    # -- Ordenes de reposicion y sustitutos ----------------------------------
    ordenes_ventana = [
        o for o in estado.ordenes
        if o.get('ventana') == estado.n_ventana
    ]
    if ordenes_ventana:
        transitos  = [o for o in ordenes_ventana if o['tipo_orden'] == 'TRANSITO_SUGERIDO']
        sustitutos = [o for o in ordenes_ventana if o['tipo_orden'] == 'SUSTITUTO_SUGERIDO']

        if transitos:
            print()
            print(f'  Transitos sugeridos ({len(transitos)}):')
            print(f'  {"SKU":<12} {"MARCA":<10} {"STOCK":>6} {"DEM/S":>6} {"PEDIR":>6} {"COB.ACT":>8} {"COB.OBJ":>8}')
            print('  ' + '-' * 65)
            for o in transitos[:5]:
                print(
                    f'  {o["codigo_sku"]:<12} '
                    f'{str(o["marca"])[:8]:<10} '
                    f'{o["stock_actual"]:>6} '
                    f'{o["demanda_semanal"]:>6.1f} '
                    f'{o["cantidad_sugerida"]:>6} '
                    f'{o["cobertura_actual_dias"]:>7.0f}d '
                    f'{o["cobertura_objetivo_dias"]:>7.0f}d'
                )

        if sustitutos:
            print()
            print(f'  Sustitutos sugeridos ({len(sustitutos)}):')
            print(f'  {"SKU":<12} {"SUSTITUTO":<12} {"DEM/SEM":>8}  MOTIVO')
            print('  ' + '-' * 55)
            for o in sustitutos[:5]:
                print(
                    f'  {o["codigo_sku"]:<12} '
                    f'{str(o.get("sku_sustituto",""))[:10]:<12} '
                    f'{o.get("dem_sustituto", 0):>8.1f}  '
                    f'{o["motivo"][:25]}'
                )

    # -- Predicciones ---------------------------------------------------------
    if predicciones:
        print()
        print(f'  Prediccion demanda proxima semana (top 5):')
        print(f'  {"SKU":<12} {"MARCA":<12} {"STOCK":>6} {"PRED 7d":>8} {"OK?":>5}')
        print('  ' + '-' * 47)
        for p in predicciones[:5]:
            ok = 'SI' if p['cobertura_ok'] else 'NO'
            print(
                f'  {p["codigo_sku"]:<12} '
                f'{str(p["marca"])[:10]:<12} '
                f'{p["stock_actual"]:>6,} '
                f'{p["demanda_pred_7d"]:>8.1f} '
                f'{ok:>5}'
            )

    print('=' * 65)


# ============================================================
# GUARDAR RESULTADOS
# ============================================================

def guardar_resultados(estado: EstadoStream) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if estado.kpis_historico:
        pd.DataFrame(estado.kpis_historico).to_csv(
            OUTPUT_DIR / 'kpis_tiempo_real.csv', index=False
        )

    if estado.alertas:
        pd.DataFrame(estado.alertas).to_csv(
            OUTPUT_DIR / 'alertas.csv', index=False
        )

    if estado.predicciones:
        pd.DataFrame(estado.predicciones).to_csv(
            OUTPUT_DIR / 'predicciones_stream.csv', index=False
        )

    if estado.ordenes:
        pd.DataFrame(estado.ordenes).to_csv(
            OUTPUT_DIR / 'ordenes_sugeridas.csv', index=False
        )

    print(f'\nResultados guardados en {OUTPUT_DIR}')
    print(f'  kpis_tiempo_real.csv      : {len(estado.kpis_historico)} registros')
    print(f'  alertas.csv               : {len(estado.alertas)} alertas')
    print(f'  predicciones_stream.csv   : {len(estado.predicciones)} predicciones')
    print(f'  ordenes_sugeridas.csv     : {len(estado.ordenes)} ordenes')


# ============================================================
# MAIN
# ============================================================

def cargar_stock_inicial(estado: EstadoStream) -> None:
    # Carga el ultimo snapshot de inventario del parquet como estado base.
    # Primera ejecucion: procesa el parquet y guarda cache pickle.
    # Ejecuciones posteriores: carga el cache en segundos.
    import pickle

    PARQUET_FILE = BASE_DIR / 'data' / 'processed' / 'datos_limpios.parquet'
    CACHE_FILE   = OUTPUT_DIR / 'estado_inicial_cache.pkl'
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not PARQUET_FILE.exists():
        print('Parquet no encontrado. El consumer arrancara sin stock inicial.')
        return

    # -- Usar cache si existe y es mas reciente que el parquet ----------------
    if CACHE_FILE.exists():
        if CACHE_FILE.stat().st_mtime >= PARQUET_FILE.stat().st_mtime:
            print('Cargando estado inicial desde cache (rapido)...')
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
            for k, v in cache['stock'].items():
                estado.stock[k] = v
            estado.precios.update(cache['precios'])
            estado.metadata.update(cache['metadata'])
            print(f'  {len(cache["stock"]):,} SKUs cargados desde cache')
            print(f'  Stock total : {sum(cache["stock"].values()):,} uds')
            return

    # -- Primera vez: procesar parquet y guardar cache ------------------------
    print('Primera ejecucion: procesando parquet y generando cache...')
    print('(Las proximas ejecuciones arrancan en segundos)')

    COLS = ['TIPO_TRANSACCION', 'FECHA', 'PAIS', 'CODIGO_SKU',
            'FAMILIA', 'MODELO', 'MARCA', 'TIER', 'ESTADO',
            'CODIGO_BODEGA', 'VALUE', 'PRECIO_UNITARIO']

    df = pd.read_parquet(PARQUET_FILE, columns=COLS)
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype(str)
    df['ESTADO']           = df['ESTADO'].astype(str).str.upper().str.strip()
    df['FECHA']            = pd.to_datetime(df['FECHA'])

    inv = df[df['TIPO_TRANSACCION'] == 'INVENTORY'].copy()
    del df

    ultimo_snapshot = (
        inv.sort_values('FECHA')
           .groupby(['PAIS', 'CODIGO_SKU', 'CODIGO_BODEGA'], as_index=False)
           .last()
    )
    del inv

    stock_cache    = {}
    precios_cache  = {}
    metadata_cache = {}

    for _, row in ultimo_snapshot.iterrows():
        clave = estado.clave(str(row['PAIS']), str(row['CODIGO_SKU']))
        stock_cache[clave] = stock_cache.get(clave, 0) + int(row['VALUE'])

        if clave not in metadata_cache:
            metadata_cache[clave] = {
                'pais'    : str(row['PAIS']),
                'sku'     : str(row['CODIGO_SKU']),
                'familia' : str(row.get('FAMILIA', '')),
                'modelo'  : str(row.get('MODELO', '')),
                'marca'   : str(row.get('MARCA', '')),
                'tier'    : str(row.get('TIER', '')),
            }

        precio = row.get('PRECIO_UNITARIO', 0)
        if str(precio) not in ('nan', '0'):
            precios_cache[clave] = float(precio)

    # Volcar en el estado
    for k, v in stock_cache.items():
        estado.stock[k] = v
    estado.precios.update(precios_cache)
    estado.metadata.update(metadata_cache)

    # Guardar cache para proximas ejecuciones
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump({'stock': stock_cache, 'precios': precios_cache, 'metadata': metadata_cache}, f)

    print(f'  Cache guardado en {CACHE_FILE}')
    print(f'  Stock inicial: {len(stock_cache):,} SKUs')
    print(f'  Stock total  : {sum(stock_cache.values()):,} uds')


def main() -> None:
    print('=' * 65)
    print('FASE 4 -- STREAMING CONSUMER')
    print('=' * 65)
    print(f'Topic          : {TOPIC_FILE}')
    print(f'Intervalo      : {INTERVALO_SEGUNDOS}s')
    print(f'Tamano ventana : {TAMANO_VENTANA} eventos')
    print('Esperando eventos... (Ctrl+C para detener)')
    print()

    # -- Cargar modelo --------------------------------------------------------
    modelo = None
    if MODEL_FILE.exists():
        modelo = joblib.load(MODEL_FILE)
        print(f'Modelo XGBoost cargado desde {MODEL_FILE}')
    else:
        print(f'Modelo no encontrado en {MODEL_FILE}')
        print('El consumer correra sin predicciones. Corre fase3_modelos.py primero.')

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Limpiar archivos de ejecuciones anteriores
    for archivo in [
        'kpis_tiempo_real.csv',
        'alertas.csv',
        'predicciones_stream.csv',
        'ordenes_sugeridas.csv',
    ]:
        ruta = OUTPUT_DIR / archivo
        if ruta.exists():
            ruta.unlink()
            print(f'  Limpiado: {archivo}')

    estado = EstadoStream()

    # -- Cargar stock inicial desde el parquet --------------------------------
    cargar_stock_inicial(estado)

    try:
        while True:
            estado.n_ventana += 1

            # -- Leer eventos nuevos del topic --------------------------------
            eventos_nuevos = leer_eventos_nuevos(estado)

            if not eventos_nuevos:
                print(f'  Ventana {estado.n_ventana:03d} | Sin eventos nuevos, esperando...')
                time.sleep(INTERVALO_SEGUNDOS)

                if MAX_VENTANAS and estado.n_ventana >= MAX_VENTANAS:
                    break
                continue

            # -- Actualizar estado con los nuevos eventos ---------------------
            for evento in eventos_nuevos:
                estado.procesar_evento(evento)

            # -- Calcular KPIs de la ventana ----------------------------------
            timestamp = datetime.now().isoformat()
            kpis      = calcular_kpis_ventana(estado, timestamp)
            alertas   = detectar_alertas(kpis, estado)

            # -- Prediccion con XGBoost ---------------------------------------
            predicciones = predecir_demanda(kpis, estado, modelo, timestamp)

            # -- Procesar transitos que llegan en esta ventana ---------------
            llegadas = procesar_transitos_llegados(estado, estado.n_ventana)
            if llegadas:
                print()
                for ll in llegadas:
                    print(f'  >> TRANSITO RECIBIDO: {ll["pais"]} | {ll["sku"]} | '
                          f'+{ll["cantidad"]} uds | stock nuevo: {ll["stock_nuevo"]}')

            # -- Ordenes de reposicion y sustitutos ---------------------------
            actualizar_inactividad(kpis, estado)
            ordenes = generar_ordenes_reposicion(
                alertas, predicciones, estado, timestamp, estado.n_ventana
            )
            estado.ordenes.extend(ordenes)

            # -- Acumular historico -------------------------------------------
            if not kpis.empty:
                estado.kpis_historico.extend(kpis.to_dict('records'))
            estado.alertas.extend(alertas)
            estado.predicciones.extend(predicciones)

            # -- Dashboard ----------------------------------------------------
            imprimir_dashboard(kpis, alertas, predicciones, len(eventos_nuevos), estado)

            # -- Guardar cada 5 ventanas --------------------------------------
            if estado.n_ventana % 5 == 0:
                guardar_resultados(estado)

            time.sleep(INTERVALO_SEGUNDOS)

            if MAX_VENTANAS and estado.n_ventana >= MAX_VENTANAS:
                print(f'\nMaximo de ventanas alcanzado ({MAX_VENTANAS}).')
                break

    except KeyboardInterrupt:
        print('\n\nConsumer detenido por el usuario.')

    finally:
        guardar_resultados(estado)
        print()
        print('=' * 65)
        print('CONSUMER FINALIZADO')
        print(f'Total ventanas procesadas : {estado.n_ventana}')
        print(f'Total eventos procesados  : {estado.lineas_procesadas}')
        print(f'Total alertas generadas   : {len(estado.alertas)}')
        print(f'Total predicciones        : {len(estado.predicciones)}')
        print('=' * 65)


if __name__ == '__main__':
    main()
