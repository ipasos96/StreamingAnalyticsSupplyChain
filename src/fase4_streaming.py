from __future__ import annotations

"""
fase4_streaming.py  —  Producer
================================
Simula un flujo de eventos de inventario y ventas en tiempo real.

Combina datos reales del parquet con eventos sinteticos generados
a partir de la distribucion historica. Escribe los eventos en
stream_topic.jsonl, que actua como topic de Kafka simulado.

Uso:
    python src/fase4_streaming.py

Salidas:
    outputs/streaming/stream_topic.jsonl   <- topic simulado
"""

import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURACION
# ============================================================

BASE_DIR      = Path(__file__).resolve().parents[1]
DATA_PROC     = BASE_DIR / 'data' / 'processed'
OUTPUT_DIR    = BASE_DIR / 'outputs' / 'streaming'
TOPIC_FILE    = OUTPUT_DIR / 'stream_topic.jsonl'

DATA_FILE     = DATA_PROC / 'datos_limpios.parquet'

# Cuantos eventos reales por cada sintetico
RATIO_REAL_SINTETICO = 3

# Delay entre eventos en segundos (simula velocidad del stream)
DELAY_SEGUNDOS = 0.3

# Cuantos eventos totales publicar (None = todos)
MAX_EVENTOS = None

random.seed(42)
np.random.seed(42)


# ============================================================
# CARGA Y PREPARACION
# ============================================================

def cargar_datos() -> tuple[pd.DataFrame, pd.DataFrame]:
    # Leer solo las columnas necesarias para no saturar memoria
    COLS = [
        'TIPO_TRANSACCION', 'FECHA', 'PAIS', 'CODIGO_SKU',
        'FAMILIA', 'MODELO', 'MARCA', 'TIER', 'ESTADO',
        'CODIGO_BODEGA', 'VALUE', 'PRECIO_UNITARIO', 'COSTO',
    ]

    df = pd.read_parquet(DATA_FILE, columns=COLS)

    # Convertir a str para evitar problemas con string[python] del parquet
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype(str)
    df['PAIS']             = df['PAIS'].astype(str)
    df['FECHA']            = pd.to_datetime(df['FECHA'])

    paises = sorted(df['PAIS'].dropna().unique().tolist())

    # Separar sin .copy() — filtrar directamente y resetear index
    # Excluir SKUs EOL de las ventas — productos discontinuados no generan ventas
    df['ESTADO'] = df['ESTADO'].astype(str).str.upper().str.strip()
    ventas = (
        df[
            (df['TIPO_TRANSACCION'] == 'SALES') &
            (df['ESTADO'] != 'EOL')
        ]
        .reset_index(drop=True)
    )
    inventario = df[df['TIPO_TRANSACCION'] == 'INVENTORY'].reset_index(drop=True)

    skus_eol = df[df['ESTADO'] == 'EOL']['CODIGO_SKU'].nunique()
    print(f'SKUs EOL excluidos de ventas simuladas: {skus_eol}')

    # Liberar el df completo de memoria
    del df

    # El inventario es un snapshot diario — el mismo SKU aparece N veces
    # con el mismo valor. Nos quedamos solo con el ultimo registro por SKU x Bodega
    # que representa el stock actual real.
    inventario = (
        inventario
        .sort_values('FECHA')
        .groupby(['PAIS', 'CODIGO_SKU', 'CODIGO_BODEGA'], as_index=False)
        .last()
        .reset_index(drop=True)
    )
    print(f'Inventario reducido al ultimo snapshot: {len(inventario):,} registros unicos por SKU x Bodega.')

    print(f'Datos cargados: {len(ventas):,} ventas | {len(inventario):,} inventario')
    print(f'Paises: {paises}')
    return ventas, inventario


def preparar_catalogo(ventas: pd.DataFrame) -> pd.DataFrame:
    # SKUs unicos con sus atributos para generar sinteticos consistentes
    catalogo = (
        ventas
        .groupby(['CODIGO_SKU', 'PAIS'])
        .agg(
            FAMILIA     = ('FAMILIA',         'first'),
            MODELO      = ('MODELO',          'first'),
            MARCA       = ('MARCA',           'first'),
            TIER        = ('TIER',            'first'),
            ESTADO      = ('ESTADO',          'first'),
            PRECIO_MED  = ('PRECIO_UNITARIO', 'median'),
            PRECIO_STD  = ('PRECIO_UNITARIO', 'std'),
            VALUE_MED   = ('VALUE',           'median'),
            VALUE_STD   = ('VALUE',           'std'),
        )
        .reset_index()
        .fillna({'PRECIO_STD': 0, 'VALUE_STD': 0, 'ESTADO': 'ACTIVE'})
    )
    return catalogo


# ============================================================
# GENERACION DE EVENTOS
# ============================================================

def evento_desde_real(fila: pd.Series, tipo: str) -> dict:
    # Convierte una fila del parquet en un evento de stream
    return {
        'evento_id'       : f'REAL_{fila.name}',
        'timestamp'       : datetime.now().isoformat(),
        'fecha_original'  : str(fila['FECHA'].date()),
        'tipo_evento'     : tipo,
        'pais'            : str(fila['PAIS']),
        'codigo_sku'      : str(fila['CODIGO_SKU']),
        'familia'         : str(fila.get('FAMILIA', '')),
        'modelo'          : str(fila.get('MODELO', '')),
        'marca'           : str(fila.get('MARCA', '')),
        'tier'            : str(fila.get('TIER', '')),
        'estado'          : str(fila.get('ESTADO', 'ACTIVE')),
        'codigo_bodega'   : str(fila.get('CODIGO_BODEGA', '')),
        'value'           : int(fila['VALUE']),
        'precio_unitario' : round(float(fila.get('PRECIO_UNITARIO', 0)), 2),
        'costo'           : round(float(fila.get('COSTO', 0)), 2),
        'fuente'          : 'REAL',
    }


def evento_sintetico(catalogo: pd.DataFrame) -> dict:
    # Toma un SKU al azar y genera un evento con valores plausibles
    sku = catalogo.sample(1).iloc[0]

    estado_sku = str(sku.get('ESTADO', 'ACTIVE')).upper()

    # SKUs NEW generan más ventas (lanzamiento reciente = demanda alta)
    multiplicador_new = random.uniform(2.0, 3.0) if estado_sku == 'NEW' else 1.0

    value = max(1, int(np.random.normal(
        loc   = max(1, sku['VALUE_MED']) * multiplicador_new,
        scale = max(0.5, sku['VALUE_STD'])
    )))

    precio = max(1.0, float(np.random.normal(
        loc   = max(1, sku['PRECIO_MED']),
        scale = max(0.1, sku['PRECIO_STD'])
    )))

    # NEW venden más frecuentemente también
    peso_ventas = 0.7 if estado_sku == 'NEW' else 0.4
    tipo = random.choices(
        ['SALES', 'INVENTORY'],
        weights=[peso_ventas, 1 - peso_ventas]
    )[0]

    fecha_sim = datetime.now() - timedelta(days=random.randint(0, 30))

    return {
        'evento_id'       : f'SIM_{random.randint(100000, 999999)}',
        'timestamp'       : datetime.now().isoformat(),
        'fecha_original'  : fecha_sim.strftime('%Y-%m-%d'),
        'tipo_evento'     : tipo,
        'pais'            : str(sku['PAIS']),
        'codigo_sku'      : str(sku['CODIGO_SKU']),
        'familia'         : str(sku['FAMILIA']),
        'modelo'          : str(sku['MODELO']),
        'marca'           : str(sku['MARCA']),
        'tier'            : str(sku['TIER']),
        'estado'          : estado_sku,
        'codigo_bodega'   : f'BOD_{random.randint(1, 50)}',
        'value'           : value,
        'precio_unitario' : round(precio, 2),
        'costo'           : round(precio * random.uniform(0.4, 0.7), 2),
        'fuente'          : 'SINTETICO',
    }


def mezclar_eventos(
    ventas:     pd.DataFrame,
    inventario: pd.DataFrame,
    catalogo:   pd.DataFrame,
) -> list[dict]:
    # Toma una muestra de eventos reales
    n_reales = MAX_EVENTOS if MAX_EVENTOS else len(ventas) + len(inventario)
    n_ventas  = int(n_reales * 0.4)
    n_inv     = int(n_reales * 0.6)

    muestra_ventas = ventas.sample(min(n_ventas, len(ventas)))
    muestra_inv    = inventario.sample(min(n_inv, len(inventario)))

    eventos_reales = (
        [evento_desde_real(r, 'SALES')     for _, r in muestra_ventas.iterrows()] +
        [evento_desde_real(r, 'INVENTORY') for _, r in muestra_inv.iterrows()]
    )

    # Intercalar sinteticos cada RATIO_REAL_SINTETICO eventos reales
    eventos_mezclados = []
    for i, ev in enumerate(eventos_reales):
        eventos_mezclados.append(ev)
        if (i + 1) % RATIO_REAL_SINTETICO == 0:
            eventos_mezclados.append(evento_sintetico(catalogo))

    # Mezclar el orden para simular llegada no ordenada
    random.shuffle(eventos_mezclados)

    total_sint = len([e for e in eventos_mezclados if e['fuente'] == 'SINTETICO'])
    total_real = len([e for e in eventos_mezclados if e['fuente'] == 'REAL'])
    print(f'Eventos preparados: {total_real} reales + {total_sint} sinteticos = {len(eventos_mezclados)} total')
    return eventos_mezclados


# ============================================================
# PUBLICACION
# ============================================================

def publicar(eventos: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Limpiar el topic anterior
    if TOPIC_FILE.exists():
        TOPIC_FILE.unlink()

    print()
    print('=' * 60)
    print('PRODUCER INICIADO')
    print(f'Topic : {TOPIC_FILE}')
    print(f'Delay : {DELAY_SEGUNDOS}s por evento')
    print('=' * 60)
    print('Publicando eventos... (Ctrl+C para detener)')
    print()

    with open(TOPIC_FILE, 'w', encoding='utf-8') as f:
        for i, evento in enumerate(eventos, 1):
            # Actualizar timestamp al momento real de publicacion
            evento['timestamp'] = datetime.now().isoformat()

            linea = json.dumps(evento, ensure_ascii=False)
            f.write(linea + '\n')
            f.flush()  # importante: escribe inmediatamente para que el consumer lo lea

            fuente = evento['fuente'][:4]
            tipo   = evento['tipo_evento'][:3]
            pais   = evento['pais']
            sku    = evento['codigo_sku']
            val    = evento['value']

            print(f'  [{i:04d}] {fuente} | {tipo} | {pais} | {sku} | {val} uds')

            time.sleep(DELAY_SEGUNDOS)

    print()
    print('=' * 60)
    print(f'Producer finalizado. {len(eventos)} eventos publicados.')
    print('=' * 60)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print('=' * 60)
    print('FASE 4 -- STREAMING PRODUCER')
    print('=' * 60)

    ventas, inventario = cargar_datos()
    catalogo           = preparar_catalogo(ventas)
    eventos            = mezclar_eventos(ventas, inventario, catalogo)

    publicar(eventos)


if __name__ == '__main__':
    main()
