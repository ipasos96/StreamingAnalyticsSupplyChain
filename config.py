"""
=============================================================
config.py — Configuración central del proyecto
TFM: Streaming Analytics para Supply Chain - Telecom
Autora: Iliana Yazmin Pasos Gallo
=============================================================
"""

import os
from pathlib import Path

# =============================================================
# RAÍZ DEL PROYECTO
# =============================================================
BASE_DIR = Path(__file__).resolve().parent

# =============================================================
# ESTRUCTURA DE CARPETAS
# =============================================================
RUTAS = {
    # Datos fuente (CSV originales por país)
    "raw"           : BASE_DIR / "data" / "raw",
    # Dataset limpio y enriquecido (output Fase 1)
    "processed"     : BASE_DIR / "data" / "processed",
    # Catálogos de bodega y modelos por país
    "catalogs"     : BASE_DIR / "data" / "catalogs",
    # KPIs calculados (output Fase 2)
    "kpis"          : BASE_DIR / "outputs" / "kpis",
    # Modelos entrenados (.pkl, .json)
    "models"        : BASE_DIR / "outputs" / "models",
    # Archivos del dashboard
    "dashboard"     : BASE_DIR / "outputs" / "dashboard",
    # Notebooks de exploración
    "notebook"     : BASE_DIR / "notebook",
    # Scripts fuente
    "src"           : BASE_DIR / "src",
}

# Crear carpetas automáticamente si no existen
for ruta in RUTAS.values():
    os.makedirs(ruta, exist_ok=True)

# =============================================================
# LISTADO DE PAÍSES
# =============================================================
PAISES = ["PY","NI"]

# =============================================================
# CONVENCIÓN DE NOMBRES DE ARCHIVOS
# =============================================================
def get_ruta_raw(pais: str, tipo: str) -> Path:
    """
    Retorna la ruta del archivo fuente de un país.
    tipo: 'datos' | 'bodega' | 'devices'
    Ej: get_ruta_raw('PY', 'datos') → data/raw/Datos_PY.csv
    """
    nombres = {
        "datos"   : f"Datos_{pais}.csv",
        "bodega"  : f"Bodega_{pais}.csv",
        "devices" : f"Devices_{pais}.csv",
    }
    return RUTAS["raw"] / nombres[tipo]

def get_ruta_processed(pais: str) -> Path:
    """Retorna la ruta del dataset enriquecido de un país."""
    return RUTAS["processed"] / f"dataset_enriquecido_{pais}.csv"

def get_ruta_modelo(nombre: str) -> Path:
    """Retorna la ruta para guardar/cargar un modelo."""
    return RUTAS["models"] / nombre

# =============================================================
# PARÁMETROS GLOBALES
# =============================================================
PARAMS = {
    # Separador del CSV de datos
    "sep_datos"         : ",",

    # Tipos de transacción y su clasificación
    "tipos_transaccion" : {
        "SALES"             : "VENTA",
        "INVENTORY"         : "INVENTARIO_BODEGA",
        "CENTRAL INVENTORY" : "INVENTARIO_CENTRAL",
        "SERVICE LEVEL"     : "NIVEL_SERVICIO",
        "TRANSITS"          : "TRANSITO",
    },

    # Orden lógico de tiers para reportes y modelos
    "tier_order"        : [
        "ENTRY", "LOW", "MID LOW", "MID", "MID HIGH",
        "HIGH", "FLAGSHIP", "ULTRA HIGH",
        "MODEMS", "CPE HOME", "CPE CORPO", "DATA CARDS",
    ],

    # Canales de venta
    "canales_venta"     : [
        "TIGO STORES", "TIGO STORE", "TELEVENTAS",
        "E-COMMERCE", "B2B", "DEALER", "CORPORATE",
    ],

    # Columnas finales del dataset enriquecido (Fase 1)
    "columnas_dataset"  : [
        "PAIS", "FECHA", "ANIO", "MES", "DIA",
        "TIPO_TRANSACCION", "TIPO_TX_ES",
        "CODIGO_SKU", "FAMILY", "MODEL_NAME", "BRAND", "TIER", "TIER_ORDEN", "STATUS",
        "CODIGO_BODEGA", "NOMBRE_BODEGA", "UBICACION", "TIPO_BODEGA",
        "VALUE", "COSTO", "VALOR_TOTAL",
    ],

    # Horizonte de forecast (meses hacia adelante)
    "forecast_horizon"  : 3,

    # Umbral para alerta de stockout (unidades mínimas)
    "umbral_stockout"   : 5,

    # Umbral para alerta de sobreinventario (días de cobertura)
    "umbral_overstock"  : 90,

    # Kafka
    "kafka_bootstrap"   : "localhost:9092",
    "kafka_topic"       : "supply-chain-eventos",

    # Spark
    "spark_app_name"    : "TFM_SupplyChain_Streaming",
    "spark_window_min"  : 5,   # ventana de agregación en minutos
}

# =============================================================
# COLUMNAS CLAVE (atajos para usar en scripts)
# =============================================================
COL_FECHA    = "FECHA"
COL_PAIS     = "PAIS"
COL_SKU      = "CODIGO_SKU"
COL_BODEGA   = "CODIGO_BODEGA"
COL_TIPO_TX  = "TIPO_TRANSACCION"
COL_VALUE    = "VALUE"
COL_COSTO    = "COSTO"
COL_VALOR    = "VALOR_TOTAL"
COL_TIER     = "TIER"
COL_BRAND    = "BRAND"
COL_CANAL    = "TIPO_BODEGA"


# =============================================================
if __name__ == "__main__":
    print("=== Configuración del proyecto ===\n")
    print(f"Directorio base : {BASE_DIR}")
    print(f"Países activos  : {PAISES}\n")
    print("Rutas del proyecto:")
    for k, v in RUTAS.items():
        existe = "Ok" if v.exists() else "Creando Carpetas..."
        print(f"  {k:<15} {existe}  {v}")
    print(f"\nKafka topic     : {PARAMS['kafka_topic']}")
    print(f"Forecast horizon: {PARAMS['forecast_horizon']} meses")
    print(f"Umbral stockout : {PARAMS['umbral_stockout']} unidades")
