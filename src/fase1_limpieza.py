from __future__ import annotations

"""
fase1_limpieza.py
=================
Limpieza y enriquecimiento del dataset crudo de ventas / inventario Supply Chain.

Este script:
1. Carga automaticamente archivos transaccionales desde data/raw.
2. Carga catalogos multi-pais de devices y bodegas desde data/catalogs.
3. Estandariza nombres de columnas y tipos de datos.
4. Valida y parsea FECHA_VENTA_INVENTARIO en formato YYYYMMDD.
5. Filtra registros no utiles para el analisis.
6. Aplica corte de fecha: excluye desde 2025-12-15 (diciembre sin datos completos).
7. Trata nulos, inconsistencias y duplicados.
8. Rellena dias sin registros de INVENTORY mediante forward fill por SKU x Bodega x Pais.
9. Recalcula PRECIO_UNITARIO a partir de COSTO / VALUE.
10. Controla outliers de PRECIO_UNITARIO con una regla robusta (IQR).
    Agrega columna ES_OUTLIER=True para registros fuera de limites (no se eliminan).
11. Enriquece el transaccional con catalogos usando llaves compuestas por pais.
12. Genera un dataset procesado y un reporte de calidad.

Entradas esperadas:
- data/raw/               -> archivos transaccionales por pais
- data/catalogs/          -> catalogos de devices y bodegas por pais

Salidas:
- data/processed/datos_limpios.parquet
- data/resumen/resumen_limpieza.csv
- data/resumen/reporte_calidad_limpieza.xlsx
"""

from pathlib import Path
import re

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================
BASE_DIR = Path(__file__).resolve().parents[1]
PATH_RAW = BASE_DIR / "data" / "raw"
PATH_CATALOGS = BASE_DIR / "data" / "catalogs"
PATH_PROCESSED = BASE_DIR / "data" / "processed"
PATH_RESUMEN = BASE_DIR / "data" / "resumen"

PATH_PROCESSED.mkdir(parents=True, exist_ok=True)
PATH_RESUMEN.mkdir(parents=True, exist_ok=True)

OUTPUT_PARQUET = PATH_PROCESSED / "datos_limpios.parquet"
OUTPUT_RESUMEN = PATH_RESUMEN / "resumen_limpieza.csv"
OUTPUT_REPORTE = PATH_RESUMEN / "reporte_calidad_limpieza.xlsx"

TIPOS_UTILES = {"SALES", "INVENTORY", "TRANSITS"}
PRECIO_MIN = 1.0
FECHA_MIN = 20210101
IQR_FACTOR = 1.5

# Diciembre no tiene informacion completa en todos los paises.
# Se excluyen registros desde esta fecha en adelante para evitar sesgo por datos parciales.
FECHA_CORTE = pd.Timestamp("2025-12-15")

SHEET_PRIORITY = ["ventas", "sales", "data", "datos", "sheet1", "hoja1"]

# Pistas para identificar catálogos por nombre de archivo.
DEVICE_NAME_HINTS = ["device", "devices", "sku", "catalogo_sku", "catalogo_devices", "maestro_sku"]
BODEGA_NAME_HINTS = ["bodega", "bodegas", "warehouse", "warehouses", "canal", "canales", "catalogo_bodega", "maestro_bodega"]

# Mapeo de columnas a nombres estándar.
ALIASES = {
    # fecha
    "FECHA": "FECHA_VENTA_INVENTARIO",
    "DATE": "FECHA_VENTA_INVENTARIO",
    "FECHA_VENTA": "FECHA_VENTA_INVENTARIO",
    "FECHA_INVENTARIO": "FECHA_VENTA_INVENTARIO",
    "FECHA_VENTA_INVENTARIO": "FECHA_VENTA_INVENTARIO",
    # transacción
    "TIPO_TRANSACCION": "TIPO_TRANSACCION",
    "TIPO_DE_TRANSACCION": "TIPO_TRANSACCION",
    "TRANSACCION": "TIPO_TRANSACCION",
    # métricas
    "VALUE": "VALUE",
    "UNIDADES": "VALUE",
    "UNITS": "VALUE",
    "CANTIDAD": "VALUE",
    "COSTO": "COSTO",
    "COST": "COSTO",
    "PRECIO_UNITARIO": "PRECIO_UNITARIO",
    "PRECIO": "PRECIO_UNITARIO",
    "UNIT_PRICE": "PRECIO_UNITARIO",
    # producto
    "CODIGO_SKU": "CODIGO_SKU",
    "COD_SKU": "CODIGO_SKU",
    "SKU": "CODIGO_SKU",
    "SKU_CODE": "CODIGO_SKU",
    "FAMILY": "FAMILIA",
    "FAMILIA": "FAMILIA",
    "MODEL_NAME": "MODELO",
    "MODEL": "MODELO",
    "MODELO": "MODELO",
    "BRAND": "MARCA",
    "MARCA": "MARCA",
    "TIER": "TIER",
    "STATUS": "ESTADO",
    "ESTADO": "ESTADO",
    # bodega
    "CODIGO_BODEGA": "CODIGO_BODEGA",
    "COD_BODEGA": "CODIGO_BODEGA",
    "BODEGA": "CODIGO_BODEGA",
    "WAREHOUSE": "CODIGO_BODEGA",
    "WAREHOUSE_ID": "CODIGO_BODEGA",
    "NOMBRE_BODEGA": "NOMBRE_BODEGA",
    "UBICACION": "UBICACION",
    "TIPO_BODEGA": "TIPO_BODEGA",
    # país / canal
    "PAIS": "PAIS",
    "COUNTRY": "PAIS",
    "COUNTRY_NAME": "PAIS",
    "CANAL": "CANAL",
    "CHANNEL": "CANAL",
    "CANAL_VENTA": "CANAL",
    "ANOMES": "ANOMES",
}

EXPECTED_TRANSACTION_COLS = [
    "PAIS",
    "FECHA_VENTA_INVENTARIO",
    "ANOMES",
    "TIPO_TRANSACCION",
    "CODIGO_SKU",
    "CODIGO_BODEGA",
    "VALUE",
    "COSTO"
]

EXPECTED_DEVICE_COLS = ["PAIS", "CODIGO_SKU", "FAMILIA", "MODELO", "MARCA", "TIER", "ESTADO"]
EXPECTED_BODEGA_COLS = ["PAIS", "CODIGO_BODEGA", "NOMBRE_BODEGA", "UBICACION", "TIPO_BODEGA"]


# ============================================================
# UTILIDADES
# ============================================================
def normalizar_nombre_columna(texto: str) -> str:
    texto = str(texto).strip()
    texto = re.sub(r"\s+", "_", texto)
    texto = re.sub(r"[^A-Za-z0-9_]+", "", texto)
    texto = re.sub(r"_+", "_", texto)
    texto = texto.upper().strip("_")
    return ALIASES.get(texto, texto)



def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    nuevas = {}
    usados = set()

    for col in df.columns:
        base = normalizar_nombre_columna(col)
        target = base
        i = 2
        while target in usados:
            target = f"{base}_{i}"
            i += 1
        nuevas[col] = target
        usados.add(target)

    return df.rename(columns=nuevas)



def normalizar_texto(serie: pd.Series) -> pd.Series:
    return (
        serie.astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.upper()
    )



def inferir_pais_desde_nombre(file_path: Path) -> str | None:
    nombre = file_path.stem.upper()
    tokens = re.split(r"[_\-\s]+", nombre)
    compactos = [t.replace(" ", "") for t in tokens if t]

    known = {
        "PANAMA": "PANAMA",
        "HONDURAS": "HONDURAS",
        "NICARAGUA": "NICARAGUA",
        "PARAGUAY": "PARAGUAY",
        "COLOMBIA": "COLOMBIA",
        "ELSALVADOR": "EL SALVADOR",
        "COSTARICA": "COSTA RICA",
        "HN": "HONDURAS",
        "SV": "EL SALVADOR",
        "PA": "PANAMA",
        "PY": "PARAGUAY",
        "NI": "NICARAGUA",
        "CO": "COLOMBIA",
        "CR": "COSTA RICA",
    }

    for token in compactos:
        if token in known:
            return known[token]
    return None



def pick_sheet(file_path: Path) -> str | int | None:
    xls = pd.ExcelFile(file_path)
    sheet_map = {s.lower(): s for s in xls.sheet_names}
    for candidate in SHEET_PRIORITY:
        if candidate.lower() in sheet_map:
            return sheet_map[candidate.lower()]
    return xls.sheet_names[0] if xls.sheet_names else None



def read_csv_robust(file_path: Path) -> pd.DataFrame:
    last_error = None
    for sep in [";", ",", "|", "\t"]:
        for encoding in [None, "latin-1"]:
            try:
                kwargs = {"sep": sep, "engine": "python"}
                if encoding:
                    kwargs["encoding"] = encoding
                df = pd.read_csv(file_path, **kwargs)
                if df.shape[1] > 1:
                    return df
            except Exception as exc:
                last_error = exc

    try:
        return pd.read_csv(file_path, sep=None, engine="python")
    except Exception as exc:
        last_error = exc
        raise ValueError(f"No se pudo detectar el separador de {file_path.name}: {last_error}")



def leer_archivo(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix in {".xlsx", ".xls"}:
        sheet = pick_sheet(file_path)
        if sheet is None:
            raise ValueError(f"No se encontró ninguna hoja en {file_path.name}")
        df = pd.read_excel(file_path, sheet_name=sheet)
    elif suffix == ".csv":
        df = read_csv_robust(file_path)
    elif suffix == ".parquet":
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Formato no soportado: {suffix}")

    df = normalizar_columnas(df)
    df["SOURCE_FILE"] = file_path.name
    df["SOURCE_COUNTRY"] = inferir_pais_desde_nombre(file_path)
    return df



def detectar_archivos(path_base: Path, hints: list[str] | None = None) -> list[Path]:
    patrones = ["*.csv", "*.xlsx", "*.xls", "*.parquet"]
    archivos: list[Path] = []
    for patron in patrones:
        archivos.extend(path_base.rglob(patron))

    if hints is None:
        return sorted(archivos)

    encontrados = []
    for fp in archivos:
        nombre = fp.stem.lower()
        if any(hint in nombre for hint in hints):
            encontrados.append(fp)
    return sorted(encontrados)


# ============================================================
# CARGA
# ============================================================
def cargar_transaccional() -> pd.DataFrame:
    archivos = detectar_archivos(PATH_RAW)
    if not archivos:
        raise FileNotFoundError(f"No encontré archivos en {PATH_RAW}.")

    dfs = []
    for file_path in archivos:
        try:
            df = leer_archivo(file_path)
            dfs.append(df)
            print(f"[0] OK transaccional -> {file_path.name}: {len(df):,} filas")
        except Exception as exc:
            print(f"[0] WARN transaccional -> No se pudo leer {file_path.name}: {exc}")

    if not dfs:
        raise ValueError("No se pudo cargar ningún archivo transaccional válido.")

    datos = pd.concat(dfs, ignore_index=True, sort=False)
    for col in EXPECTED_TRANSACTION_COLS:
        if col not in datos.columns:
            datos[col] = pd.NA

    print(f"\n[0] Total transaccional: {len(datos):,} filas | {datos.shape[1]} columnas")
    return datos



def cargar_catalogo(paths: list[Path], expected_cols: list[str], label: str) -> pd.DataFrame:
    if not paths:
        print(f"[CAT] No encontré catálogo de {label} en {PATH_CATALOGS}")
        return pd.DataFrame(columns=expected_cols)

    dfs = []
    for file_path in paths:
        try:
            df = leer_archivo(file_path)
            dfs.append(df)
            print(f"[CAT] OK {label} -> {file_path.name}: {len(df):,} filas")
        except Exception as exc:
            print(f"[CAT] WARN {label} -> {file_path.name}: {exc}")

    if not dfs:
        return pd.DataFrame(columns=expected_cols)

    cat = pd.concat(dfs, ignore_index=True, sort=False)
    for col in expected_cols:
        if col not in cat.columns:
            cat[col] = pd.NA
    return cat


# ============================================================
# LIMPIEZA BASE
# ============================================================
def tipar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    cols_texto = [
        "PAIS", "TIPO_TRANSACCION", "CODIGO_SKU", "CODIGO_BODEGA", "CANAL",
        "MARCA", "TIER", "MODELO", "FAMILIA", "ESTADO", "NOMBRE_BODEGA",
        "UBICACION", "TIPO_BODEGA", "SOURCE_FILE", "SOURCE_COUNTRY"
    ]
    for col in cols_texto:
        if col in df.columns:
            df[col] = normalizar_texto(df[col])
            df[col] = df[col].replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})

    cols_numericas = [
        "VALUE", "COSTO", "PRECIO_UNITARIO", "FECHA_VENTA_INVENTARIO", "ANOMES"
    ]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "PAIS" in df.columns:
        df["PAIS"] = df["PAIS"].fillna(df.get("SOURCE_COUNTRY"))

    return df



def filtrar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df[df["TIPO_TRANSACCION"].isin(TIPOS_UTILES)].copy()
    print(f"[1] Tipos útiles: -{antes - len(df):,} filas | quedan {len(df):,}")
    return df



def eliminar_nulos_criticos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.dropna(subset=["PAIS", "CODIGO_SKU", "CODIGO_BODEGA", "VALUE", "COSTO", "FECHA_VENTA_INVENTARIO"])
    df = df[(df["VALUE"] > 0) & (df["COSTO"] > 0)].copy()
    print(f"[2] Nulos / ceros críticos: -{antes - len(df):,} filas | quedan {len(df):,}")
    return df



def recalcular_precio(df: pd.DataFrame) -> pd.DataFrame:
    df["PRECIO_UNITARIO"] = df["COSTO"] / df["VALUE"]
    antes = len(df)
    df = df[df["PRECIO_UNITARIO"].notna() & (df["PRECIO_UNITARIO"] > PRECIO_MIN)].copy()
    print(f"[3] PRECIO_UNITARIO <= {PRECIO_MIN}: -{antes - len(df):,} filas | quedan {len(df):,}")
    return df



def parsear_fecha(df: pd.DataFrame) -> pd.DataFrame:
    df["FECHA_VENTA_INVENTARIO"] = pd.to_numeric(df["FECHA_VENTA_INVENTARIO"], errors="coerce")
    df["FECHA"] = pd.to_datetime(
        df["FECHA_VENTA_INVENTARIO"].astype("Int64").astype("string"),
        format="%Y%m%d",
        errors="coerce",
    )

    antes = len(df)
    df = df.dropna(subset=["FECHA"]).copy()
    print(f"[4a] Fechas no parseables: -{antes - len(df):,}")

    antes = len(df)
    df = df[df["FECHA_VENTA_INVENTARIO"] >= FECHA_MIN].copy()
    print(f"[4b] Fechas anteriores a {FECHA_MIN}: -{antes - len(df):,} | quedan {len(df):,}")

    df["ANIO"] = df["FECHA"].dt.year
    df["MES"] = df["FECHA"].dt.month
    df["MES_NOMBRE"] = df["FECHA"].dt.month_name()
    df["YEAR_MONTH"] = df["FECHA"].dt.to_period("M").astype("string")
    return df



def eliminar_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    subset_cols = [
        col for col in [
            "PAIS", "FECHA_VENTA_INVENTARIO", "TIPO_TRANSACCION", "CODIGO_SKU",
            "CODIGO_BODEGA", "VALUE", "COSTO"
        ] if col in df.columns
    ]
    df = df.drop_duplicates(subset=subset_cols).copy()
    print(f"[5] Duplicados eliminados: -{antes - len(df):,} | quedan {len(df):,}")
    return df



def aplicar_corte_fecha(df: pd.DataFrame) -> pd.DataFrame:
    # Diciembre no tiene informacion completa en todos los paises.
    # Se excluyen registros desde FECHA_CORTE en adelante.
    antes = len(df)
    df = df[df["FECHA"] < FECHA_CORTE].copy()
    print(f"[5b] Corte {FECHA_CORTE.date()}: -{antes - len(df):,} filas | quedan {len(df):,}")
    return df



def rellenar_dias_sin_inventario(df: pd.DataFrame) -> pd.DataFrame:
    # Los dias sin registros de inventario corresponden a errores operativos
    # (fines de semana, cortes de sistema). Se imputa con el ultimo valor
    # conocido por SKU x Bodega x Pais
    inv = df[df["TIPO_TRANSACCION"] == "INVENTORY"].copy()

    if inv.empty:
        print("[5c] Sin registros de INVENTORY para rellenar.")
        return df

    # Construir grid completo de fechas por SKU x Bodega x Pais
    fecha_min = inv["FECHA"].min()
    fecha_max = inv["FECHA"].max()
    rango     = pd.date_range(start=fecha_min, end=fecha_max, freq="D")

    inv_diario = (
        inv
        .groupby(["FECHA", "PAIS", "CODIGO_SKU", "CODIGO_BODEGA"])["VALUE"]
        .sum()
        .reset_index()
    )

    claves = inv_diario[["PAIS", "CODIGO_SKU", "CODIGO_BODEGA"]].drop_duplicates()
    fechas = pd.DataFrame({"FECHA": rango})

    # Cross join claves x fechas
    grid = claves.merge(fechas, how="cross")

    inv_completo = grid.merge(
        inv_diario, on=["FECHA", "PAIS", "CODIGO_SKU", "CODIGO_BODEGA"], how="left"
    )

    # Forward fill por grupo
    inv_completo = inv_completo.sort_values(["PAIS", "CODIGO_SKU", "CODIGO_BODEGA", "FECHA"])
    inv_completo["VALUE"] = (
        inv_completo
        .groupby(["PAIS", "CODIGO_SKU", "CODIGO_BODEGA"])["VALUE"]
        .transform(lambda x: x.ffill())
    )
    inv_completo["VALUE"] = inv_completo["VALUE"].fillna(0).astype(int)

    dias_rellenados = int(grid.shape[0] - inv_diario.shape[0])
    print(f"[5c] Forward fill inventario: {dias_rellenados:,} dias rellenados con valor anterior")

    # Completar columnas necesarias para el parquet final
    # Tomar una fila de referencia por clave para heredar TIPO_TRANSACCION, COSTO, etc.
    referencia = (
        inv
        .sort_values("FECHA")
        .drop_duplicates(subset=["PAIS", "CODIGO_SKU", "CODIGO_BODEGA"], keep="first")
        [["PAIS", "CODIGO_SKU", "CODIGO_BODEGA", "COSTO", "PRECIO_UNITARIO",
          "TIPO_TRANSACCION", "ANOMES", "SOURCE_FILE", "SOURCE_COUNTRY"]]
    )
    inv_completo = inv_completo.merge(
        referencia, on=["PAIS", "CODIGO_SKU", "CODIGO_BODEGA"], how="left"
    )

    # Recalcular columnas de fecha
    inv_completo["FECHA_VENTA_INVENTARIO"] = inv_completo["FECHA"].dt.strftime("%Y%m%d").astype(int)
    inv_completo["ANIO"]       = inv_completo["FECHA"].dt.year
    inv_completo["MES"]        = inv_completo["FECHA"].dt.month
    inv_completo["MES_NOMBRE"] = inv_completo["FECHA"].dt.month_name()
    inv_completo["YEAR_MONTH"] = inv_completo["FECHA"].dt.to_period("M").astype("string")
    inv_completo["ANOMES"]     = (inv_completo["ANIO"] * 100 + inv_completo["MES"])

    # Reemplazar los registros de INVENTORY en df con el grid completo
    df_sin_inv    = df[df["TIPO_TRANSACCION"] != "INVENTORY"].copy()
    df_con_inv    = inv_completo[[c for c in df.columns if c in inv_completo.columns]]
    df_resultado  = pd.concat([df_sin_inv, df_con_inv], ignore_index=True)

    print(f"    Total final tras forward fill: {len(df_resultado):,} filas")
    return df_resultado



def tratar_outliers_precio(df: pd.DataFrame) -> pd.DataFrame:
    total_caps = 0

    # Marcar outliers antes del capping para conservar la informacion
    df["ES_OUTLIER"] = False

    for tipo in sorted(TIPOS_UTILES):
        mask = df["TIPO_TRANSACCION"] == tipo
        serie = df.loc[mask, "PRECIO_UNITARIO"]

        if serie.notna().sum() < 5:
            print(f"    {tipo}: sin suficientes datos para IQR")
            continue

        q1 = serie.quantile(0.25)
        q3 = serie.quantile(0.75)
        iqr = q3 - q1

        if pd.isna(iqr) or iqr == 0:
            print(f"    {tipo}: IQR=0, sin capping")
            continue

        lim_inf = max(q1 - IQR_FACTOR * iqr, 0)
        lim_sup = q3 + IQR_FACTOR * iqr
        caps_inf = int((serie < lim_inf).sum())
        caps_sup = int((serie > lim_sup).sum())
        total_caps += caps_inf + caps_sup

        # Marcar como outlier antes de capear
        df.loc[mask & ((df["PRECIO_UNITARIO"] < lim_inf) | (df["PRECIO_UNITARIO"] > lim_sup)), "ES_OUTLIER"] = True

        df.loc[mask, "PRECIO_UNITARIO"] = serie.clip(lower=lim_inf, upper=lim_sup)
        print(f"    {tipo}: limites [{lim_inf:.2f}, {lim_sup:.2f}] | caps inf={caps_inf:,} sup={caps_sup:,}")

    print(f"[6] Outliers capados en PRECIO_UNITARIO: {total_caps:,} | flag ES_OUTLIER agregado")
    return df



def agregar_metricas_derivadas(df: pd.DataFrame) -> pd.DataFrame:
    df["INGRESO_TOTAL"] = df["VALUE"] * df["PRECIO_UNITARIO"]
    df["MARGEN_BRUTO"] = df["INGRESO_TOTAL"] - df["COSTO"]
    df["MARGEN_PCT"] = np.where(df["INGRESO_TOTAL"] > 0, df["MARGEN_BRUTO"] / df["INGRESO_TOTAL"], np.nan)
    return df


# ============================================================
# CATÁLOGOS Y ENRIQUECIMIENTO
# ============================================================
def preparar_catalogo_devices(df_devices: pd.DataFrame) -> pd.DataFrame:
    if df_devices.empty:
        return pd.DataFrame(columns=EXPECTED_DEVICE_COLS)

    df_devices = normalizar_columnas(df_devices).copy()
    for col in EXPECTED_DEVICE_COLS:
        if col not in df_devices.columns:
            df_devices[col] = pd.NA

    for col in EXPECTED_DEVICE_COLS:
        df_devices[col] = normalizar_texto(df_devices[col])
        df_devices[col] = df_devices[col].replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})

    df_devices["PAIS"] = df_devices["PAIS"].fillna(df_devices.get("SOURCE_COUNTRY"))
    df_devices = df_devices.dropna(subset=["PAIS", "CODIGO_SKU"]).copy()
    df_devices = df_devices.drop_duplicates(subset=["PAIS", "CODIGO_SKU"], keep="first")
    return df_devices[EXPECTED_DEVICE_COLS]



def preparar_catalogo_bodegas(df_bodegas: pd.DataFrame) -> pd.DataFrame:
    if df_bodegas.empty:
        return pd.DataFrame(columns=EXPECTED_BODEGA_COLS)

    df_bodegas = normalizar_columnas(df_bodegas).copy()
    for col in EXPECTED_BODEGA_COLS:
        if col not in df_bodegas.columns:
            df_bodegas[col] = pd.NA

    for col in EXPECTED_BODEGA_COLS:
        df_bodegas[col] = normalizar_texto(df_bodegas[col])
        df_bodegas[col] = df_bodegas[col].replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})

    df_bodegas["PAIS"] = df_bodegas["PAIS"].fillna(df_bodegas.get("SOURCE_COUNTRY"))
    df_bodegas = df_bodegas.dropna(subset=["PAIS", "CODIGO_BODEGA"]).copy()
    df_bodegas = df_bodegas.drop_duplicates(subset=["PAIS", "CODIGO_BODEGA"], keep="first")
    return df_bodegas[EXPECTED_BODEGA_COLS]



def enriquecer_con_catalogos(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    device_files = detectar_archivos(PATH_CATALOGS, DEVICE_NAME_HINTS)
    bodega_files = detectar_archivos(PATH_CATALOGS, BODEGA_NAME_HINTS)

    raw_devices = cargar_catalogo(device_files, EXPECTED_DEVICE_COLS, "devices")
    raw_bodegas = cargar_catalogo(bodega_files, EXPECTED_BODEGA_COLS, "bodegas")

    devices = preparar_catalogo_devices(raw_devices)
    bodegas = preparar_catalogo_bodegas(raw_bodegas)

    stats = {
        "devices_catalog_rows": len(devices),
        "bodegas_catalog_rows": len(bodegas),
    }

    df = df.copy()

    if not devices.empty:
        df = df.merge(devices, on=["PAIS", "CODIGO_SKU"], how="left", suffixes=("", "_DEV"))
        for col in ["FAMILIA", "MODELO", "MARCA", "TIER", "ESTADO"]:
            alt = f"{col}_DEV"
            if alt in df.columns:
                if col in df.columns:
                    df[col] = df[col].fillna(df[alt])
                    df.drop(columns=[alt], inplace=True)
                else:
                    df.rename(columns={alt: col}, inplace=True)

    if not bodegas.empty:
        df = df.merge(bodegas, on=["PAIS", "CODIGO_BODEGA"], how="left", suffixes=("", "_BOD"))
        for col in ["NOMBRE_BODEGA", "UBICACION", "TIPO_BODEGA"]:
            alt = f"{col}_BOD"
            if alt in df.columns:
                if col in df.columns:
                    df[col] = df[col].fillna(df[alt])
                    df.drop(columns=[alt], inplace=True)
                else:
                    df.rename(columns={alt: col}, inplace=True)

    stats["pct_match_devices"] = float(df["MODELO"].notna().mean()) if "MODELO" in df.columns and len(df) else 0.0
    stats["pct_match_bodegas"] = float(df["NOMBRE_BODEGA"].notna().mean()) if "NOMBRE_BODEGA" in df.columns and len(df) else 0.0
    return df, stats


# ============================================================
# SALIDAS Y REPORTES
# ============================================================
def guardar_salidas(df: pd.DataFrame, df_original: pd.DataFrame, enrichment_stats: dict) -> pd.DataFrame:
    cols_texto = [
        "PAIS", "TIPO_TRANSACCION", "CANAL", "CODIGO_SKU", "MODELO", "MARCA", "TIER",
        "FAMILIA", "ESTADO", "CODIGO_BODEGA", "NOMBRE_BODEGA", "UBICACION", "TIPO_BODEGA",
        "MES_NOMBRE", "YEAR_MONTH", "SOURCE_FILE", "SOURCE_COUNTRY"
    ]
    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].astype("string")

    cols_numericas = [
        "FECHA_VENTA_INVENTARIO", "ANOMES", "ANIO", "MES", "VALUE", "COSTO", "PRECIO_UNITARIO"
    ]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    orden_preferido = [
        "FECHA", "FECHA_VENTA_INVENTARIO", "ANIO", "MES", "MES_NOMBRE", "YEAR_MONTH", "ANOMES",
        "PAIS", "CANAL", "TIPO_TRANSACCION",
        "CODIGO_SKU", "FAMILIA", "MODELO", "MARCA", "TIER", "ESTADO",
        "CODIGO_BODEGA", "NOMBRE_BODEGA", "UBICACION", "TIPO_BODEGA",
        "VALUE", "PRECIO_UNITARIO", "INGRESO_TOTAL", "COSTO", 
        "SOURCE_FILE", "SOURCE_COUNTRY"
    ]
    columnas_existentes = [c for c in orden_preferido if c in df.columns]
    resto = [c for c in df.columns if c not in columnas_existentes]
    df = df[columnas_existentes + resto]

    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"\n[8] Parquet generado: {OUTPUT_PARQUET}")

    resumen = pd.DataFrame({
        "metrica": [
            "filas_originales", "filas_limpias", "filas_eliminadas", "pct_retenido",
            "tipos_transaccion", "paises_unicos", "skus_unicos", "bodegas_unicas",
            "fecha_min", "fecha_max", "catalogo_devices_filas", "catalogo_bodegas_filas",
            "pct_match_devices", "pct_match_bodegas"
        ],
        "valor": [
            len(df_original),
            len(df),
            len(df_original) - len(df),
            f"{(len(df) / len(df_original) * 100):.1f}%" if len(df_original) else "0.0%",
            ", ".join(sorted(df["TIPO_TRANSACCION"].dropna().astype(str).unique())) if "TIPO_TRANSACCION" in df.columns else "",
            df["PAIS"].nunique(dropna=True) if "PAIS" in df.columns else 0,
            df["CODIGO_SKU"].nunique(dropna=True) if "CODIGO_SKU" in df.columns else 0,
            df["CODIGO_BODEGA"].nunique(dropna=True) if "CODIGO_BODEGA" in df.columns else 0,
            df["FECHA"].min().date() if "FECHA" in df.columns and df["FECHA"].notna().any() else pd.NA,
            df["FECHA"].max().date() if "FECHA" in df.columns and df["FECHA"].notna().any() else pd.NA,
            enrichment_stats.get("devices_catalog_rows", 0),
            enrichment_stats.get("bodegas_catalog_rows", 0),
            f"{enrichment_stats.get('pct_match_devices', 0.0) * 100:.1f}%",
            f"{enrichment_stats.get('pct_match_bodegas', 0.0) * 100:.1f}%",
        ]
    })
    resumen.to_csv(OUTPUT_RESUMEN, index=False)
    print(f"[8] Resumen generado: {OUTPUT_RESUMEN}")

    faltantes = df.isna().sum().sort_values(ascending=False).reset_index()
    faltantes.columns = ["columna", "nulos"]
    faltantes["pct_nulos"] = np.where(len(df) > 0, faltantes["nulos"] / len(df), 0)

    por_pais = (
        df["PAIS"].fillna("SIN_PAIS").value_counts(dropna=False).rename_axis("PAIS").reset_index(name="REGISTROS")
        if "PAIS" in df.columns else pd.DataFrame()
    )
    por_tipo = (
        df["TIPO_TRANSACCION"].fillna("SIN_TIPO").value_counts(dropna=False).rename_axis("TIPO_TRANSACCION").reset_index(name="REGISTROS")
        if "TIPO_TRANSACCION" in df.columns else pd.DataFrame()
    )
    match_catalogos = pd.DataFrame({
        "metrica": ["pct_match_devices", "pct_match_bodegas"],
        "valor": [
            enrichment_stats.get("pct_match_devices", 0.0),
            enrichment_stats.get("pct_match_bodegas", 0.0),
        ]
    })

    with pd.ExcelWriter(OUTPUT_REPORTE, engine="openpyxl") as writer:
        resumen.to_excel(writer, sheet_name="resumen", index=False)
        faltantes.to_excel(writer, sheet_name="faltantes", index=False)
        por_pais.to_excel(writer, sheet_name="por_pais", index=False)
        por_tipo.to_excel(writer, sheet_name="por_tipo", index=False)
        match_catalogos.to_excel(writer, sheet_name="match_catalogos", index=False)

    print(f"[8] Reporte de calidad generado: {OUTPUT_REPORTE}")
    print("\nResumen final:")
    print(resumen.to_string(index=False))
    return resumen


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    print("=" * 65)
    print("FASE 1 -- LIMPIEZA DE DATOS + ENRIQUECIMIENTO MULTI-PAIS")
    print("=" * 65)

    datos = cargar_transaccional()
    df_original = datos.copy()

    datos = tipar_columnas(datos)
    datos = filtrar_tipos(datos)
    datos = eliminar_nulos_criticos(datos)
    datos = recalcular_precio(datos)
    datos = parsear_fecha(datos)
    datos = aplicar_corte_fecha(datos)
    datos = eliminar_duplicados(datos)
    datos = rellenar_dias_sin_inventario(datos)
    datos = tratar_outliers_precio(datos)
    #datos = agregar_metricas_derivadas(datos)

    datos, enrichment_stats = enriquecer_con_catalogos(datos)

    guardar_salidas(datos, df_original, enrichment_stats)

    print("\nArchivo generado exitosamente.")


if __name__ == "__main__":
    main()
