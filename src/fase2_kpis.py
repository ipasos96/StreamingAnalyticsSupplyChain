from __future__ import annotations

"""
fase2_kpis.py
=============
Calculo de KPIs de Supply Chain por pais y consolidado global.

Este script:
1. Carga el parquet limpio generado por fase1_limpieza.py.
2. Separa las transacciones en ventas e inventario.
3. Calcula KPIs de ventas (ASP, sell-through, crecimiento MoM/YoY).
4. Calcula KPIs de inventario (DOH, turnover, stockout rate, fill rate).
5. Calcula KPIs por marca, modelo y tier.
6. Ejecuta diagnostico de fill rate (dead stock / SKUs EOL).
7. Exporta resultados por pais en CSVs individuales.
8. Genera consolidados globales multi-pais.
9. Genera tabla ejecutiva global y reporte de calidad en Excel.

Entradas:
- data/processed/datos_limpios.parquet

Salidas:
- outputs/kpis/{PAIS}/          -> CSVs por pais
- outputs/kpis/global_*.csv     -> consolidados globales
- outputs/kpis/tabla_ejecutiva_global.csv
- outputs/kpis/reporte_kpis.xlsx
"""

from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURACIÓN
# ============================================================

BASE_DIR      = Path(__file__).resolve().parents[1]
DATA_DIR      = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR    = BASE_DIR / "outputs" / "kpis"

DATA_FILE = PROCESSED_DIR / "datos_limpios.parquet"

PAISES       = ["PY", "NI"]   # agrega aquí más países
TOP_N_MARCAS  = 8
TOP_N_MODELOS = 15


# ============================================================
# UTILIDADES
# ============================================================

def asegurar_directorio(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def obtener_dias_mes(year_month: str) -> int:
    periodo = pd.Period(str(year_month), freq="M")
    return periodo.days_in_month


def validar_columnas(df: pd.DataFrame, columnas: List[str], nombre_df: str) -> None:
    faltantes = [c for c in columnas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas en {nombre_df}: {faltantes}")


# ============================================================
# CARGA Y PREPARACIÓN
# ============================================================

# ============================================================
# CARGA SEPARADA POR TIPO — evita cargar 16M filas en memoria
# ============================================================

COLS_SALES = [
    "TIPO_TRANSACCION", "PAIS", "FECHA", "YEAR_MONTH", "ANIO", "MES",
    "VALUE", "PRECIO_UNITARIO", "COSTO", "CODIGO_SKU",
    "MARCA", "MODELO", "TIER", "ESTADO", "CODIGO_BODEGA",
]

COLS_INVENTORY = [
    "TIPO_TRANSACCION", "PAIS", "FECHA", "YEAR_MONTH",
    "VALUE", "CODIGO_SKU", "CODIGO_BODEGA",
    "MARCA", "MODELO", "TIER", "ESTADO",
]

def cargar_datos() -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No existe el archivo: {DATA_FILE}")

    # Leer solo las columnas necesarias para cada tipo
    # y filtrar en el mismo read_parquet con filters= (pushdown de Parquet)
    print("Cargando SALES...")
    sales = pd.read_parquet(
        DATA_FILE,
        columns=[c for c in COLS_SALES if c != "TIPO_TRANSACCION"],
        filters=[("TIPO_TRANSACCION", "==", "SALES")],
    )
    print(f"  SALES: {len(sales):,} filas")

    print("Cargando INVENTORY...")
    inventory = pd.read_parquet(
        DATA_FILE,
        columns=[c for c in COLS_INVENTORY if c != "TIPO_TRANSACCION"],
        filters=[("TIPO_TRANSACCION", "==", "INVENTORY")],
    )
    print(f"  INVENTORY: {len(inventory):,} filas")

    return sales, inventory


def preparar_sales(df_sales: pd.DataFrame) -> pd.DataFrame:
    columnas_requeridas = [
        "PAIS", "FECHA", "YEAR_MONTH", "ANIO", "MES",
        "VALUE", "PRECIO_UNITARIO", "CODIGO_SKU",
        "MARCA", "MODELO", "TIER",
    ]
    validar_columnas(df_sales, columnas_requeridas, "df_sales")

    df = df_sales.copy()
    df["FECHA"]          = pd.to_datetime(df["FECHA"], errors="coerce")
    df["YEAR_MONTH"]     = df["YEAR_MONTH"].astype(str)
    df["ANIO"]           = pd.to_numeric(df["ANIO"], errors="coerce")
    df["MES"]            = pd.to_numeric(df["MES"], errors="coerce")
    df["VALUE"]          = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0)
    df["PRECIO_UNITARIO"]= pd.to_numeric(df["PRECIO_UNITARIO"], errors="coerce").fillna(0)
    df["COSTO"]          = pd.to_numeric(df.get("COSTO"), errors="coerce").fillna(0) \
                           if "COSTO" in df.columns else np.nan
    df["INGRESO_TOTAL"]  = df["VALUE"] * df["PRECIO_UNITARIO"]
    return df


def preparar_inventory(df_inventory: pd.DataFrame) -> pd.DataFrame:
    columnas_requeridas = [
        "PAIS", "FECHA", "YEAR_MONTH", "VALUE",
        "CODIGO_SKU", "CODIGO_BODEGA", "MARCA", "MODELO", "TIER",
    ]
    validar_columnas(df_inventory, columnas_requeridas, "df_inventory")

    df = df_inventory.copy()
    if "ESTADO" not in df.columns:
        df["ESTADO"] = np.nan

    df["FECHA"]      = pd.to_datetime(df["FECHA"], errors="coerce")
    df["YEAR_MONTH"] = df["YEAR_MONTH"].astype(str)
    df["VALUE"]      = pd.to_numeric(df["VALUE"], errors="coerce").fillna(0)
    return df


# ============================================================
# KPIs DE VENTAS
# ============================================================

def calcular_kpis_ventas_generales(
    s: pd.DataFrame,
    iv: pd.DataFrame,
    pais: str
) -> pd.DataFrame:
    ventas_tot   = s["VALUE"].sum()
    ingresos_tot = s["INGRESO_TOTAL"].sum()
    asp          = ingresos_tot / ventas_tot if ventas_tot > 0 else np.nan
    inv_prom_g   = iv["VALUE"].mean() if len(iv) > 0 else np.nan

    sell_thru = (
        ventas_tot / (ventas_tot + inv_prom_g)
        if pd.notna(inv_prom_g) and inv_prom_g > 0
        else np.nan
    )

    return pd.DataFrame({
        "PAIS"  : [pais] * 4,
        "KPI"   : ["Unidades vendidas", "Ingresos totales", "ASP", "Sell-through Rate"],
        "VALOR" : [ventas_tot, ingresos_tot, asp, sell_thru],
        "UNIDAD": ["ud", "USD", "USD/ud", "%"],
    })


def calcular_kpis_ventas_mensuales(s: pd.DataFrame, pais: str) -> pd.DataFrame:
    vm = (
        s.groupby(["ANIO", "MES", "YEAR_MONTH"], dropna=False)
        .agg(
            UNIDADES     = ("VALUE",           "sum"),
            INGRESOS     = ("INGRESO_TOTAL",   "sum"),
            ASP          = ("PRECIO_UNITARIO", "mean"),
            SKUS_ACTIVOS = ("CODIGO_SKU",      "nunique"),
        )
        .reset_index()
        .sort_values("YEAR_MONTH")
    )
    vm["PAIS"]                      = pais
    vm["CRECIMIENTO_UNID_MOM"]      = vm["UNIDADES"].pct_change()
    vm["CRECIMIENTO_INGRESOS_MOM"]  = vm["INGRESOS"].pct_change()
    vm = vm.sort_values(["MES", "ANIO"])
    vm["CRECIMIENTO_INGRESOS_YOY"]  = vm.groupby("MES")["INGRESOS"].pct_change()
    vm["CRECIMIENTO_UNID_YOY"]      = vm.groupby("MES")["UNIDADES"].pct_change()
    return vm.sort_values("YEAR_MONTH")


def calcular_kpis_ventas_anuales(s: pd.DataFrame, pais: str) -> pd.DataFrame:
    va = (
        s.groupby("ANIO", dropna=False)
        .agg(
            UNIDADES = ("VALUE",         "sum"),
            INGRESOS = ("INGRESO_TOTAL", "sum"),
        )
        .reset_index()
        .sort_values("ANIO")
    )
    va["PAIS"]                     = pais
    va["CRECIMIENTO_INGRESOS_YOY"] = va["INGRESOS"].pct_change()
    va["CRECIMIENTO_UNID_YOY"]     = va["UNIDADES"].pct_change()
    return va


# ============================================================
# KPIs DE INVENTARIO  (optimizado)
# ============================================================

def construir_snapshot_inventario(iv: pd.DataFrame, pais: str) -> pd.DataFrame:
    inv_base = (
        iv.sort_values(["CODIGO_BODEGA", "CODIGO_SKU", "FECHA"])
        .groupby(["CODIGO_BODEGA", "CODIGO_SKU", "YEAR_MONTH"], dropna=False)
        .agg(
            INV_INICIAL = ("VALUE", "first"),
            INV_CIERRE  = ("VALUE", "last"),
            INV_PROM    = ("VALUE", "mean"),
        )
        .reset_index()
    )

    inv_mes = (
        inv_base.groupby("YEAR_MONTH", dropna=False)
        .agg(
            INV_INICIAL  = ("INV_INICIAL", "sum"),
            INV_CIERRE   = ("INV_CIERRE",  "sum"),
            INV_PROMEDIO = ("INV_PROM",    "sum"),
        )
        .reset_index()
    )

    inv_mes["PAIS"] = pais
    # OPTIMIZACION: vectorizado en lugar de apply fila a fila
    inv_mes["DIAS_MES"] = pd.to_datetime(
        inv_mes["YEAR_MONTH"].astype(str), format="%Y-%m"
    ).dt.days_in_month

    return inv_mes


def calcular_kpis_inventario(
    s: pd.DataFrame,
    iv: pd.DataFrame,
    pais: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    inv_mes = construir_snapshot_inventario(iv, pais)

    vmp = (
        s.groupby("YEAR_MONTH", dropna=False)
        .agg(VENTAS_UNIDADES=("VALUE", "sum"))
        .reset_index()
    )

    ki = vmp.merge(
        inv_mes[["YEAR_MONTH", "INV_INICIAL", "INV_CIERRE", "INV_PROMEDIO", "DIAS_MES"]],
        on="YEAR_MONTH",
        how="left",
    )
    ki["PAIS"]          = pais
    ki["DOH"]           = (ki["INV_PROMEDIO"] / ki["VENTAS_UNIDADES"].replace(0, np.nan)) * ki["DIAS_MES"]
    ki["STOCK_TURNOVER"]= ki["VENTAS_UNIDADES"] / ki["INV_PROMEDIO"].replace(0, np.nan)
    ki["SELL_THROUGH"]  = ki["VENTAS_UNIDADES"] / (
        ki["VENTAS_UNIDADES"] + ki["INV_CIERRE"].replace(0, np.nan)
    )

    # OPTIMIZACION: agg vectorizado — elimina el apply con lambda (el más lento)
    so = (
        iv.groupby(["CODIGO_SKU", "CODIGO_BODEGA"], dropna=False)
        .agg(
            DIAS_TOTALES  = ("FECHA", "nunique"),
            DIAS_STOCKOUT = ("VALUE", lambda x: int((x == 0).sum())),
        )
        .reset_index()
    )
    so["PAIS"]          = pais
    so["STOCKOUT_RATE"] = so["DIAS_STOCKOUT"] / so["DIAS_TOTALES"].replace(0, np.nan)

    # OPTIMIZACION: un solo merge en lugar de dos groupby separados
    f_i = (
        iv.groupby("CODIGO_SKU", dropna=False)
        .agg(DIAS_INVENTARIO=("FECHA", "nunique"))
        .reset_index()
    )
    f_s = (
        s.groupby("CODIGO_SKU", dropna=False)
        .agg(DIAS_CON_VENTAS=("FECHA", "nunique"))
        .reset_index()
    )
    fr = f_i.merge(f_s, on="CODIGO_SKU", how="left")
    fr["DIAS_CON_VENTAS"] = fr["DIAS_CON_VENTAS"].fillna(0)
    fr["PAIS"]            = pais
    fr["FILL_RATE"]       = fr["DIAS_CON_VENTAS"] / fr["DIAS_INVENTARIO"].replace(0, np.nan)

    return ki, so, fr, inv_mes


# ============================================================
# KPIs DE MARCA, MODELO Y TIER
# ============================================================

def calcular_kpis_marca(s: pd.DataFrame, pais: str) -> pd.DataFrame:
    km = (
        s.groupby("MARCA", dropna=False)
        .agg(
            UNIDADES = ("VALUE",           "sum"),
            INGRESOS = ("INGRESO_TOTAL",   "sum"),
            ASP      = ("PRECIO_UNITARIO", "mean"),
            MODELOS  = ("MODELO",          "nunique"),
        )
        .reset_index()
        .sort_values("UNIDADES", ascending=False)
    )
    km["PAIS"]                    = pais
    km["PARTICIPACION_UNIDADES"]  = km["UNIDADES"] / km["UNIDADES"].sum()
    km["PARTICIPACION_INGRESOS"]  = km["INGRESOS"] / km["INGRESOS"].sum()
    return km


def calcular_top_modelos(s: pd.DataFrame, pais: str) -> pd.DataFrame:
    tm = (
        s.groupby(["MODELO", "MARCA", "TIER"], dropna=False)
        .agg(
            UNIDADES = ("VALUE",           "sum"),
            INGRESOS = ("INGRESO_TOTAL",   "sum"),
            ASP      = ("PRECIO_UNITARIO", "mean"),
        )
        .reset_index()
        .sort_values("UNIDADES", ascending=False)
        .head(TOP_N_MODELOS)
    )
    tm["PAIS"] = pais
    return tm


def calcular_kpis_tier(s: pd.DataFrame, pais: str) -> pd.DataFrame:
    kt = (
        s.groupby("TIER", dropna=False)
        .agg(
            UNIDADES = ("VALUE",           "sum"),
            INGRESOS = ("INGRESO_TOTAL",   "sum"),
            ASP      = ("PRECIO_UNITARIO", "mean"),
            MODELOS  = ("MODELO",          "nunique"),
        )
        .reset_index()
        .sort_values("UNIDADES", ascending=False)
    )
    kt["PAIS"]                   = pais
    kt["PARTICIPACION_UNIDADES"] = kt["UNIDADES"] / kt["UNIDADES"].sum()
    kt["PARTICIPACION_INGRESOS"] = kt["INGRESOS"] / kt["INGRESOS"].sum()
    return kt


# ============================================================
# DIAGNÓSTICO DE FILL RATE
# ============================================================

def diagnostico_fill_rate_ultimo_mes(
    s: pd.DataFrame,
    iv: pd.DataFrame,
    pais: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if len(iv) == 0:
        return pd.DataFrame(), pd.DataFrame()

    ultimo_mes = iv["YEAR_MONTH"].max()

    skus_inv_ultimo = (
        iv[iv["YEAR_MONTH"] == ultimo_mes]
        .groupby("CODIGO_SKU", dropna=False)
        .agg(
            INV_TOTAL = ("VALUE",   "sum"),
            MODELO    = ("MODELO",  "first"),
            MARCA     = ("MARCA",   "first"),
            TIER      = ("TIER",    "first"),
            ESTADO    = ("ESTADO",  "first"),
        )
        .reset_index()
    )

    skus_venta_ultimo = (
        s[s["YEAR_MONTH"] == ultimo_mes]
        .groupby("CODIGO_SKU", dropna=False)
        .agg(VENTAS_MES=("VALUE", "sum"))
        .reset_index()
    )

    diagnostico = skus_inv_ultimo.merge(skus_venta_ultimo, on="CODIGO_SKU", how="left")
    diagnostico["VENDIO"]   = diagnostico["VENTAS_MES"].notna() & (diagnostico["VENTAS_MES"] > 0)
    diagnostico["TIPO_SKU"] = diagnostico["VENDIO"].map({True: "Con venta", False: "Sin venta en el mes"})
    diagnostico["PAIS"]     = pais
    diagnostico["YEAR_MONTH"] = ultimo_mes

    dead_stock = (
        diagnostico[~diagnostico["VENDIO"]]
        .sort_values("INV_TOTAL", ascending=False)
        .head(15)[["CODIGO_SKU", "MODELO", "MARCA", "TIER", "ESTADO", "INV_TOTAL"]]
    )

    return diagnostico, dead_stock


# ============================================================
# EXPORTACIÓN POR PAÍS
# ============================================================

def exportar_resultados_pais(
    dir_pais: Path,
    res_ventas: pd.DataFrame,
    vm: pd.DataFrame,
    va: pd.DataFrame,
    ki: pd.DataFrame,
    so: pd.DataFrame,
    fr: pd.DataFrame,
    inv_mes: pd.DataFrame,
    km: pd.DataFrame,
    tm: pd.DataFrame,
    kt: pd.DataFrame,
    diagnostico_fill: pd.DataFrame,
    dead_stock: pd.DataFrame,
) -> None:
    asegurar_directorio(dir_pais)

    res_ventas.to_csv(dir_pais / "kpi_ventas_generales.csv",         index=False)
    vm.to_csv(        dir_pais / "kpi_ventas_mensuales.csv",          index=False)
    va.to_csv(        dir_pais / "kpi_ventas_anuales.csv",            index=False)
    ki.to_csv(        dir_pais / "kpi_doh_turnover_sellthrough.csv",  index=False)
    so.to_csv(        dir_pais / "kpi_stockout_rate.csv",             index=False)
    fr.to_csv(        dir_pais / "kpi_fill_rate.csv",                 index=False)
    inv_mes.to_csv(   dir_pais / "inventario_snapshot_mes.csv",       index=False)
    km.to_csv(        dir_pais / "kpi_por_marca.csv",                 index=False)
    tm.to_csv(        dir_pais / "top_modelos.csv",                   index=False)
    kt.to_csv(        dir_pais / "kpi_por_tier.csv",                  index=False)

    if not diagnostico_fill.empty:
        diagnostico_fill.to_csv(dir_pais / "diagnostico_fill_rate.csv", index=False)
    if not dead_stock.empty:
        dead_stock.to_csv(dir_pais / "dead_stock_ultimo_mes.csv", index=False)


# ============================================================
# CONSOLIDADOS GLOBALES
# ============================================================

def consolidar_globales(acum: Dict[str, List[pd.DataFrame]]) -> None:
    asegurar_directorio(OUTPUT_DIR)

    if acum["ventas_generales"]:
        pd.concat(acum["ventas_generales"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_ventas_generales.csv", index=False)

    if acum["ventas_mes"]:
        pd.concat(acum["ventas_mes"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_ventas_mensuales.csv", index=False)

    if acum["ventas_anio"]:
        pd.concat(acum["ventas_anio"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_ventas_anuales.csv", index=False)

    if acum["kpi_inv"]:
        global_inv = pd.concat(acum["kpi_inv"], ignore_index=True)
        global_inv.to_csv(OUTPUT_DIR / "global_kpi_inventario.csv", index=False)

        resumen_inv_pais = (
            global_inv.groupby("PAIS", dropna=False)
            .agg(
                DOH_PROM       = ("DOH",             "mean"),
                VENTAS_TOT     = ("VENTAS_UNIDADES", "sum"),
                INV_PROM_TOT   = ("INV_PROMEDIO",    "sum"),
                INV_CIERRE_TOT = ("INV_CIERRE",      "sum"),
            )
            .reset_index()
        )
        resumen_inv_pais["TURNOVER_GLOBAL"] = (
            resumen_inv_pais["VENTAS_TOT"] / resumen_inv_pais["INV_PROM_TOT"].replace(0, np.nan)
        )
        resumen_inv_pais["SELL_THROUGH_GLOBAL"] = (
            resumen_inv_pais["VENTAS_TOT"] /
            (resumen_inv_pais["VENTAS_TOT"] + resumen_inv_pais["INV_CIERRE_TOT"].replace(0, np.nan))
        )
        resumen_inv_pais.sort_values("DOH_PROM", ascending=False).to_csv(
            OUTPUT_DIR / "global_resumen_inventario_pais.csv", index=False)

    if acum["stockout"]:
        global_so = pd.concat(acum["stockout"], ignore_index=True)
        global_so.to_csv(OUTPUT_DIR / "global_stockout_rate.csv", index=False)

        so_pais = (
            global_so.groupby("PAIS", dropna=False)
            .agg(STOCKOUT_RATE_PROM=("STOCKOUT_RATE", "mean"))
            .reset_index()
        )
        skus_stockout = (
            global_so[global_so["STOCKOUT_RATE"] > 0]
            .groupby("PAIS", dropna=False)["CODIGO_SKU"]
            .nunique()
            .reset_index(name="SKUS_CON_STOCKOUT")
        )
        so_pais = so_pais.merge(skus_stockout, on="PAIS", how="left")
        so_pais["SKUS_CON_STOCKOUT"] = so_pais["SKUS_CON_STOCKOUT"].fillna(0).astype(int)
        so_pais.to_csv(OUTPUT_DIR / "global_stockout_rate_pais.csv", index=False)

    if acum["fill_rate"]:
        global_fr = pd.concat(acum["fill_rate"], ignore_index=True)
        global_fr.to_csv(OUTPUT_DIR / "global_fill_rate.csv", index=False)
        (
            global_fr.groupby("PAIS", dropna=False)
            .agg(FILL_RATE_PROM=("FILL_RATE", "mean"))
            .reset_index()
            .to_csv(OUTPUT_DIR / "global_fill_rate_pais.csv", index=False)
        )

    if acum["marca"]:
        pd.concat(acum["marca"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_kpi_marca.csv", index=False)

    if acum["modelos"]:
        pd.concat(acum["modelos"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_top_modelos.csv", index=False)

    if acum["tier"]:
        pd.concat(acum["tier"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_kpi_tier.csv", index=False)

    if acum["diagnostico_fill"]:
        pd.concat(acum["diagnostico_fill"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_diagnostico_fill_rate.csv", index=False)

    if acum["dead_stock"]:
        pd.concat(acum["dead_stock"], ignore_index=True).to_csv(
            OUTPUT_DIR / "global_dead_stock_ultimo_mes.csv", index=False)

    print("Consolidados globales exportados.")


def calcular_tabla_ejecutiva_global(
    sales: pd.DataFrame,
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    te = (
        sales
        .groupby(["PAIS", "ANIO", "YEAR_MONTH"], dropna=False)
        .agg(
            UNIDADES        = ("VALUE",           "sum"),
            INGRESOS        = ("INGRESO_TOTAL",   "sum"),
            ASP             = ("PRECIO_UNITARIO", "mean"),
            SKUS_ACTIVOS    = ("CODIGO_SKU",      "nunique"),
            BODEGAS_ACTIVAS = ("CODIGO_BODEGA",   "nunique"),
        )
        .reset_index()
        .sort_values(["PAIS", "YEAR_MONTH"])
    )
    te["CRECIMIENTO_INGRESOS_MOM"] = te.groupby("PAIS")["INGRESOS"].pct_change()

    if not inventory.empty:
        inv_cierre = (
            inventory
            .sort_values(["PAIS", "CODIGO_SKU", "FECHA"])
            .groupby(["PAIS", "YEAR_MONTH"], dropna=False)
            .agg(INV_CIERRE=("VALUE", "sum"))
            .reset_index()
        )
        te = te.merge(inv_cierre, on=["PAIS", "YEAR_MONTH"], how="left")

    return te


def generar_reporte_excel(
    output_dir: Path,
    sales: pd.DataFrame,
    inventory: pd.DataFrame,
    acum: Dict[str, List[pd.DataFrame]],
    tabla_ejecutiva: pd.DataFrame,
) -> None:
    output_file = output_dir / "reporte_kpis.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        tabla_ejecutiva.to_excel(writer, sheet_name="ejecutivo_global", index=False)

        if acum["ventas_generales"]:
            pd.concat(acum["ventas_generales"], ignore_index=True).to_excel(
                writer, sheet_name="ventas_generales", index=False)

        if acum["kpi_inv"]:
            global_inv = pd.concat(acum["kpi_inv"], ignore_index=True)
            resumen = (
                global_inv.groupby("PAIS", dropna=False)
                .agg(
                    DOH_PROM       = ("DOH",             "mean"),
                    VENTAS_TOT     = ("VENTAS_UNIDADES", "sum"),
                    INV_PROM_TOT   = ("INV_PROMEDIO",    "sum"),
                    INV_CIERRE_TOT = ("INV_CIERRE",      "sum"),
                )
                .reset_index()
            )
            resumen["TURNOVER_GLOBAL"] = (
                resumen["VENTAS_TOT"] / resumen["INV_PROM_TOT"].replace(0, np.nan)
            )
            resumen["SELL_THROUGH_GLOBAL"] = (
                resumen["VENTAS_TOT"] /
                (resumen["VENTAS_TOT"] + resumen["INV_CIERRE_TOT"].replace(0, np.nan))
            )
            resumen.to_excel(writer, sheet_name="inventario_pais", index=False)

        if acum["stockout"]:
            so = pd.concat(acum["stockout"], ignore_index=True)
            so_resumen = (
                so.groupby("PAIS", dropna=False)
                .agg(
                    STOCKOUT_RATE_PROM = ("STOCKOUT_RATE", "mean"),
                    SKUS_CON_STOCKOUT  = ("STOCKOUT_RATE", lambda x: (x > 0).sum()),
                )
                .reset_index()
            )
            so_resumen.to_excel(writer, sheet_name="stockout_fill_rate", index=False)

        if acum["marca"]:
            pd.concat(acum["marca"], ignore_index=True).to_excel(
                writer, sheet_name="top_marcas", index=False)

        if acum["modelos"]:
            pd.concat(acum["modelos"], ignore_index=True).to_excel(
                writer, sheet_name="top_modelos", index=False)

        if acum["tier"]:
            pd.concat(acum["tier"], ignore_index=True).to_excel(
                writer, sheet_name="kpi_tier", index=False)

        if acum["dead_stock"]:
            pd.concat(acum["dead_stock"], ignore_index=True).to_excel(
                writer, sheet_name="dead_stock", index=False)

    print(f"Reporte Excel generado: {output_file}")


def separar_transacciones(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "TIPO_TRANSACCION" not in df.columns:
        raise ValueError("No existe la columna 'TIPO_TRANSACCION' en el DataFrame.")

    print("Tipos de transacción encontrados:", df["TIPO_TRANSACCION"].dropna().unique())

    sales     = df[df["TIPO_TRANSACCION"] == "SALES"].copy()
    inventory = df[df["TIPO_TRANSACCION"] == "INVENTORY"].copy()

    return sales, inventory


# ============================================================
# FLUJO PRINCIPAL  (corregido: acum inicializado antes del loop)
# ============================================================

def ejecutar_fase_2() -> None:
    df = cargar_datos()

    sales, inventory = cargar_datos()
    sales     = preparar_sales(sales)
    inventory = preparar_inventory(inventory)

    print("Columnas sales:",     sales.columns.tolist())
    print("Columnas inventory:", inventory.columns.tolist())

    # FIX: inicializar acumulador ANTES del loop
    acum: Dict[str, List[pd.DataFrame]] = {
        "ventas_generales" : [],
        "ventas_mes"       : [],
        "ventas_anio"      : [],
        "kpi_inv"          : [],
        "stockout"         : [],
        "fill_rate"        : [],
        "marca"            : [],
        "modelos"          : [],
        "tier"             : [],
        "diagnostico_fill" : [],
        "dead_stock"       : [],
    }

    for pais in PAISES:
        print(f"\n--- Procesando {pais} ---")
        s  = sales[sales["PAIS"] == pais].copy()
        iv = inventory[inventory["PAIS"] == pais].copy()

        if s.empty:
            print(f"[INFO] No hay ventas para {pais}, se omite.")
            continue

        res_ventas = calcular_kpis_ventas_generales(s, iv, pais)
        vm         = calcular_kpis_ventas_mensuales(s, pais)
        va         = calcular_kpis_ventas_anuales(s, pais)

        if not iv.empty:
            ki, so, fr, inv_mes          = calcular_kpis_inventario(s, iv, pais)
            diagnostico_fill, dead_stock = diagnostico_fill_rate_ultimo_mes(s, iv, pais)
        else:
            ki = so = fr = inv_mes = diagnostico_fill = dead_stock = pd.DataFrame()

        km = calcular_kpis_marca(s, pais)
        tm = calcular_top_modelos(s, pais)
        kt = calcular_kpis_tier(s, pais)

        # FIX: acumular resultados de este país
        if not res_ventas.empty:      acum["ventas_generales"].append(res_ventas)
        if not vm.empty:              acum["ventas_mes"].append(vm)
        if not va.empty:              acum["ventas_anio"].append(va)
        if not ki.empty:              acum["kpi_inv"].append(ki)
        if not so.empty:              acum["stockout"].append(so)
        if not fr.empty:              acum["fill_rate"].append(fr)
        if not km.empty:              acum["marca"].append(km)
        if not tm.empty:              acum["modelos"].append(tm)
        if not kt.empty:              acum["tier"].append(kt)
        if not diagnostico_fill.empty: acum["diagnostico_fill"].append(diagnostico_fill)
        if not dead_stock.empty:       acum["dead_stock"].append(dead_stock)

        exportar_resultados_pais(
            dir_pais=OUTPUT_DIR / pais,
            res_ventas=res_ventas, vm=vm, va=va,
            ki=ki, so=so, fr=fr, inv_mes=inv_mes,
            km=km, tm=tm, kt=kt,
            diagnostico_fill=diagnostico_fill,
            dead_stock=dead_stock,
        )

    # -- Consolidado global --------------------------------------------------
    consolidar_globales(acum)

    # -- Tabla ejecutiva global -----------------------------------------------
    tabla_ejecutiva = calcular_tabla_ejecutiva_global(sales, inventory)
    tabla_ejecutiva.to_csv(OUTPUT_DIR / "tabla_ejecutiva_global.csv", index=False)
    print(f"Tabla ejecutiva global: {len(tabla_ejecutiva)} filas exportadas.")

    # -- Reporte Excel --------------------------------------------------------
    generar_reporte_excel(OUTPUT_DIR, sales, inventory, acum, tabla_ejecutiva)

    print("=" * 65)
    print("FASE 2 COMPLETADA")
    print(f"Salidas en: {OUTPUT_DIR}")
    print("=" * 65)


def main() -> None:
    ejecutar_fase_2()


if __name__ == "__main__":
    main()