from __future__ import annotations

"""
fase3_modelos.py
================
Entrenamiento y evaluacion de modelos de prediccion de demanda.

Este script es la version productiva del notebook 03_modelos.ipynb.
Reproduce exactamente el mismo pipeline pero sin graficos interactivos,
guarda todas las salidas a disco y puede ejecutarse desde consola.

Pasos:
1. Carga el parquet limpio generado por fase1_limpieza.py.
2. Filtra ventas y agrega por semana x SKU x Pais.
3. Construye features temporales, lags y ventanas rodantes.
4. Hace split temporal 80/20 (nunca split aleatorio en series de tiempo).
5. Entrena Regresion Lineal, Random Forest y XGBoost.
6. Evalua y compara los 3 modelos (MAE, RMSE, R2, MAPE).
7. Guarda los modelos entrenados como .pkl.
8. Guarda metricas, predicciones y graficos en outputs/models/.

Uso:
    python src/fase3_modelos.py

Entradas:
    data/processed/datos_limpios.parquet

Salidas:
    outputs/models/model_linear_regression.pkl
    outputs/models/model_random_forest.pkl
    outputs/models/model_xgboost.pkl
    outputs/models/scaler_linear.pkl
    outputs/models/metricas_modelos.csv
    outputs/models/metricas_por_pais.csv
    outputs/models/predicciones_test.parquet
    outputs/models/reporte_modelos.xlsx
    outputs/models/*.png
"""

from pathlib import Path

import joblib
import matplotlib
matplotlib.use('Agg')  # sin pantalla, solo guarda a disco
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder, StandardScaler

from xgboost import XGBRegressor


# ============================================================
# CONFIGURACION
# ============================================================

BASE_DIR   = Path(__file__).resolve().parents[1]
DATA_PROC  = BASE_DIR / 'data' / 'processed'
OUTPUT_DIR = BASE_DIR / 'outputs' / 'models'

DATA_FILE = DATA_PROC / 'datos_limpios.parquet'

# Hiperparametros — mismos que el notebook
RF_PARAMS = dict(
    n_estimators     = 300,
    max_depth        = 12,
    min_samples_leaf = 5,
    max_features     = 'sqrt',
    n_jobs           = -1,
    random_state     = 42,
)

XGB_PARAMS = dict(
    n_estimators     = 500,
    learning_rate    = 0.05,
    max_depth        = 6,
    subsample        = 0.8,
    colsample_bytree = 0.8,
    min_child_weight = 5,
    reg_alpha        = 0.1,
    reg_lambda       = 1.0,
    n_jobs           = -1,
    random_state     = 42,
    verbosity        = 0,
)

FEATURE_COLS = [
    'SEMANA_ANO', 'MES', 'TRIMESTRE', 'ANO',
    'SEMANA_SIN', 'SEMANA_COS', 'MES_SIN', 'MES_COS',
    'LAG_1W', 'LAG_2W', 'LAG_4W', 'LAG_8W', 'LAG_13W',
    'ROLLING_4W_MEAN', 'ROLLING_4W_STD', 'ROLLING_8W_MEAN',
    'PRECIO_PROM', 'COSTO_PROM', 'NUM_TRANSAC',
    'PAIS_ENC', 'FAMILIA_ENC', 'MARCA_ENC',
]
TARGET      = 'DEMANDA'
TRAIN_PCT   = 0.80

PALETTE = ['#00d4aa', '#6c63ff', '#ff6b6b', '#ffd166']

plt.rcParams.update({
    'figure.facecolor' : '#0f1117',
    'axes.facecolor'   : '#1a1d27',
    'axes.edgecolor'   : '#3a3d4d',
    'axes.labelcolor'  : '#e0e0e0',
    'xtick.color'      : '#a0a0b0',
    'ytick.color'      : '#a0a0b0',
    'text.color'       : '#e0e0e0',
    'grid.color'       : '#2a2d3d',
    'grid.linestyle'   : '--',
    'font.family'      : 'DejaVu Sans',
    'font.size'        : 11,
})


# ============================================================
# CARGA Y PREPARACION
# ============================================================

def cargar_datos() -> pd.DataFrame:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f'No existe el parquet: {DATA_FILE}')

    df = pd.read_parquet(DATA_FILE)
    print(f'Parquet cargado: {len(df):,} filas | {df.shape[1]} columnas')
    print(f'Periodo : {df["FECHA"].min().date()} -- {df["FECHA"].max().date()}')
    print(f'Paises  : {sorted(df["PAIS"].dropna().unique().tolist())}')
    return df


def preparar_ventas(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['TIPO_TRANSACCION'] == 'SALES'].copy()
    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce').fillna(0)

    print(f'Registros de venta: {len(df):,}')

    df['YEAR_WEEK'] = df['FECHA'].dt.to_period('W').dt.start_time

    df_agg = (
        df.groupby(['YEAR_WEEK', 'CODIGO_SKU', 'FAMILIA', 'MODELO', 'MARCA', 'PAIS'])
          .agg(
              DEMANDA     = ('VALUE',           'sum'),
              NUM_TRANSAC = ('VALUE',           'count'),
              PRECIO_PROM = ('PRECIO_UNITARIO', 'mean'),
              COSTO_PROM  = ('COSTO',           'mean'),
          )
          .reset_index()
          .sort_values(['PAIS', 'CODIGO_SKU', 'YEAR_WEEK'])
    )

    print(f'Dataset agregado: {len(df_agg):,} filas')
    return df_agg


# ============================================================
# FEATURE ENGINEERING
# ============================================================

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    d   = df.copy().sort_values(['PAIS', 'CODIGO_SKU', 'YEAR_WEEK'])
    grp = ['PAIS', 'CODIGO_SKU']

    # -- Temporales -----------------------------------------------------------
    d['SEMANA_ANO'] = d['YEAR_WEEK'].dt.isocalendar().week.astype(int)
    d['MES']        = d['YEAR_WEEK'].dt.month
    d['TRIMESTRE']  = d['YEAR_WEEK'].dt.quarter
    d['ANO']        = d['YEAR_WEEK'].dt.year

    # Codificacion ciclica
    d['SEMANA_SIN'] = np.sin(2 * np.pi * d['SEMANA_ANO'] / 52)
    d['SEMANA_COS'] = np.cos(2 * np.pi * d['SEMANA_ANO'] / 52)
    d['MES_SIN']    = np.sin(2 * np.pi * d['MES'] / 12)
    d['MES_COS']    = np.cos(2 * np.pi * d['MES'] / 12)

    # -- Lags -----------------------------------------------------------------
    for lag in [1, 2, 4, 8, 13]:
        d[f'LAG_{lag}W'] = d.groupby(grp)['DEMANDA'].shift(lag)

    # -- Ventanas rodantes ----------------------------------------------------
    d['ROLLING_4W_MEAN'] = (
        d.groupby(grp)['DEMANDA']
         .transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())
    )
    d['ROLLING_4W_STD'] = (
        d.groupby(grp)['DEMANDA']
         .transform(lambda x: x.shift(1).rolling(4, min_periods=2).std().fillna(0))
    )
    d['ROLLING_8W_MEAN'] = (
        d.groupby(grp)['DEMANDA']
         .transform(lambda x: x.shift(1).rolling(8, min_periods=1).mean())
    )

    # -- Encoding categorico --------------------------------------------------
    for col in ['PAIS', 'FAMILIA', 'MARCA']:
        le = LabelEncoder()
        d[f'{col}_ENC'] = le.fit_transform(d[col].astype(str))

    return d


# ============================================================
# SPLIT TEMPORAL
# ============================================================

def split_temporal(df_feat: pd.DataFrame) -> tuple:
    df_model = df_feat[FEATURE_COLS + [TARGET, 'YEAR_WEEK', 'PAIS']].dropna()
    print(f'Dataset listo para modelar: {len(df_model):,} filas')

    split_date = df_model['YEAR_WEEK'].quantile(TRAIN_PCT, interpolation='nearest')
    print(f'Fecha de corte train/test  : {split_date.date()}')

    train = df_model[df_model['YEAR_WEEK'] <= split_date]
    test  = df_model[df_model['YEAR_WEEK'] >  split_date]

    X_train = train[FEATURE_COLS]
    y_train = train[TARGET]
    X_test  = test[FEATURE_COLS]
    y_test  = test[TARGET]

    print(f'Train: {len(X_train):,} muestras  ({train["YEAR_WEEK"].min().date()} -- {train["YEAR_WEEK"].max().date()})')
    print(f'Test : {len(X_test):,}  muestras  ({test["YEAR_WEEK"].min().date()} -- {test["YEAR_WEEK"].max().date()})')

    return train, test, X_train, y_train, X_test, y_test


# ============================================================
# ENTRENAMIENTO
# ============================================================

def entrenar_modelos(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test:  pd.DataFrame,
    y_test:  pd.Series,
) -> tuple:
    # -- Regresion Lineal -----------------------------------------------------
    print('Entrenando Regresion Lineal...')
    scaler     = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc  = scaler.transform(X_test)

    lr        = LinearRegression()
    lr.fit(X_train_sc, y_train)
    y_pred_lr = np.clip(lr.predict(X_test_sc), 0, None)
    print('  Regresion Lineal lista.')

    # -- Random Forest --------------------------------------------------------
    print('Entrenando Random Forest...')
    rf        = RandomForestRegressor(**RF_PARAMS)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    print('  Random Forest listo.')

    # -- XGBoost --------------------------------------------------------------
    print('Entrenando XGBoost...')
    xgb = XGBRegressor(**XGB_PARAMS)
    xgb.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    y_pred_xgb = xgb.predict(X_test)
    print('  XGBoost listo.')

    return lr, rf, xgb, scaler, y_pred_lr, y_pred_rf, y_pred_xgb


# ============================================================
# EVALUACION
# ============================================================

def evaluar(nombre: str, y_true: pd.Series, y_pred: np.ndarray) -> dict:
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    mask = y_true > 0
    mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    return {'Modelo': nombre, 'MAE': mae, 'RMSE': rmse, 'R2': r2, 'MAPE (%)': mape}


def calcular_metricas(
    y_test:    pd.Series,
    y_pred_lr: np.ndarray,
    y_pred_rf: np.ndarray,
    y_pred_xgb: np.ndarray,
) -> pd.DataFrame:
    resultados = pd.DataFrame([
        evaluar('Regresion Lineal', y_test, y_pred_lr),
        evaluar('Random Forest',    y_test, y_pred_rf),
        evaluar('XGBoost',          y_test, y_pred_xgb),
    ]).set_index('Modelo').round(3)

    print('\n-- Metricas en Test Set --')
    print(resultados.to_string())
    return resultados


def calcular_metricas_por_pais(
    test:       pd.DataFrame,
    y_pred_best: np.ndarray,
    mejor_nombre: str,
) -> pd.DataFrame:
    test_eval              = test[['YEAR_WEEK', 'PAIS', TARGET]].copy()
    test_eval['PRED']      = y_pred_best
    test_eval['ABS_ERROR'] = np.abs(test_eval[TARGET] - test_eval['PRED'])

    pais_metrics = (
        test_eval.groupby('PAIS')
        .apply(lambda g: pd.Series({
            'MAE'       : mean_absolute_error(g[TARGET], g['PRED']),
            'RMSE'      : np.sqrt(mean_squared_error(g[TARGET], g['PRED'])),
            'R2'        : r2_score(g[TARGET], g['PRED']),
            'N_muestras': len(g),
        }))
        .reset_index()
        .sort_values('MAE')
    )

    print(f'\n-- Metricas por pais ({mejor_nombre}) --')
    print(pais_metrics.round(3).to_string(index=False))
    return test_eval, pais_metrics


# ============================================================
# GRAFICOS
# ============================================================

def guardar_split(df_model: pd.DataFrame, split_date: pd.Timestamp) -> None:
    fig, ax = plt.subplots(figsize=(13, 4))
    weekly  = df_model.groupby('YEAR_WEEK')['DEMANDA'].sum()

    ax.fill_between(weekly.index[weekly.index <= split_date],
                    weekly[weekly.index <= split_date],
                    alpha=0.45, color=PALETTE[0], label='Train')
    ax.fill_between(weekly.index[weekly.index > split_date],
                    weekly[weekly.index > split_date],
                    alpha=0.45, color=PALETTE[1], label='Test')
    ax.plot(weekly.index, weekly, color='white', lw=1)
    ax.axvline(split_date, color=PALETTE[2], lw=2, ls='--',
               label=f'Corte: {split_date.date()}')
    ax.set_title('Demanda semanal total -- Split temporal Train / Test')
    ax.set_xlabel('Semana')
    ax.set_ylabel('Unidades')
    ax.legend()
    ax.grid(alpha=0.4)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'split_temporal.png', dpi=150, bbox_inches='tight')
    plt.close()


def guardar_comparacion_modelos(resultados: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 4, figsize=(16, 5))
    metricas  = ['MAE', 'RMSE', 'R2', 'MAPE (%)']
    etiquetas = ['Lin. Reg.', 'Rnd Forest', 'XGBoost']

    for ax, metrica in zip(axes, metricas):
        valores = resultados[metrica].values
        bars    = ax.bar(etiquetas, valores, color=PALETTE[:3],
                         edgecolor='white', linewidth=0.6)
        best = np.argmin(valores) if metrica != 'R2' else np.argmax(valores)
        bars[best].set_edgecolor('#ffd166')
        bars[best].set_linewidth(2.5)
        for bar, val in zip(bars, valores):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(valores) * 0.01,
                    f'{val:.2f}', ha='center', va='bottom', fontsize=10)
        ax.set_title(metrica)
        ax.grid(axis='y', alpha=0.4)

    fig.suptitle('Comparacion de modelos -- Prediccion de Demanda', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'comparacion_modelos.png', dpi=150, bbox_inches='tight')
    plt.close()


def guardar_real_vs_predicho(
    y_test:    pd.Series,
    y_pred_lr: np.ndarray,
    y_pred_rf: np.ndarray,
    y_pred_xgb: np.ndarray,
) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    preds  = [y_pred_lr, y_pred_rf, y_pred_xgb]
    labels = ['Regresion Lineal', 'Random Forest', 'XGBoost']

    for ax, pred, label, color in zip(axes, preds, labels, PALETTE):
        ax.scatter(y_test, pred, alpha=0.3, s=15, color=color)
        lim = max(y_test.max(), pred.max())
        ax.plot([0, lim], [0, lim], 'w--', lw=1.5)
        ax.set_xlabel('Real')
        ax.set_ylabel('Predicho')
        ax.set_title(label)
        ax.grid(alpha=0.3)
        r2 = r2_score(y_test, pred)
        ax.text(0.05, 0.92, f'R2 = {r2:.3f}', transform=ax.transAxes,
                color='white', fontsize=11,
                bbox=dict(boxstyle='round', facecolor='#2a2d3d', alpha=0.8))

    fig.suptitle('Real vs Predicho por modelo', y=1.02)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'real_vs_predicho.png', dpi=150, bbox_inches='tight')
    plt.close()


def guardar_serie_temporal(
    test:         pd.DataFrame,
    y_pred_best:  np.ndarray,
    mejor_nombre: str,
) -> None:
    test_plot         = test.copy()
    test_plot['PRED'] = y_pred_best
    weekly_real       = test_plot.groupby('YEAR_WEEK')[TARGET].sum()
    weekly_pred       = test_plot.groupby('YEAR_WEEK')['PRED'].sum()

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(weekly_real.index, weekly_real,
            color=PALETTE[0], lw=2.5, marker='o', ms=4, label='Real')
    ax.plot(weekly_pred.index, weekly_pred,
            color=PALETTE[1], lw=2, marker='s', ms=4, ls='--',
            label=f'Predicho ({mejor_nombre})')
    ax.fill_between(weekly_real.index, weekly_real, weekly_pred,
                    alpha=0.15, color=PALETTE[2], label='Error')
    ax.set_title(f'Demanda real vs predicha (Test Set) -- {mejor_nombre}')
    ax.set_xlabel('Semana')
    ax.set_ylabel('Unidades')
    ax.legend()
    ax.grid(alpha=0.4)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'serie_real_vs_predicha.png', dpi=150, bbox_inches='tight')
    plt.close()


def guardar_feature_importance(
    rf:  RandomForestRegressor,
    xgb: XGBRegressor,
) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    rf_imp  = pd.Series(rf.feature_importances_,  index=FEATURE_COLS).sort_values()
    xgb_imp = pd.Series(xgb.feature_importances_, index=FEATURE_COLS).sort_values()

    axes[0].barh(rf_imp.index,  rf_imp.values,  color=PALETTE[0], edgecolor='none')
    axes[0].set_title('Random Forest -- Feature Importance')
    axes[0].set_xlabel('Importancia')
    axes[0].grid(axis='x', alpha=0.4)

    axes[1].barh(xgb_imp.index, xgb_imp.values, color=PALETTE[1], edgecolor='none')
    axes[1].set_title('XGBoost -- Feature Importance')
    axes[1].set_xlabel('Importancia')
    axes[1].grid(axis='x', alpha=0.4)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'feature_importance.png', dpi=150, bbox_inches='tight')
    plt.close()


def guardar_mae_por_pais(pais_metrics: pd.DataFrame, mejor_nombre: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.bar(pais_metrics['PAIS'], pais_metrics['MAE'],
                  color=PALETTE[0], edgecolor='white', linewidth=0.6)
    for bar, val in zip(bars, pais_metrics['MAE']):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f'{val:.1f}', ha='center', va='bottom', fontsize=10)
    ax.set_title(f'MAE por pais -- {mejor_nombre}')
    ax.set_ylabel('MAE (unidades)')
    ax.grid(axis='y', alpha=0.4)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'mae_por_pais.png', dpi=150, bbox_inches='tight')
    plt.close()


# ============================================================
# PERSISTENCIA
# ============================================================

def guardar_modelos(
    lr:    LinearRegression,
    rf:    RandomForestRegressor,
    xgb:   XGBRegressor,
    scaler: StandardScaler,
) -> None:
    joblib.dump(lr,     OUTPUT_DIR / 'model_linear_regression.pkl')
    joblib.dump(rf,     OUTPUT_DIR / 'model_random_forest.pkl')
    joblib.dump(xgb,    OUTPUT_DIR / 'model_xgboost.pkl')
    joblib.dump(scaler, OUTPUT_DIR / 'scaler_linear.pkl')
    print('Modelos guardados.')


def guardar_resultados(
    resultados:   pd.DataFrame,
    pais_metrics: pd.DataFrame,
    test_eval:    pd.DataFrame,
    train:        pd.DataFrame,
    test:         pd.DataFrame,
    mejor_nombre: str,
) -> None:
    # CSVs
    resultados.to_csv(OUTPUT_DIR / 'metricas_modelos.csv')
    pais_metrics.to_csv(OUTPUT_DIR / 'metricas_por_pais.csv', index=False)
    test_eval.to_parquet(OUTPUT_DIR / 'predicciones_test.parquet', index=False)

    # Reporte Excel con todas las salidas en un solo archivo
    with pd.ExcelWriter(OUTPUT_DIR / 'reporte_modelos.xlsx', engine='openpyxl') as writer:
        resultados.to_excel(writer, sheet_name='metricas_globales')
        pais_metrics.to_excel(writer, sheet_name='metricas_por_pais', index=False)
        test_eval.to_excel(writer, sheet_name='predicciones_test', index=False)

        resumen = pd.DataFrame({
            'metrica': [
                'modelo_ganador', 'MAE', 'RMSE', 'R2', 'MAPE (%)',
                'train_inicio', 'train_fin', 'train_muestras',
                'test_inicio', 'test_fin', 'test_muestras',
            ],
            'valor': [
                mejor_nombre,
                resultados.loc[mejor_nombre, 'MAE'],
                resultados.loc[mejor_nombre, 'RMSE'],
                resultados.loc[mejor_nombre, 'R2'],
                resultados.loc[mejor_nombre, 'MAPE (%)'],
                str(train['YEAR_WEEK'].min().date()),
                str(train['YEAR_WEEK'].max().date()),
                len(train),
                str(test['YEAR_WEEK'].min().date()),
                str(test['YEAR_WEEK'].max().date()),
                len(test),
            ]
        })
        resumen.to_excel(writer, sheet_name='resumen_ejecutivo', index=False)

    print('Resultados guardados.')


# ============================================================
# MODELO DE GAMA — PREDICCION PARA SKUs NUEVOS
# ============================================================

def preparar_ventas_gama(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Agrega ventas por MARCA x TIER x PAIS x Semana
    df = df_raw[df_raw['TIPO_TRANSACCION'] == 'SALES'].copy()
    df['FECHA']     = pd.to_datetime(df['FECHA'])
    df['VALUE']     = pd.to_numeric(df['VALUE'], errors='coerce').fillna(0)
    df['YEAR_WEEK'] = df['FECHA'].dt.to_period('W').dt.start_time

    df_agg = (
        df.dropna(subset=['MARCA', 'TIER'])
          .groupby(['YEAR_WEEK', 'MARCA', 'TIER', 'PAIS'])
          .agg(
              DEMANDA_GAMA = ('VALUE',           'sum'),
              NUM_SKUS     = ('VALUE',           'count'),
              PRECIO_PROM  = ('PRECIO_UNITARIO', 'mean'),
              COSTO_PROM   = ('COSTO',           'mean'),
          )
          .reset_index()
          .sort_values(['PAIS', 'MARCA', 'TIER', 'YEAR_WEEK'])
    )

    print(f'Dataset de gama: {len(df_agg):,} filas | '
          f'{df_agg.groupby(["MARCA","TIER","PAIS"]).ngroups} combinaciones')
    return df_agg


def build_features_gama(df: pd.DataFrame) -> pd.DataFrame:
    d   = df.copy().sort_values(['PAIS', 'MARCA', 'TIER', 'YEAR_WEEK'])
    grp = ['PAIS', 'MARCA', 'TIER']

    d['SEMANA_ANO'] = d['YEAR_WEEK'].dt.isocalendar().week.astype(int)
    d['MES']        = d['YEAR_WEEK'].dt.month
    d['TRIMESTRE']  = d['YEAR_WEEK'].dt.quarter
    d['ANO']        = d['YEAR_WEEK'].dt.year
    d['SEMANA_SIN'] = np.sin(2 * np.pi * d['SEMANA_ANO'] / 52)
    d['SEMANA_COS'] = np.cos(2 * np.pi * d['SEMANA_ANO'] / 52)
    d['MES_SIN']    = np.sin(2 * np.pi * d['MES'] / 12)
    d['MES_COS']    = np.cos(2 * np.pi * d['MES'] / 12)

    for lag in [1, 2, 4, 8, 13]:
        d[f'LAG_{lag}W'] = d.groupby(grp)['DEMANDA_GAMA'].shift(lag)

    d['ROLLING_4W_MEAN'] = (
        d.groupby(grp)['DEMANDA_GAMA']
         .transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())
    )
    d['ROLLING_4W_STD'] = (
        d.groupby(grp)['DEMANDA_GAMA']
         .transform(lambda x: x.shift(1).rolling(4, min_periods=2).std().fillna(0))
    )
    d['ROLLING_8W_MEAN'] = (
        d.groupby(grp)['DEMANDA_GAMA']
         .transform(lambda x: x.shift(1).rolling(8, min_periods=1).mean())
    )

    for col in ['PAIS', 'MARCA', 'TIER']:
        le = LabelEncoder()
        d[f'{col}_ENC'] = le.fit_transform(d[col].astype(str))

    return d


FEATURE_COLS_GAMA = [
    'SEMANA_ANO', 'MES', 'TRIMESTRE', 'ANO',
    'SEMANA_SIN', 'SEMANA_COS', 'MES_SIN', 'MES_COS',
    'LAG_1W', 'LAG_2W', 'LAG_4W', 'LAG_8W', 'LAG_13W',
    'ROLLING_4W_MEAN', 'ROLLING_4W_STD', 'ROLLING_8W_MEAN',
    'PRECIO_PROM', 'COSTO_PROM', 'NUM_SKUS',
    'PAIS_ENC', 'MARCA_ENC', 'TIER_ENC',
]
TARGET_GAMA = 'DEMANDA_GAMA'


def entrenar_modelo_gama(df_raw: pd.DataFrame) -> tuple:
    df_agg  = preparar_ventas_gama(df_raw)
    df_feat = build_features_gama(df_agg)

    df_model = df_feat[
        FEATURE_COLS_GAMA + [TARGET_GAMA, 'YEAR_WEEK', 'PAIS', 'MARCA', 'TIER']
    ].dropna()

    split_date = df_model['YEAR_WEEK'].quantile(TRAIN_PCT, interpolation='nearest')

    train = df_model[df_model['YEAR_WEEK'] <= split_date]
    test  = df_model[df_model['YEAR_WEEK'] >  split_date]

    xgb_gama = XGBRegressor(
        n_estimators     = 500,
        learning_rate    = 0.05,
        max_depth        = 6,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        min_child_weight = 3,
        reg_alpha        = 0.1,
        reg_lambda       = 1.0,
        n_jobs           = -1,
        random_state     = 42,
        verbosity        = 0,
    )
    xgb_gama.fit(
        train[FEATURE_COLS_GAMA], train[TARGET_GAMA],
        eval_set=[(test[FEATURE_COLS_GAMA], test[TARGET_GAMA])],
        verbose=False
    )

    y_pred = xgb_gama.predict(test[FEATURE_COLS_GAMA])
    metricas = evaluar('XGBoost Gama', test[TARGET_GAMA], y_pred)
    print(f'  Gama -- MAE: {metricas["MAE"]:.2f} | R2: {metricas["R2"]:.3f}')

    # Tabla de referencia: demanda promedio por MARCA x TIER x PAIS
    tabla_ref = (
        df_agg
        .groupby(['PAIS', 'MARCA', 'TIER'])
        .agg(
            DEMANDA_SEMANAL_PROM = ('DEMANDA_GAMA', 'mean'),
            DEMANDA_SEMANAL_MAX  = ('DEMANDA_GAMA', 'max'),
            DEMANDA_SEMANAL_STD  = ('DEMANDA_GAMA', 'std'),
            SEMANAS_CON_DATOS    = ('DEMANDA_GAMA', 'count'),
            PRECIO_PROM          = ('PRECIO_PROM',  'mean'),
        )
        .round(1)
        .reset_index()
        .sort_values(['PAIS', 'MARCA', 'DEMANDA_SEMANAL_PROM'], ascending=[True, True, False])
    )

    return xgb_gama, tabla_ref, metricas


def guardar_modelo_gama(
    xgb_gama:  XGBRegressor,
    tabla_ref: pd.DataFrame,
    metricas:  dict,
) -> None:
    joblib.dump(xgb_gama, OUTPUT_DIR / 'model_xgboost_gama.pkl')
    tabla_ref.to_csv(OUTPUT_DIR / 'tabla_referencia_gama.csv', index=False)
    tabla_ref.to_parquet(OUTPUT_DIR / 'tabla_referencia_gama.parquet', index=False)

    print(f'  model_xgboost_gama.pkl guardado')
    print(f'  tabla_referencia_gama.csv  ({len(tabla_ref)} combinaciones MARCA x TIER x PAIS)')


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print('=' * 65)
    print('FASE 3 -- MODELOS DE PREDICCION DE DEMANDA')
    print('=' * 65)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -- Carga y preparacion --------------------------------------------------
    df_raw  = cargar_datos()
    df_agg  = preparar_ventas(df_raw)
    df_feat = build_features(df_agg)

    # -- Split ----------------------------------------------------------------
    train, test, X_train, y_train, X_test, y_test = split_temporal(df_feat)

    # -- Grafico del split ----------------------------------------------------
    df_model = df_feat[FEATURE_COLS + [TARGET, 'YEAR_WEEK', 'PAIS']].dropna()
    split_date = df_model['YEAR_WEEK'].quantile(TRAIN_PCT, interpolation='nearest')
    guardar_split(df_model, split_date)

    # -- Entrenamiento --------------------------------------------------------
    print('\n-- Entrenamiento --')
    lr, rf, xgb, scaler, y_pred_lr, y_pred_rf, y_pred_xgb = entrenar_modelos(
        X_train, y_train, X_test, y_test
    )

    # -- Evaluacion -----------------------------------------------------------
    print('\n-- Evaluacion --')
    resultados   = calcular_metricas(y_test, y_pred_lr, y_pred_rf, y_pred_xgb)
    mejor_nombre = resultados['MAE'].idxmin()
    pred_map     = {
        'Regresion Lineal': y_pred_lr,
        'Random Forest'   : y_pred_rf,
        'XGBoost'         : y_pred_xgb,
    }
    y_pred_best  = pred_map[mejor_nombre]

    test_eval, pais_metrics = calcular_metricas_por_pais(test, y_pred_best, mejor_nombre)

    # -- Graficos -------------------------------------------------------------
    print('\n-- Generando graficos --')
    guardar_comparacion_modelos(resultados)
    guardar_real_vs_predicho(y_test, y_pred_lr, y_pred_rf, y_pred_xgb)
    guardar_serie_temporal(test, y_pred_best, mejor_nombre)
    guardar_feature_importance(rf, xgb)
    guardar_mae_por_pais(pais_metrics, mejor_nombre)
    print('  Graficos guardados.')

    # -- Persistencia ---------------------------------------------------------
    print('\n-- Guardando modelos y resultados --')
    guardar_modelos(lr, rf, xgb, scaler)
    guardar_resultados(resultados, pais_metrics, test_eval, train, test, mejor_nombre)

    # -- Modelo de gama ------------------------------------------------------
    print('\n-- Modelo de gama (SKUs nuevos sin historial) --')
    xgb_gama, tabla_ref, metricas_gama = entrenar_modelo_gama(df_raw)
    guardar_modelo_gama(xgb_gama, tabla_ref, metricas_gama)

    # Agregar metricas de gama al Excel
    with pd.ExcelWriter(OUTPUT_DIR / 'reporte_modelos.xlsx',
                        engine='openpyxl', mode='a',
                        if_sheet_exists='replace') as writer:
        pd.DataFrame([metricas_gama]).set_index('Modelo').to_excel(
            writer, sheet_name='metricas_gama'
        )
        tabla_ref.to_excel(writer, sheet_name='referencia_gama', index=False)

    # -- Resumen final --------------------------------------------------------
    print()
    print('=' * 65)
    print('FASE 3 COMPLETADA')
    print(f'Modelo SKU ganador : {mejor_nombre}')
    print(f'  MAE              : {resultados.loc[mejor_nombre, "MAE"]:.2f} uds/SKU/semana')
    print(f'  R2               : {resultados.loc[mejor_nombre, "R2"]:.3f}')
    print(f'Modelo Gama (nuevo): XGBoost Gama')
    print(f'  MAE              : {metricas_gama["MAE"]:.2f} uds/gama/semana')
    print(f'  R2               : {metricas_gama["R2"]:.3f}')
    print(f'Salidas en         : {OUTPUT_DIR}')
    print('=' * 65)


if __name__ == '__main__':
    main()
