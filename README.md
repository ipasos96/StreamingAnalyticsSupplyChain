# TFM — Streaming Analytics para Supply Chain
## Modelos Predictivos de Demanda en Telecomunicaciones Multi-País
**Autora:** Iliana Yazmin Pasos Gallo

---

## Estructura del proyecto

```
StreamingAnalyticsSupplyChain/
├── data/
│   ├── raw/               ← archivos fuente originales
│   ├── processed/         ← datos limpios (generados por fase1)
│   └── catalogs/          ← catálogos de referencia
├── outputs/
│   ├── dashboard/         ← gráficas del EDA
│   ├── kpis/              ← tablas de KPIs
│   ├── models/            ← modelos entrenados (.pkl)
│   └── streaming/         ← salidas del pipeline en tiempo real
├── src/
│   ├── fase1_limpieza.py
│   ├── fase2_kpis.py
│   ├── fase3_modelos.py
│   ├── fase4_streaming.py
│   ├── fase4_consumer.py
│   └── dashboard_streaming.py
├── notebooks/
│   ├── 01_exploracion.ipynb
│   ├── 02_kpis.ipynb
│   └── 03_modelos.ipynb
├── requirements.txt
└── README.md
```

---

## Datos

Los datos no están incluidos en el repositorio por su tamaño.

### Descarga

1. Descarga la carpeta completa desde [Google Drive](https://drive.google.com/drive/folders/151_unOKlMvEeXKnQ127PP7K_9ag0PgOa?usp=sharing)
2. Colócala en la raíz del proyecto, de modo que quede así:
```
StreamingAnalyticsSupplyChain/
└── data/
    ├── raw/        ← archivos fuente originales
    ├── processed/  ← se genera al ejecutar Fase 1
    └── catalogs/   ← catálogos de referencia
```

> La carpeta `data/processed/` se genera automáticamente al ejecutar la Fase 1.

---
## Requisitos previos

Antes de ejecutar cualquier comando, asegúrate de estar dentro de la carpeta del proyecto:

```bash
cd ruta/a/StreamingAnalyticsSupplyChain
```

Todos los comandos deben ejecutarse desde esta carpeta raíz, de lo contrario los scripts no encontrarán los archivos de datos y configuración.

## Instalación

### 1. Crear entorno virtual

```bash
python -m venv venv
```

### 2. Activar el entorno virtual

**Windows:**
```bash
venv\Scripts\activate
```

**Mac / Linux:**
```bash
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Ejecución paso a paso

### Fase 1 — Limpieza y preparación de datos
```bash
python src/fase1_limpieza.py
```
Genera `data/processed/datos_limpios.parquet`

---

### Fase 2 — Cálculo de KPIs de Supply Chain
```bash
python src/fase2_kpis.py
```
Genera las tablas de KPIs en `outputs/kpis/`

---

### Fase 3 — Entrenamiento de modelos predictivos
```bash
python src/fase3_modelos.py
```
Entrena Regresión Lineal, Random Forest y XGBoost.
Guarda los modelos en `outputs/models/`

---

### Fase 4 — Pipeline de Streaming (dos terminales)

Abrir **dos terminales** con el entorno virtual activado:

En caso de no activarse el entorno virtual en la terminal ejecutar:
```bash
venv\Scripts\activate
```

**Terminal 1 — Productor** (genera el flujo de eventos):
```bash
python src/fase4_streaming.py
```

**Terminal 2 — Consumidor** (procesa los eventos en tiempo real):
```bash
python src/fase4_consumer.py
```

---

### Dashboard interactivo (Streamlit)

Con el consumidor corriendo, abrir una tercera terminal:

```bash
streamlit run src/dashboard_streaming.py
```

Se abrirá automáticamente en el navegador en `http://localhost:8501`

---

## Notebooks de análisis

Los notebooks están en la carpeta `notebooks/` y pueden ejecutarse
de forma independiente en Jupyter Notebook o JupyterLab:

```bash
jupyter notebook
```

| Notebook | Contenido |
|---|---|
| `01_exploracion.ipynb` | Análisis exploratorio de datos (EDA) |
| `02_kpis.ipynb` | Cálculo y visualización de KPIs |
| `03_modelos.ipynb` | Entrenamiento y evaluación de modelos |

---

## Tecnologías utilizadas

| Componente | Herramienta |
|---|---|
| Lenguaje | Python 3.11 |
| Manipulación de datos | pandas, NumPy |
| Modelado ML | scikit-learn, XGBoost |
| Visualización EDA | matplotlib, seaborn |
| Streaming | Python (producer/consumer) |
| Dashboard | Streamlit, Plotly |
| Serialización | joblib, pyarrow |
