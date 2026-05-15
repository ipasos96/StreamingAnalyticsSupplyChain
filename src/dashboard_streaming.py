"""
dashboard_streaming.py — Dashboard en tiempo real para Fase 4
==============================================================
Lee los CSVs generados por fase4_consumer.py y los muestra
en un dashboard Streamlit que se auto-refresca cada 3 segundos.

Uso:
    streamlit run dashboard_streaming.py

Requiere:
    pip install streamlit plotly pandas watchdog
"""

import time
from pathlib import Path
try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ============================================================
# CONFIGURACION
# ============================================================

BASE_DIR   = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / 'outputs' / 'streaming'

KPIS_FILE   = OUTPUT_DIR / 'kpis_tiempo_real.csv'
ALERTAS_FILE = OUTPUT_DIR / 'alertas.csv'
PREDS_FILE  = OUTPUT_DIR / 'predicciones_stream.csv'
ORDENES_FILE = OUTPUT_DIR / 'ordenes_sugeridas.csv'

REFRESH_SEGUNDOS = 60

# Paleta de colores
COLOR_PRIMARY  = '#00D4FF'
COLOR_DANGER   = '#FF4C4C'
COLOR_WARNING  = '#FFB347'
COLOR_SUCCESS  = '#4CFF91'
COLOR_BG       = '#0A0E1A'
COLOR_CARD     = '#111827'
COLOR_TEXT     = '#E2E8F0'
COLOR_MUTED    = '#64748B'

# ============================================================
# CONFIGURACION DE PAGINA
# ============================================================

st.set_page_config(
    page_title='Supply Chain | Streaming Dashboard',
    page_icon='📡',
    layout='wide',
    initial_sidebar_state='collapsed',
)

# ============================================================
# CSS PERSONALIZADO
# ============================================================

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

/* Reset y base */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0A0E1A;
    color: #E2E8F0;
}

/* Ocultar elementos de Streamlit */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1rem 2rem 2rem 2rem !important; }

/* Header principal */
.dash-header {
    background: linear-gradient(135deg, #0A0E1A 0%, #0D1B2A 50%, #0A0E1A 100%);
    border-bottom: 1px solid #1E293B;
    padding: 1.2rem 0 1rem 0;
    margin-bottom: 1.5rem;
}
.dash-title {
    font-family: 'Space Mono', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    color: #00D4FF;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.dash-subtitle {
    font-size: 0.78rem;
    color: #64748B;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-top: 0.1rem;
}
.live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(0, 212, 255, 0.1);
    border: 1px solid rgba(0, 212, 255, 0.3);
    border-radius: 20px;
    padding: 4px 12px;
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    color: #00D4FF;
}
.live-dot {
    width: 7px; height: 7px;
    background: #00D4FF;
    border-radius: 50%;
    animation: pulse 1.5s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.8); }
}

/* KPI Cards */
.kpi-card {
    background: #111827;
    border: 1px solid #1E293B;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, #00D4FF, #0088AA);
}
.kpi-label {
    font-size: 0.68rem;
    color: #64748B;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.4rem;
}
.kpi-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.9rem;
    font-weight: 700;
    color: #E2E8F0;
    line-height: 1;
}
.kpi-delta {
    font-size: 0.72rem;
    color: #64748B;
    margin-top: 0.3rem;
}

/* Alerta cards */
.alerta-card {
    border-radius: 8px;
    padding: 0.7rem 1rem;
    margin-bottom: 0.5rem;
    border-left: 3px solid;
    font-size: 0.82rem;
}
.alerta-STOCKOUT      { background: rgba(255,76,76,0.08);  border-color: #FF4C4C; }
.alerta-DOH_BAJO      { background: rgba(255,179,71,0.08); border-color: #FFB347; }
.alerta-SOBRESTOCK    { background: rgba(100,116,139,0.1); border-color: #64748B; }
.alerta-PICO_DEMANDA  { background: rgba(0,212,255,0.08);  border-color: #00D4FF; }
.alerta-COBERTURA_INSUFICIENTE { background: rgba(255,179,71,0.08); border-color: #FFB347; }

.alerta-tipo {
    font-family: 'Space Mono', monospace;
    font-size: 0.65rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}
.alerta-detalle { color: #94A3B8; margin-top: 0.15rem; }

/* Sección títulos */
.section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.72rem;
    color: #00D4FF;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    border-bottom: 1px solid #1E293B;
    padding-bottom: 0.5rem;
    margin-bottom: 1rem;
}

/* Tabla */
.stDataFrame { background: #111827 !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 4px; }
::-webkit-scrollbar-track { background: #0A0E1A; }
::-webkit-scrollbar-thumb { background: #1E293B; border-radius: 4px; }

/* Panel Hero */
.hero-metric {
    background: rgba(0,0,0,0.3);
    border: 1px solid #1E293B;
    border-radius: 10px;
    padding: 1rem;
    text-align: center;
}
.hero-metric-value {
    font-family: 'Space Mono', monospace;
    font-size: 2rem;
    font-weight: 700;
    line-height: 1;
}
.hero-metric-label {
    font-size: 0.65rem;
    color: #64748B;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 0.4rem;
}
.hero-metric-sub {
    font-size: 0.7rem;
    margin-top: 0.3rem;
}
.hero-insight {
    background: rgba(0,212,255,0.05);
    border-left: 3px solid #00D4FF;
    border-radius: 0 8px 8px 0;
    padding: 0.6rem 1rem;
    margin-top: 0.8rem;
    font-size: 0.75rem;
    color: #94A3B8;
}

/* Sin datos */
.no-data {
    text-align: center;
    color: #334155;
    font-family: 'Space Mono', monospace;
    font-size: 0.8rem;
    padding: 2rem;
    border: 1px dashed #1E293B;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)


# ============================================================
# FUNCIONES DE CARGA
# ============================================================

@st.cache_data(ttl=REFRESH_SEGUNDOS)
def cargar_kpis():
    if not KPIS_FILE.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(KPIS_FILE)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEGUNDOS)
def cargar_alertas():
    if not ALERTAS_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(ALERTAS_FILE)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEGUNDOS)
def cargar_predicciones():
    if not PREDS_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(PREDS_FILE)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEGUNDOS)
def cargar_ordenes():
    if not ORDENES_FILE.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(ORDENES_FILE)
    except Exception:
        return pd.DataFrame()


# ============================================================
# COMPONENTES UI
# ============================================================

def render_kpi_card(label, value, delta=None):
    delta_html = f'<div class="kpi-delta">{delta}</div>' if delta else ''
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


ICONOS_ALERTA = {
    'STOCKOUT'              : ('🔴', '#FF4C4C'),
    'DOH_BAJO'              : ('🟠', '#FFB347'),
    'SOBRESTOCK'            : ('⚪', '#94A3B8'),
    'PICO_DEMANDA'          : ('🔵', '#00D4FF'),
    'COBERTURA_INSUFICIENTE': ('🟡', '#FFB347'),
}

def render_alerta(row):
    tipo   = str(row.get('tipo_alerta', ''))
    icono, color = ICONOS_ALERTA.get(tipo, ('⚪', '#64748B'))
    pais   = row.get('pais', '')
    sku    = row.get('codigo_sku', '')
    marca  = row.get('marca', '')
    detalle = row.get('detalle', '')
    ventana = row.get('ventana', '')

    st.markdown(f"""
    <div class="alerta-card alerta-{tipo}">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <span class="alerta-tipo" style="color:{color}">{icono} {tipo}</span>
            <span style="font-size:0.65rem; color:#475569;">V{ventana} | {pais}</span>
        </div>
        <div style="font-size:0.8rem; color:#CBD5E1; margin-top:3px;">
            <strong>{sku}</strong> {f'· {marca}' if marca else ''}
            {f'<span style="color:#64748B; font-size:0.72rem;"> · {row.get("modelo","")}</span>' if row.get("modelo") else ''}
        </div>
        <div class="alerta-detalle">{detalle}</div>
    </div>
    """, unsafe_allow_html=True)


# ============================================================
# RENDER PRINCIPAL
# ============================================================


# ============================================================
# PANEL BATCH VS STREAMING
# ============================================================

def render_batch_vs_streaming(kpis: pd.DataFrame, alertas: pd.DataFrame, ventana_actual: int):
    """Panel comparativo Batch vs Streaming para justificar la arquitectura."""

    import plotly.graph_objects as go

    INTERVALO_S      = 3        # segundos por ventana en streaming
    BATCH_HORAS      = 24       # frecuencia batch diaria
    BATCH_S          = BATCH_HORAS * 3600
    PRECIO_UNIDAD    = 250      # USD promedio por unidad sin vender

    # Métricas calculadas desde los datos reales del stream
    n_ventanas       = ventana_actual if ventana_actual > 0 else 1
    tiempo_stream_s  = n_ventanas * INTERVALO_S
    tiempo_stream_h  = tiempo_stream_s / 3600

    # Alertas reales generadas
    n_alertas_total  = len(alertas) if not alertas.empty else 0
    n_stockouts      = len(alertas[alertas['tipo_alerta'] == 'STOCKOUT']) if not alertas.empty else 0
    n_doh_bajo       = len(alertas[alertas['tipo_alerta'] == 'DOH_BAJO'])  if not alertas.empty else 0

    # Throughput real
    n_skus           = kpis['codigo_sku'].nunique() if not kpis.empty else 0
    eventos_por_s    = round(n_skus / INTERVALO_S, 1) if n_skus > 0 else 0

    # Tiempo de detección streaming vs batch
    latencia_stream  = INTERVALO_S
    latencia_batch   = BATCH_S
    factor_velocidad = round(latencia_batch / latencia_stream)

    # Stockouts detectados a tiempo — en batch se descubrirían al día siguiente
    horas_retraso_batch = BATCH_HORAS
    unidades_perdidas   = n_stockouts * horas_retraso_batch * 2  # ~2 uds/hora perdidas
    ahorro_deteccion    = unidades_perdidas * PRECIO_UNIDAD

    st.markdown("""
    <div style="background:linear-gradient(135deg,#0D1B2A,#0A1628);
         border:1px solid #1E3A5F; border-radius:16px; padding:1.5rem 2rem; margin-bottom:1.5rem;">
        <div style="font-family:'Space Mono',monospace; font-size:0.7rem; color:#00D4FF;
             text-transform:uppercase; letter-spacing:0.2em; margin-bottom:0.5rem;">
            ⚡ ¿Por qué Streaming Analytics vs Batch?
        </div>
        <div style="font-size:0.85rem; color:#94A3B8; line-height:1.6;">
            En un entorno batch tradicional, los datos se actualizan <b style="color:#FFB347;">cada 24 horas</b>.
            Con esta arquitectura de streaming, el sistema detecta eventos críticos en
            <b style="color:#4CFF91;">menos de 3 segundos</b> — una mejora de
            <b style="color:#00D4FF;">×{:,}</b> en velocidad de respuesta.
        </div>
    </div>
    """.format(factor_velocidad), unsafe_allow_html=True)

    # -- Métricas clave comparativas ------------------------------------------
    st.markdown('<div class="section-title">📊 Métricas Comparativas</div>', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"""
        <div class="hero-metric">
            <div style="font-size:0.65rem; color:#64748B; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.8rem;">
                ⏱️ Latencia de Detección
            </div>
            <div style="display:flex; justify-content:space-around; align-items:flex-end;">
                <div style="text-align:center;">
                    <div style="font-family:'Space Mono',monospace; font-size:1.4rem; color:#4CFF91; font-weight:700;">3s</div>
                    <div style="font-size:0.6rem; color:#4CFF91;">STREAMING</div>
                </div>
                <div style="font-size:1.2rem; color:#334155;">vs</div>
                <div style="text-align:center;">
                    <div style="font-family:'Space Mono',monospace; font-size:1.4rem; color:#FF4C4C; font-weight:700;">24h</div>
                    <div style="font-size:0.6rem; color:#FF4C4C;">BATCH</div>
                </div>
            </div>
            <div style="font-size:0.68rem; color:#00D4FF; margin-top:0.5rem; text-align:center;">
                ×{factor_velocidad:,} más rápido
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="hero-metric">
            <div style="font-size:0.65rem; color:#64748B; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.8rem;">
                🚀 Tasa de Procesamiento · SKUs procesados/seg
            </div>
            <div style="text-align:center;">
                <div style="font-family:'Space Mono',monospace; font-size:1.8rem; color:#00D4FF; font-weight:700;">{eventos_por_s}</div>
                <div style="font-size:0.65rem; color:#64748B;">SKUs procesados/seg</div>
            </div>
            <div style="font-size:0.68rem; color:#94A3B8; margin-top:0.5rem; text-align:center;">
                {n_skus:,} SKUs · ventana {INTERVALO_S}s
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="hero-metric">
            <div style="font-size:0.65rem; color:#64748B; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.8rem;">
                🔴 Reacción ante Stockout
            </div>
            <div style="display:flex; justify-content:space-around; align-items:flex-end;">
                <div style="text-align:center;">
                    <div style="font-family:'Space Mono',monospace; font-size:1.4rem; color:#4CFF91; font-weight:700;">&lt;1min</div>
                    <div style="font-size:0.6rem; color:#4CFF91;">STREAMING</div>
                </div>
                <div style="font-size:1.2rem; color:#334155;">vs</div>
                <div style="text-align:center;">
                    <div style="font-family:'Space Mono',monospace; font-size:1.4rem; color:#FF4C4C; font-weight:700;">+24h</div>
                    <div style="font-size:0.6rem; color:#FF4C4C;">BATCH</div>
                </div>
            </div>
            <div style="font-size:0.68rem; color:#FFB347; margin-top:0.5rem; text-align:center;">
                {n_stockouts} stockouts detectados al instante
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c4:
        st.markdown(f"""
        <div class="hero-metric">
            <div style="font-size:0.65rem; color:#64748B; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.8rem;">
                💰 Ahorro por Detección Temprana
            </div>
            <div style="text-align:center;">
                <div style="font-family:'Space Mono',monospace; font-size:1.5rem; color:#4CFF91; font-weight:700;">${ahorro_deteccion:,.0f}</div>
                <div style="font-size:0.65rem; color:#64748B;">USD estimado</div>
            </div>
            <div style="font-size:0.68rem; color:#94A3B8; margin-top:0.5rem; text-align:center;">
                vs ventas perdidas en batch
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)

    # -- Gráfico comparativo de detección en el tiempo ------------------------
    st.markdown('<div class="section-title">📈 Simulación: Detección Batch vs Streaming</div>', unsafe_allow_html=True)

    if not kpis.empty and kpis['ventana'].nunique() > 3:
        evol = kpis.groupby('ventana').agg(
            stockouts=('stock_actual', lambda x: (x == 0).sum()),
            ventas_tot=('ventas_acum', 'sum'),
        ).reset_index()

        # Simular batch: solo "ve" los datos cada 24h = cada 28800 ventanas (con 3s)
        # Para la simulacion usamos cada N ventanas
        ventanas_por_batch = max(1, int(BATCH_S / INTERVALO_S))
        evol['batch_visible'] = evol['ventana'].apply(
            lambda v: v if v % ventanas_por_batch == 0 else None
        )
        evol['stockouts_batch'] = evol['stockouts'].where(evol['batch_visible'].notna()).ffill()

        fig = go.Figure()

        # Línea streaming — detección inmediata
        fig.add_scatter(
            x=evol['ventana'], y=evol['stockouts'],
            name='🟢 Streaming (tiempo real)',
            line=dict(color='#4CFF91', width=2),
            fill='tozeroy', fillcolor='rgba(76,255,145,0.05)',
        )

        # Línea batch — solo actualiza periódicamente
        fig.add_scatter(
            x=evol['ventana'], y=evol['stockouts_batch'],
            name='🔴 Batch (simulado 24h)',
            line=dict(color='#FF4C4C', width=2, dash='dash'),
        )

        fig.update_layout(
            paper_bgcolor='#111827', plot_bgcolor='#111827',
            font=dict(color='#94A3B8', family='DM Sans'),
            margin=dict(l=0, r=0, t=10, b=40),
            height=280,
            legend=dict(bgcolor='rgba(0,0,0,0)', orientation='h', y=1.1),
            xaxis=dict(showgrid=True, gridcolor='#1E293B', title='Ventana'),
            yaxis=dict(showgrid=True, gridcolor='#1E293B', title='Stockouts detectados'),
        )
        st.plotly_chart(fig, width="stretch", config={'displayModeBar': False},
                        key=f'chart_batch_{time.time_ns()}')

        st.markdown(f"""
        <div style="font-size:0.72rem; color:#475569; font-family:'Space Mono',monospace;
             background:rgba(0,0,0,0.2); border-radius:8px; padding:0.8rem 1rem; margin-top:-0.5rem;">
            💡 La línea roja simula cómo un sistema batch detectaría los mismos stockouts
            con un retraso de 24 horas. En ese tiempo, el sistema de streaming ya generó
            <b style="color:#4CFF91;">{n_alertas_total} alertas</b> y
            <b style="color:#00D4FF;">tránsitos de reposición automáticos</b>.
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)

    # -- Tabla comparativa arquitecturas --------------------------------------
    st.markdown('<div class="section-title">🏗️ Comparativa de Arquitecturas</div>', unsafe_allow_html=True)

    col_t1, col_t2 = st.columns(2)

    with col_t1:
        st.markdown("""
        <div style="background:rgba(255,76,76,0.05); border:1px solid #FF4C4C33;
             border-radius:12px; padding:1.2rem;">
            <div style="font-family:'Space Mono',monospace; font-size:0.75rem;
                 color:#FF4C4C; margin-bottom:1rem;">❌ ARQUITECTURA BATCH (Actual)</div>
            <div style="font-size:0.8rem; color:#94A3B8; line-height:2;">
                ⏰ Actualización: cada 24h (o semanal)<br>
                🐌 Latencia: 24-168 horas<br>
                👁️ Visibilidad: datos del día anterior<br>
                🔴 Stockout: detectado 1-3 días después<br>
                📦 Sobrestock: capital inmovilizado semanas<br>
                🤖 Predicción: modelos offline, batch semanal<br>
                🌍 Multi-país: consolidación manual<br>
                ⚡ Reacción: reactiva (después del problema)
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_t2:
        st.markdown(f"""
        <div style="background:rgba(76,255,145,0.05); border:1px solid #4CFF9133;
             border-radius:12px; padding:1.2rem;">
            <div style="font-family:'Space Mono',monospace; font-size:0.75rem;
                 color:#4CFF91; margin-bottom:1rem;">✅ ARQUITECTURA STREAMING (Propuesta)</div>
            <div style="font-size:0.8rem; color:#94A3B8; line-height:2;">
                ⚡ Actualización: cada {INTERVALO_S} segundos<br>
                🚀 Latencia: &lt;{INTERVALO_S} segundos<br>
                👁️ Visibilidad: tiempo real<br>
                🟢 Stockout: detectado en &lt;1 minuto<br>
                📦 Sobrestock: alertado antes de acumular<br>
                🤖 Predicción: XGBoost online, R²=0.905<br>
                🌍 Multi-país: unificado y automático<br>
                ⚡ Reacción: proactiva (antes del problema)
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown('<br>', unsafe_allow_html=True)

    # -- Resumen estadístico del sistema en ejecución -------------------------
    st.markdown('<div class="section-title">📋 Resumen de la Sesión Actual</div>', unsafe_allow_html=True)

    r1, r2, r3, r4, r5 = st.columns(5)
    with r1: render_kpi_card('Ventanas Procesadas', f'{ventana_actual:,}', f'{tiempo_stream_h:.1f}h de stream')
    with r2: render_kpi_card('SKUs Monitoreados', f'{n_skus:,}', 'en tiempo real')
    with r3: render_kpi_card('Alertas Generadas', f'{n_alertas_total:,}', f'{n_stockouts} stockouts')
    with r4: render_kpi_card('Tasa de Procesamiento', f'{eventos_por_s}', 'SKUs procesados/seg')
    with r5: render_kpi_card('Tiempo Ahorrado', f'{factor_velocidad:,}×', 'vs batch 24h')


def render_dashboard(pais_sel, marca_sel, tier_sel, solo_con_ventas, estado_sku_sel, busqueda_sku):
    # Cargar datos
    kpis      = cargar_kpis()
    alertas   = cargar_alertas()
    prediccs  = cargar_predicciones()
    ordenes   = cargar_ordenes()

    # Esperar a que el consumer genere datos con la columna 'ventana'
    if kpis.empty or 'ventana' not in kpis.columns:
        st.markdown("""
        <div style="text-align:center; padding: 4rem 0;">
            <div style="font-size:2rem; margin-bottom:1rem;">ESPERANDO</div>
            <div style="font-size:1.2rem; color:#00D4FF; font-family:'Space Mono',monospace;">
                Esperando datos del pipeline...
            </div>
            <div style="font-size:0.85rem; color:#475569; margin-top:0.5rem;">
                Asegurate de que el producer y el consumer estan corriendo.
            </div>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(3)
        st.rerun()
        return

    # Ultima ventana
    ventana_actual = int(kpis['ventana'].max()) if not kpis.empty else 0
    ts_actual = kpis['timestamp'].max().strftime('%H:%M:%S') if not kpis.empty else '--:--:--'

    # -- HEADER ---------------------------------------------------------------
    col_h1, col_h2 = st.columns([3, 1])
    with col_h1:
        st.markdown(f"""
        <div class="dash-header">
            <div class="dash-title">📡 Supply Chain · Streaming Dashboard</div>
            <div class="dash-subtitle">Telco Multi-País · Monitorización en Tiempo Real</div>
        </div>
        """, unsafe_allow_html=True)
    with col_h2:
        st.markdown(f"""
        <div style="text-align:right; padding-top:1rem;">
            <div class="live-badge"><div class="live-dot"></div> LIVE · {ts_actual}</div>
            <div style="font-size:0.7rem; color:#475569; margin-top:6px; font-family:'Space Mono',monospace;">
                VENTANA {ventana_actual:03d}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Aplicar filtros recibidos como parámetros
    def aplicar_filtros(df: pd.DataFrame, aplicar_estado: bool = False) -> pd.DataFrame:
        if df.empty:
            return df
        if pais_sel and 'pais' in df.columns:
            df = df[df['pais'].isin(pais_sel)]
        if marca_sel and 'marca' in df.columns:
            df = df[df['marca'].isin(marca_sel)]
        if tier_sel and 'tier' in df.columns:
            df = df[df['tier'].isin(tier_sel)]
        if aplicar_estado and estado_sku_sel != 'Todos' and 'estado_sku' in df.columns:
            df = df[df['estado_sku'] == estado_sku_sel]
        if solo_con_ventas and 'ventas_acum' in df.columns:
            df = df[df['ventas_acum'] > 0]
        if busqueda_sku:
            termino = busqueda_sku.strip().lower()
            mask = pd.Series([False] * len(df), index=df.index)
            for col in ['codigo_sku', 'modelo']:
                if col in df.columns:
                    mask |= df[col].astype(str).str.lower().str.contains(termino, na=False)
            df = df[mask]
        return df

    kpis     = aplicar_filtros(kpis,     aplicar_estado=True)
    alertas  = aplicar_filtros(alertas,  aplicar_estado=True)
    prediccs = aplicar_filtros(prediccs, aplicar_estado=True)
    ordenes  = aplicar_filtros(ordenes,  aplicar_estado=True)

    # -- PESTAÑAS PRINCIPALES ------------------------------------------------
    tab1, tab2 = st.tabs(['📡 Dashboard en Tiempo Real', '⚡ Batch vs Streaming'])

    with tab2:
        render_batch_vs_streaming(kpis, alertas, ventana_actual)

    with tab1:

        # -- PANEL HERO ----------------------------------------------------------
        st.markdown('<div class="section-title">🤖 Impacto del Modelo Predictivo</div>', unsafe_allow_html=True)

        ultima_pred     = prediccs[prediccs['ventana'] == prediccs['ventana'].max()] if not prediccs.empty else pd.DataFrame()
        transitos_todos = ordenes[ordenes['tipo_orden'] == 'TRANSITO_SUGERIDO'] if not ordenes.empty else pd.DataFrame()

        en_riesgo          = ultima_pred[~ultima_pred['cobertura_ok']]['codigo_sku'].tolist() if not ultima_pred.empty else []
        con_transito       = transitos_todos['codigo_sku'].unique().tolist() if not transitos_todos.empty else []
        stockouts_evitados = len([s for s in en_riesgo if s in con_transito])

        if not ultima_pred.empty and 'demanda_pred_7d' in ultima_pred.columns:
            sobrestock_pred = ultima_pred[(ultima_pred['demanda_pred_7d'] > 0) & (ultima_pred['stock_actual'] > ultima_pred['demanda_pred_7d'] * 4)]
            n_sobrestock = len(sobrestock_pred)
        else:
            sobrestock_pred = pd.DataFrame()
            n_sobrestock = 0

        ahorro_usd = 0
        if n_sobrestock > 0 and not kpis.empty:
            ultima_kpis = kpis[kpis['ventana'] == kpis['ventana'].max()]
            filas_sobre = ultima_kpis[ultima_kpis['codigo_sku'].isin(sobrestock_pred['codigo_sku'].tolist())]
            if not filas_sobre.empty:
                exceso_uds = float(filas_sobre['stock_actual'].sum()) - float(sobrestock_pred['demanda_pred_7d'].sum()) * 4
                ahorro_usd = max(0, exceso_uds * float(filas_sobre['precio_prom'].mean()))

        # Métricas reales del modelo XGBoost entrenado en fase 3
        # Evaluado sobre datos históricos reales (test set temporal)
        MODELO_R2   = 0.905
        MODELO_MAE  = 3.529
        MODELO_MAPE = 11.194
        precision   = round(100 - MODELO_MAPE, 1)  # 88.8%

        h1, h2, h3, h4 = st.columns(4)
        COK  = '#4CFF91'; CWAR = '#FFB347'; CINF = '#00D4FF'; COFF = '#64748B'

        with h1:
            c = COK if stockouts_evitados > 0 else COFF
            sub = ('✅ SKUs en riesgo con tránsito activo' if stockouts_evitados > 0 else '— sin riesgo detectado')
            st.markdown(f'''<div class="hero-metric"><div class="hero-metric-value" style="color:{c};">{stockouts_evitados}</div><div class="hero-metric-label">Stockouts Evitados</div><div class="hero-metric-sub" style="color:{c};">{sub}</div></div>''', unsafe_allow_html=True)

        with h2:
            c = CWAR if n_sobrestock > 0 else COK
            sub = (f'⚠️ {n_sobrestock} SKUs con exceso según predicción' if n_sobrestock > 0 else '✅ stock alineado con demanda')
            st.markdown(f'''<div class="hero-metric"><div class="hero-metric-value" style="color:{c};">{n_sobrestock}</div><div class="hero-metric-label">Sobrestock Detectado</div><div class="hero-metric-sub" style="color:{c};">{sub}</div></div>''', unsafe_allow_html=True)

        with h3:
            c = CINF if ahorro_usd > 0 else COFF
            sub = ('💰 capital liberable por ajuste de stock' if ahorro_usd > 0 else '— calculando...')
            st.markdown(f'''<div class="hero-metric"><div class="hero-metric-value" style="color:{c}; font-size:1.5rem;">${ahorro_usd:,.0f}</div><div class="hero-metric-label">Ahorro Estimado USD</div><div class="hero-metric-sub" style="color:{c};">{sub}</div></div>''', unsafe_allow_html=True)

        with h4:
            st.markdown(f'''<div class="hero-metric">
                <div class="hero-metric-value" style="color:#4CFF91;">88.8%</div>
                <div class="hero-metric-label">Precisión del Modelo</div>
                <div class="hero-metric-sub" style="color:#4CFF91;">🎯 MAPE=11.2% sobre datos reales</div>
                <div style="font-size:0.62rem; color:#334155; margin-top:4px;">R²=0.905 · MAE=3.53 · fase 3</div>
            </div>''', unsafe_allow_html=True)

        parts = []
        if stockouts_evitados > 0: parts.append(f'{stockouts_evitados} SKUs en riesgo — tránsitos generados a tiempo')
        if ahorro_usd > 0:         parts.append(f'${ahorro_usd:,.0f} USD en exceso de stock identificado')
        if precision > 0:          parts.append(f'modelo XGBoost con 88.8% de precisión (R²=0.905, evaluado en fase 3)')
        if parts:
            st.markdown(f'<div class="hero-insight">💡 {" · ".join(parts)}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="no-data">⏳ Acumulando datos para calcular impacto del modelo...</div>', unsafe_allow_html=True)

        st.markdown('<br>', unsafe_allow_html=True)

        # -- KPIs RESUMEN ---------------------------------------------------------
        st.markdown('<div class="section-title">KPIs Resumen</div>', unsafe_allow_html=True)

        if kpis.empty or len(kpis) == 0:
            st.markdown('<div class="no-data">⏳ Esperando datos del consumer... o sin resultados para el filtro actual</div>', unsafe_allow_html=True)
        else:
            # Ultima ventana
            ultima = kpis[kpis['ventana'] == kpis['ventana'].max()].copy()
            if ultima.empty:
                st.markdown('<div class="no-data">Sin datos para los filtros seleccionados</div>', unsafe_allow_html=True)
            # Excluir SKUs sin metadata completa (familia/marca/tier en NA)
            for col in ['marca', 'familia', 'tier']:
                if col in ultima.columns:
                    ultima = ultima[~ultima[col].astype(str).isin(['<NA>', 'nan', 'None', ''])]

            total_skus    = len(ultima)
            skus_activos  = int((ultima['ventas_acum'] > 0).sum())
            skus_stockout = int((ultima['stock_actual'] == 0).sum())
            ventas_tot    = int(ultima['ventas_acum'].sum())
            ingreso_tot   = ultima['ingreso_acum'].sum()
            doh_validos   = ultima[ultima['doh'] < 999]['doh']
            doh_prom      = doh_validos.mean() if len(doh_validos) > 0 else 0

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            with c1: render_kpi_card('SKUs Monitoreados', f'{total_skus:,}')
            with c2: render_kpi_card('SKUs con Ventas', f'{skus_activos:,}', f'{skus_activos/total_skus*100:.0f}% activos' if total_skus else None)
            with c3: render_kpi_card('En Stockout', f'{skus_stockout:,}', '⚠️ requieren acción' if skus_stockout > 0 else '✅ sin stockouts')
            with c4: render_kpi_card('Ventas Acum.', f'{ventas_tot:,}', 'unidades')
            with c5: render_kpi_card('Ingresos Acum.', f'${ingreso_tot:,.0f}', 'USD acumulado')
            with c6: render_kpi_card('DOH Promedio', f'{doh_prom:.1f}d', 'días de cobertura' if doh_prom > 0 else '--')

            st.markdown('<br>', unsafe_allow_html=True)

            # -- Gráficos de KPIs -------------------------------------------------
            col_g1, col_g2 = st.columns(2)

            with col_g1:
                st.markdown('<div class="section-title">Top 15 SKUs · Ventas Acumuladas</div>', unsafe_allow_html=True)
                cols_top15 = ['codigo_sku', 'marca', 'ventas_acum', 'stock_actual']
                if 'modelo' in ultima.columns:
                    cols_top15.append('modelo')
                ultima['ventas_acum'] = pd.to_numeric(ultima['ventas_acum'], errors='coerce').fillna(0)
                top15 = ultima.nlargest(15, 'ventas_acum')[cols_top15].copy()
                top15['ventas_acum'] = top15['ventas_acum'].astype(int)
                if 'modelo' in top15.columns:
                    top15['label'] = top15['codigo_sku'].astype(str) + ' · ' + top15['modelo'].astype(str).str[:20]
                else:
                    top15['label'] = top15['codigo_sku'].astype(str) + ' · ' + top15['marca'].astype(str).str[:8]
                modelo_col = top15['modelo'].astype(str) if 'modelo' in top15.columns else pd.Series([''] * len(top15))
                top15['tooltip'] = (
                    '<b>' + top15['codigo_sku'].astype(str) + '</b><br>' +
                    top15['marca'].astype(str) +
                    top15.apply(lambda r: f'<br>{r["modelo"]}' if 'modelo' in r and r['modelo'] not in ('', 'nan', 'None') else '', axis=1) +
                    '<br>Ventas: ' + top15['ventas_acum'].astype(str) + ' uds' +
                    '<br>Stock: ' + top15['stock_actual'].astype(str) + ' uds'
                )

                # Colores degradado manual sin colorscale (evita bug null de Plotly)
                max_v = top15['ventas_acum'].max() or 1
                colores = [
                    f'rgba(0, {int(100 + 112 * v / max_v)}, {int(180 + 75 * v / max_v)}, 0.85)'
                    for v in top15['ventas_acum']
                ]
                fig = go.Figure()
                fig.add_bar(
                    x=top15['ventas_acum'],
                    y=top15['label'],
                    orientation='h',
                    marker=dict(color=colores),
                    text=top15['ventas_acum'].apply(lambda x: f'{int(x):,}' if pd.notna(x) and x > 0 else ''),
                    textposition='outside',
                    textfont=dict(color='#94A3B8', size=10, family='Space Mono'),
                    customdata=top15[['tooltip']],
                    hovertemplate='%{customdata[0]}<extra></extra>',
                )
                fig.update_layout(
                    paper_bgcolor='#111827', plot_bgcolor='#111827',
                    font=dict(color='#94A3B8', family='DM Sans'),
                    margin=dict(l=0, r=60, t=10, b=10),
                    height=320,
                    yaxis=dict(autorange='reversed', tickfont=dict(size=10)),
                    xaxis=dict(showgrid=True, gridcolor='#1E293B', zeroline=False),
                )
                st.plotly_chart(fig, width="stretch", config={'displayModeBar': False}, key=f'chart_top15_{time.time_ns()}')

            with col_g2:
                st.markdown('<div class="section-title">Distribución DOH por Marca</div>', unsafe_allow_html=True)
                doh_df = ultima[ultima['doh'] < 200].copy()

                if not doh_df.empty and 'marca' in doh_df.columns:
                    fig2 = px.box(
                        doh_df, x='marca', y='doh',
                        color_discrete_sequence=['#00D4FF'],
                    )
                    fig2.update_layout(
                        paper_bgcolor='#111827', plot_bgcolor='#111827',
                        font=dict(color='#94A3B8', family='DM Sans'),
                        margin=dict(l=0, r=0, t=10, b=40),
                        height=320,
                        xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(size=9)),
                        yaxis=dict(showgrid=True, gridcolor='#1E293B', title='DOH (días)'),
                        showlegend=False,
                    )
                    st.plotly_chart(fig2, width="stretch", config={'displayModeBar': False}, key=f'chart_doh_box_{time.time_ns()}')
                else:
                        st.markdown('<div class="no-data">Sin datos de DOH aún</div>', unsafe_allow_html=True)

        # -- Evolución KPIs en el tiempo --------------------------------------
        st.markdown('<div class="section-title">Evolución · Ventas & DOH por Ventana</div>', unsafe_allow_html=True)

        if kpis['ventana'].nunique() > 1:
            evol = kpis.groupby('ventana').agg(
                ventas_tot=('ventas_acum', 'sum'),
                doh_prom=('doh', lambda x: x[x < 999].mean()),
                stockouts=('stock_actual', lambda x: (x == 0).sum()),
            ).reset_index()

            fig3 = go.Figure()
            fig3.add_scatter(
                x=evol['ventana'], y=evol['ventas_tot'],
                name='Ventas acum.', line=dict(color='#00D4FF', width=2),
                fill='tozeroy', fillcolor='rgba(0,212,255,0.05)',
            )
            fig3.add_scatter(
                x=evol['ventana'], y=evol['doh_prom'],
                name='DOH prom.', yaxis='y2',
                line=dict(color='#FFB347', width=2, dash='dot'),
            )
            fig3.update_layout(
                paper_bgcolor='#111827', plot_bgcolor='#111827',
                font=dict(color='#94A3B8', family='DM Sans'),
                margin=dict(l=0, r=60, t=10, b=10),
                height=220,
                legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(size=11)),
                xaxis=dict(showgrid=True, gridcolor='#1E293B', title='Ventana'),
                yaxis=dict(showgrid=True, gridcolor='#1E293B', title='Ventas'),
                yaxis2=dict(overlaying='y', side='right', title='DOH (días)', showgrid=False),
            )
            st.plotly_chart(fig3, width="stretch", config={'displayModeBar': False}, key=f'chart_evol_{time.time_ns()}')

            # -- ALERTAS + PREDICCIONES -----------------------------------------------
            st.markdown('<br>', unsafe_allow_html=True)
            col_a, col_p = st.columns([1, 1])

            with col_a:
                st.markdown('<div class="section-title">Alertas en Tiempo Real</div>', unsafe_allow_html=True)
            if alertas.empty:
                st.markdown('<div class="no-data">✅ Sin alertas registradas</div>', unsafe_allow_html=True)
            else:
                # Últimas 20 alertas, más recientes primero
                ultimas = alertas.tail(20).iloc[::-1]

                # Resumen por tipo
                resumen = alertas['tipo_alerta'].value_counts()
                cols_r = st.columns(len(resumen))
                colores_badge = {
                    'STOCKOUT': '#FF4C4C', 'DOH_BAJO': '#FFB347',
                    'SOBRESTOCK': '#64748B', 'PICO_DEMANDA': '#00D4FF',
                    'COBERTURA_INSUFICIENTE': '#FFB347',
                }
                for i, (tipo, cnt) in enumerate(resumen.items()):
                    color = colores_badge.get(tipo, '#64748B')
                    cols_r[i].markdown(f"""
                    <div style="text-align:center; background:rgba(0,0,0,0.3);
                         border:1px solid {color}33; border-radius:8px; padding:6px 4px;">
                        <div style="font-size:1.2rem; font-family:'Space Mono',monospace;
                             color:{color}; font-weight:700;">{cnt}</div>
                        <div style="font-size:0.6rem; color:#475569; text-transform:uppercase;
                             letter-spacing:0.08em;">{tipo.replace('_',' ')}</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown('<br>', unsafe_allow_html=True)
                container = st.container(height=380)
                with container:
                    for _, row in ultimas.iterrows():
                        render_alerta(row)

            with col_p:
                st.markdown('<div class="section-title">Predicciones XGBoost · Próxima Semana</div>', unsafe_allow_html=True)
            if prediccs.empty:
                st.markdown('<div class="no-data">⏳ Sin predicciones aún (requiere historial mínimo)</div>', unsafe_allow_html=True)
            else:
                ultimas_pred = prediccs[prediccs['ventana'] == prediccs['ventana'].max()]

                # Métricas rápidas
                ok_count = int(ultimas_pred['cobertura_ok'].sum())
                no_ok    = len(ultimas_pred) - ok_count
                c1, c2 = st.columns(2)
                c1.markdown(f"""
                <div style="background:rgba(76,255,145,0.08); border:1px solid #4CFF9133;
                     border-radius:8px; padding:10px; text-align:center;">
                    <div style="font-size:1.4rem; font-family:'Space Mono',monospace;
                         color:#4CFF91; font-weight:700;">{ok_count}</div>
                    <div style="font-size:0.65rem; color:#475569; text-transform:uppercase;">Cobertura OK</div>
                </div>
                """, unsafe_allow_html=True)
                c2.markdown(f"""
                <div style="background:rgba(255,76,76,0.08); border:1px solid #FF4C4C33;
                     border-radius:8px; padding:10px; text-align:center;">
                    <div style="font-size:1.4rem; font-family:'Space Mono',monospace;
                         color:#FF4C4C; font-weight:700;">{no_ok}</div>
                    <div style="font-size:0.65rem; color:#475569; text-transform:uppercase;">Sin Cobertura</div>
                </div>
                """, unsafe_allow_html=True)

                st.markdown('<br>', unsafe_allow_html=True)

                # Tabla de predicciones con semáforo — más clara que el gráfico de barras
                top_pred = ultimas_pred.nlargest(15, 'demanda_pred_7d').copy()
                if 'modelo' not in top_pred.columns:
                    top_pred['modelo'] = ''

                # Calcular cobertura en días
                top_pred['cobertura_dias'] = (
                    top_pred['stock_actual'] / (top_pred['demanda_pred_7d'] / 7)
                ).replace([float('inf'), float('nan')], 999).round(0).astype(int)

                top_pred['semaforo'] = top_pred['cobertura_ok'].map({
                    True: '🟢', False: '🔴'
                })

                cols_pred = ['semaforo', 'pais', 'codigo_sku', 'modelo', 'marca',
                             'stock_actual', 'demanda_pred_7d', 'cobertura_dias']
                cols_pred = [c for c in cols_pred if c in top_pred.columns]

                st.dataframe(
                    top_pred[cols_pred],
                    width="stretch",
                    hide_index=True,
                    column_config={
                        'semaforo'        : st.column_config.TextColumn('', width='small'),
                        'pais'            : st.column_config.TextColumn('País', width='small'),
                        'codigo_sku'      : st.column_config.TextColumn('SKU'),
                        'modelo'          : st.column_config.TextColumn('Modelo'),
                        'marca'           : st.column_config.TextColumn('Marca'),
                        'stock_actual'    : st.column_config.NumberColumn('Stock', format='%d'),
                        'demanda_pred_7d' : st.column_config.NumberColumn('Pred. 7d', format='%.1f'),
                        'cobertura_dias'  : st.column_config.NumberColumn('Cob. (días)', format='%d'),
                    }
                )

            # -- ÓRDENES DE REPOSICIÓN ------------------------------------------------
            st.markdown('<br>', unsafe_allow_html=True)
            st.markdown('<div class="section-title">Órdenes de Reposición Sugeridas</div>', unsafe_allow_html=True)

            if ordenes.empty:
                st.markdown('<div class="no-data">Sin órdenes sugeridas aún</div>', unsafe_allow_html=True)
            else:
                col_o1, col_o2 = st.columns([1, 2])

                with col_o1:
                    sustitutos = ordenes[ordenes['tipo_orden'] == 'SUSTITUTO_SUGERIDO'].copy()
                if not sustitutos.empty:
                    st.markdown("**🔄 Sustitutos Sugeridos**")
                    con_sust = sustitutos[sustitutos['sku_sustituto'] != 'SIN_SUSTITUTO'].copy()
                    sin_sust = sustitutos[sustitutos['sku_sustituto'] == 'SIN_SUSTITUTO']

                    if not con_sust.empty:
                        cols_s = ['pais', 'codigo_sku', 'modelo', 'estado_sku', 'marca', 'sku_sustituto', 'modelo_sustituto', 'dem_sustituto', 'motivo']
                        cols_s = [c for c in cols_s if c in con_sust.columns]
                        st.dataframe(
                            con_sust[cols_s].tail(15),
                            width="stretch",
                            hide_index=True,
                            column_config={
                                'estado_sku': st.column_config.TextColumn('Estado'),
                                'modelo'    : st.column_config.TextColumn('Modelo SKU'),
                                'modelo_sustituto': st.column_config.TextColumn('Modelo Sustituto'),
                            }
                        )

                    if not sin_sust.empty:
                        n_sin = len(sin_sust)
                        marcas_sin = sin_sust['marca'].value_counts().head(3).to_dict()
                        marcas_str = ', '.join([f'{m} ({c})' for m, c in marcas_sin.items()])
                        st.markdown(f"""
                        <div style="background:rgba(100,116,139,0.1); border:1px solid #1E293B;
                             border-radius:8px; padding:10px; margin-top:8px;">
                            <div style="font-size:0.7rem; color:#64748B; text-transform:uppercase; letter-spacing:0.08em;">Top SKUs · Cantidad a Pedir</div>
                            <div style="font-size:1.1rem; font-family:'Space Mono',monospace; color:#94A3B8; font-weight:700;">{n_sin} SKUs</div>
                            <div style="font-size:0.72rem; color:#475569; margin-top:4px;">Marcas: {marcas_str}</div>
                        </div>
                        """, unsafe_allow_html=True)

                # Gráfico top SKUs a reponer
                transitos = ordenes[ordenes['tipo_orden'] == 'TRANSITO_SUGERIDO'].copy()
                if not transitos.empty:
                    top_ord = transitos.nlargest(8, 'cantidad_sugerida').copy()
                    top_ord['label'] = top_ord['codigo_sku'].astype(str) + ' · ' + top_ord.get('modelo', top_ord['marca']).astype(str).str[:15]
                    fig_o = px.bar(
                        top_ord, x='cantidad_sugerida', y='label',
                        orientation='h', color='cantidad_sugerida',
                        color_continuous_scale=[[0, '#0D3B4F'], [1, '#00D4FF']],
                        title='Top SKUs · Cantidad a Pedir',
                    )
                    fig_o.update_layout(
                        paper_bgcolor='#111827', plot_bgcolor='#111827',
                        font=dict(color='#94A3B8', family='DM Sans', size=10),
                        margin=dict(l=0, r=0, t=30, b=10),
                        height=260,
                        coloraxis_showscale=False,
                        yaxis=dict(autorange='reversed'),
                        xaxis=dict(showgrid=True, gridcolor='#1E293B'),
                        title_font=dict(size=11, color='#64748B'),
                    )
                    st.plotly_chart(fig_o, width="stretch", config={'displayModeBar': False}, key=f'chart_ordenes_{time.time_ns()}')

                with col_o2:
                    transitos = ordenes[ordenes['tipo_orden'] == 'TRANSITO_SUGERIDO'].copy()
                if not transitos.empty:
                    st.markdown("**🚚 Tránsitos Sugeridos**")
                    cols_show = ['ventana', 'pais', 'codigo_sku', 'modelo', 'marca', 'stock_actual',
                                 'demanda_semanal', 'cantidad_sugerida',
                                 'cobertura_actual_dias', 'cobertura_objetivo_dias', 'ventana_llegada']
                    cols_show = [c for c in cols_show if c in transitos.columns]
                    st.dataframe(
                        transitos[cols_show].tail(30),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            'ventana'                : st.column_config.NumberColumn('Ventana', format='%d'),
                            'modelo'                 : st.column_config.TextColumn('Modelo'),
                            'stock_actual'           : st.column_config.NumberColumn('Stock', format='%d'),
                            'demanda_semanal'        : st.column_config.NumberColumn('Dem/Sem', format='%.1f'),
                            'cantidad_sugerida'      : st.column_config.NumberColumn('Pedir', format='%d'),
                            'cobertura_actual_dias'  : st.column_config.NumberColumn('Cob.Act (d)', format='%.0f'),
                            'cobertura_objetivo_dias': st.column_config.NumberColumn('Cob.Obj (d)', format='%.0f'),
                            'ventana_llegada'        : st.column_config.NumberColumn('Llega V.', format='%d'),
                        }
                    )

        # -- FOOTER ---------------------------------------------------------------
        st.markdown(f"""
        <div style="text-align:center; color:#1E293B; font-size:0.65rem;
         font-family:'Space Mono',monospace; margin-top:2rem; padding-top:1rem;
         border-top:1px solid #0F172A;">
        SUPPLY CHAIN STREAMING ANALYTICS · TFM UOC 2026 · AUTO-REFRESH {REFRESH_SEGUNDOS}s
        </div>
        """, unsafe_allow_html=True)


# ============================================================
# FILTROS FIJOS — fuera del loop para evitar DuplicateKey
# ============================================================

_kpis_tmp   = cargar_kpis()
paises_disp = sorted(_kpis_tmp['pais'].dropna().unique().tolist())   if not _kpis_tmp.empty else []
marcas_disp = sorted(_kpis_tmp['marca'].dropna().unique().tolist())  if not _kpis_tmp.empty and 'marca' in _kpis_tmp.columns else []
tiers_disp  = sorted(_kpis_tmp['tier'].dropna().unique().tolist())   if not _kpis_tmp.empty and 'tier'  in _kpis_tmp.columns else []

st.markdown('<div class="section-title">⚙️ Filtros</div>', unsafe_allow_html=True)
col_f1, col_f2, col_f3, col_f4, col_f5, col_f6 = st.columns([2, 3, 2, 2, 2, 2])
with col_f1:
    pais_sel = st.multiselect('🌎 País', paises_disp, default=paises_disp, key='filtro_pais')
with col_f2:
    marca_sel = st.multiselect('📱 Marca', marcas_disp, default=marcas_disp, key='filtro_marca')
with col_f3:
    tier_sel = st.multiselect('🏷️ Tier/Gama', tiers_disp, default=tiers_disp, key='filtro_tier')
with col_f4:
    estados_disp_global = ['Todos', 'ACTIVE', 'NEW']
    estado_sku_sel = st.selectbox('🏷️ Estado SKU', estados_disp_global, key='filtro_estado_global')
with col_f5:
    busqueda_sku = st.text_input('🔍 Buscar SKU / Modelo', placeholder='ej: CO668A o iPhone 15', key='filtro_busqueda')
with col_f6:
    st.markdown('<br>', unsafe_allow_html=True)
    solo_con_ventas = st.toggle('Solo con ventas', value=False, key='filtro_ventas')

st.markdown('---')

# ============================================================
# RENDER + AUTO-REFRESH
# ============================================================

render_dashboard(pais_sel, marca_sel, tier_sel, solo_con_ventas, estado_sku_sel, busqueda_sku)

# Auto-refresh usando meta refresh del navegador — sin duplicados
st.markdown(
    f'<meta http-equiv="refresh" content="{REFRESH_SEGUNDOS}">',
    unsafe_allow_html=True
)