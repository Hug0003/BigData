"""
Dashboard Streamlit — Aircraft Tracking
Lit les données depuis PostgreSQL (table aircraft_states) et affiche :
  - KPIs globaux
  - Carte mondiale des positions
  - Top pays d'immatriculation et pays survolés
  - Table filtrée

Usage:
    streamlit run dashboard.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

st.set_page_config(
    page_title="Aircraft Tracking",
    page_icon="✈",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Données
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine():
    url = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'admin')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'admin123')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'bigdata_db')}"
    )
    return create_engine(url)


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    """Charge le snapshot le plus récent depuis aircraft_states."""
    query = """
        SELECT
            icao24, callsign,
            origin_country, current_country, current_country_code,
            longitude, latitude,
            baro_altitude, velocity, true_track, vertical_rate,
            on_ground, data_timestamp
        FROM aircraft_states
        WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM aircraft_states)
    """
    return pd.read_sql(query, get_engine())


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.title("✈ Aircraft Tracking Dashboard")

try:
    df = load_data()
except Exception as e:
    st.error(f"Connexion PostgreSQL impossible : {e}")
    st.info("Lance Docker (`docker-compose up -d`), la migration (`python src/migrate.py`) puis l'ETL (`python etl.py`).")
    st.stop()

if df.empty:
    st.warning("Aucune donnée en base. Lance `python src/main.py` puis `python etl.py`.")
    st.stop()

snapshot_time = pd.to_datetime(df["data_timestamp"].iloc[0])

# --- KPIs ----------------------------------------------------------------
k1, k2, k3, k4 = st.columns(4)
k1.metric("Avions en vol", f"{len(df):,}")
k2.metric("Pays d'origine", df["origin_country"].nunique())
k3.metric("Pays survolés", df["current_country"].nunique())
k4.metric("Snapshot", snapshot_time.strftime("%Y-%m-%d %H:%M UTC"))

st.divider()

# --- Carte ---------------------------------------------------------------
st.subheader("Positions en temps réel")

fig_map = px.scatter_mapbox(
    df,
    lat="latitude",
    lon="longitude",
    color="origin_country",
    hover_name="callsign",
    hover_data={
        "icao24": True,
        "origin_country": True,
        "current_country": True,
        "baro_altitude": ":.0f",
        "velocity": ":.0f",
        "latitude": False,
        "longitude": False,
    },
    zoom=1,
    opacity=0.8,
)
fig_map.update_traces(marker=dict(size=4))
fig_map.update_layout(
    mapbox_style="open-street-map",
    showlegend=False,
    height=560,
    margin=dict(l=0, r=0, t=0, b=0),
)
st.plotly_chart(fig_map, width="stretch")

st.divider()

# --- Graphiques pays -----------------------------------------------------
col_l, col_r = st.columns(2)

with col_l:
    st.subheader("Top 15 — Pays d'immatriculation")
    top_origin = (
        df["origin_country"]
        .value_counts()
        .head(15)
        .rename_axis("Pays")
        .reset_index(name="Avions")
    )
    fig_origin = px.bar(
        top_origin, x="Avions", y="Pays", orientation="h",
        color="Avions", color_continuous_scale="Blues",
    )
    fig_origin.update_layout(
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        height=460,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_origin, width="stretch")

with col_r:
    st.subheader("Top 15 — Pays actuellement survolés")
    top_current = (
        df["current_country"]
        .value_counts()
        .head(15)
        .rename_axis("Pays")
        .reset_index(name="Avions")
    )
    fig_current = px.bar(
        top_current, x="Avions", y="Pays", orientation="h",
        color="Avions", color_continuous_scale="Oranges",
    )
    fig_current.update_layout(
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        height=460,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_current, width="stretch")

st.divider()

# --- Table filtrée -------------------------------------------------------
st.subheader("Données détaillées")

f1, f2, f3 = st.columns(3)
with f1:
    origins = ["Tous"] + sorted(df["origin_country"].dropna().unique().tolist())
    sel_origin = st.selectbox("Pays d'immatriculation", origins)
with f2:
    currents = ["Tous"] + sorted(df["current_country"].dropna().unique().tolist())
    sel_current = st.selectbox("Pays survolé", currents)
with f3:
    search = st.text_input("Callsign / ICAO24", placeholder="ex: AFR, 3c6444")

df_view = df.copy()
if sel_origin != "Tous":
    df_view = df_view[df_view["origin_country"] == sel_origin]
if sel_current != "Tous":
    df_view = df_view[df_view["current_country"] == sel_current]
if search:
    mask = (
        df_view["callsign"].str.contains(search, case=False, na=False)
        | df_view["icao24"].str.contains(search, case=False, na=False)
    )
    df_view = df_view[mask]

st.caption(f"{len(df_view):,} avions affichés")
st.dataframe(
    df_view[[
        "icao24", "callsign", "origin_country", "current_country",
        "longitude", "latitude", "baro_altitude", "velocity", "true_track",
    ]].rename(columns={
        "icao24": "ICAO24",
        "callsign": "Callsign",
        "origin_country": "Pays d'origine",
        "current_country": "Pays survolé",
        "longitude": "Lon",
        "latitude": "Lat",
        "baro_altitude": "Alt (m)",
        "velocity": "Vitesse (m/s)",
        "true_track": "Cap (°)",
    }),
    width="stretch",
    hide_index=True,
)
