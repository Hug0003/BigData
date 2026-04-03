import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Aircraft Tracking",
    page_icon="✈",
    layout="wide",
)

st.title("✈ Aircraft Tracking Dashboard")

# ── Connexion ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    default_host = "postgres" if os.path.exists("/.dockerenv") else "localhost"
    host = os.getenv("POSTGRES_HOST", default_host)
    url = f"postgresql://{os.getenv('POSTGRES_USER','admin')}:{os.getenv('POSTGRES_PASSWORD','admin123')}@{host}:5432/postgres"
    return create_engine(url)

engine = get_engine()

# ── Données du dernier snapshot ──────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_latest_snapshot():
    with engine.connect() as conn:
        ts = conn.execute(text(
            "SELECT MAX(data_timestamp) FROM aircraft_states"
        )).scalar()
        df = pd.read_sql(
            text("SELECT * FROM aircraft_states WHERE data_timestamp = :ts"),
            conn, params={"ts": ts}
        )
    return df, ts

@st.cache_data(ttl=60)
def load_hourly_counts():
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                date_trunc('hour', data_timestamp AT TIME ZONE 'Europe/Paris') AS hour,
                COUNT(DISTINCT data_timestamp) AS snapshots,
                COUNT(*) AS total_records,
                AVG(COUNT(*)) OVER (
                    ORDER BY date_trunc('hour', data_timestamp AT TIME ZONE 'Europe/Paris')
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) AS rolling_avg
            FROM aircraft_states
            GROUP BY 1
            ORDER BY 1
        """), conn)
    return df

@st.cache_data(ttl=60)
def load_top_countries(n=15):
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT origin_country, COUNT(DISTINCT icao24) AS unique_aircraft
            FROM aircraft_states
            WHERE data_timestamp = (SELECT MAX(data_timestamp) FROM aircraft_states)
            GROUP BY origin_country
            ORDER BY unique_aircraft DESC
            LIMIT :n
        """), conn, params={"n": n})
    return df

# ── Chargement ───────────────────────────────────────────────────────────────
with st.spinner("Chargement des données..."):
    df, latest_ts = load_latest_snapshot()
    df_hourly = load_hourly_counts()
    df_countries = load_top_countries()

# ── KPIs ─────────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Avions (dernier snapshot)", f"{len(df):,}")
col2.metric("En vol", f"{df[df['on_ground'] == False].shape[0]:,}")
col3.metric("Au sol", f"{df[df['on_ground'] == True].shape[0]:,}")
col4.metric("Pays d'origine", f"{df['origin_country'].nunique()}")
col5.metric("Dernier snapshot", latest_ts.strftime("%H:%M:%S") if latest_ts else "—")

st.divider()

# ── Carte ─────────────────────────────────────────────────────────────────────
st.subheader("Position des aéronefs")

df_map = df.dropna(subset=["latitude", "longitude"]).copy()
df_map["on_ground_label"] = df_map["on_ground"].map({True: "Sol", False: "En vol"})
df_map["altitude_label"] = df_map["baro_altitude"].fillna(0).round(0).astype(int)
df_map["velocity_kmh"] = (df_map["velocity"].fillna(0) * 3.6).round(0).astype(int)

fig_map = px.scatter_map(
    df_map,
    lat="latitude",
    lon="longitude",
    color="on_ground_label",
    color_discrete_map={"En vol": "#00b4d8", "Sol": "#e63946"},
    hover_name="callsign",
    hover_data={
        "icao24": True,
        "origin_country": True,
        "current_country": True,
        "altitude_label": True,
        "velocity_kmh": True,
        "on_ground_label": False,
        "latitude": False,
        "longitude": False,
    },
    labels={
        "on_ground_label": "Statut",
        "altitude_label": "Altitude (m)",
        "velocity_kmh": "Vitesse (km/h)",
        "origin_country": "Pays d'origine",
        "current_country": "Pays actuel",
    },
    zoom=2,
    height=500,
    map_style="carto-darkmatter",
)
fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, legend_title_text="Statut")
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ── Graphiques ────────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 15 pays d'origine")
    fig_countries = px.bar(
        df_countries,
        x="unique_aircraft",
        y="origin_country",
        orientation="h",
        color="unique_aircraft",
        color_continuous_scale="Blues",
        labels={"unique_aircraft": "Aéronefs uniques", "origin_country": "Pays"},
    )
    fig_countries.update_layout(
        coloraxis_showscale=False,
        yaxis={"categoryorder": "total ascending"},
        margin={"t": 10},
    )
    st.plotly_chart(fig_countries, use_container_width=True)

with col_right:
    st.subheader("Distribution des altitudes (en vol)")
    df_flying = df_map[df_map["on_ground"] == False]
    df_flying = df_flying[df_flying["baro_altitude"] > 0]
    fig_alt = px.histogram(
        df_flying,
        x="baro_altitude",
        nbins=40,
        labels={"baro_altitude": "Altitude barométrique (m)", "count": "Nombre"},
        color_discrete_sequence=["#00b4d8"],
    )
    fig_alt.update_layout(margin={"t": 10}, bargap=0.05)
    st.plotly_chart(fig_alt, use_container_width=True)

st.divider()

# ── Évolution dans le temps ────────────────────────────────────────────────────
st.subheader("Aéronefs enregistrés par heure")
fig_time = go.Figure()
fig_time.add_trace(go.Bar(
    x=df_hourly["hour"],
    y=df_hourly["total_records"],
    name="Total enregistrements",
    marker_color="#00b4d8",
    opacity=0.6,
))
fig_time.add_trace(go.Scatter(
    x=df_hourly["hour"],
    y=df_hourly["rolling_avg"],
    name="Moyenne glissante (3h)",
    line=dict(color="#e63946", width=2),
))
fig_time.update_layout(
    xaxis_title="Heure",
    yaxis_title="Enregistrements",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin={"t": 30},
    height=300,
)
st.plotly_chart(fig_time, use_container_width=True)

st.divider()

# ── Tableau derniers vols ─────────────────────────────────────────────────────
st.subheader("Derniers aéronefs enregistrés")

df_display = df[[
    "icao24", "callsign", "origin_country", "current_country",
    "baro_altitude", "velocity", "on_ground", "data_timestamp"
]].copy()
df_display["velocity_kmh"] = (df_display["velocity"].fillna(0) * 3.6).round(1)
df_display["baro_altitude"] = df_display["baro_altitude"].fillna(0).round(0)
df_display["on_ground"] = df_display["on_ground"].map({True: "Sol", False: "En vol"})
df_display = df_display.drop(columns=["velocity"]).rename(columns={
    "icao24": "ICAO24",
    "callsign": "Indicatif",
    "origin_country": "Pays d'origine",
    "current_country": "Pays actuel",
    "baro_altitude": "Altitude (m)",
    "velocity_kmh": "Vitesse (km/h)",
    "on_ground": "Statut",
    "data_timestamp": "Horodatage",
})

search = st.text_input("Rechercher (indicatif, pays...)", "")
if search:
    mask = df_display.apply(lambda col: col.astype(str).str.contains(search, case=False)).any(axis=1)
    df_display = df_display[mask]

st.dataframe(df_display.head(200), use_container_width=True, height=400)
st.caption(f"{len(df_display)} aéronefs affichés · données à {latest_ts.strftime('%Y-%m-%d %H:%M:%S UTC') if latest_ts else '—'}")
