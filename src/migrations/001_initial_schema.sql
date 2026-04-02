-- Migration 001 : schéma initial pour le suivi d'avions
-- Chaque ligne = snapshot d'un avion à un instant T

CREATE TABLE IF NOT EXISTS aircraft_states (
    id                   SERIAL PRIMARY KEY,
    icao24               VARCHAR(10)   NOT NULL,          -- identifiant ICAO 24-bit (hex)
    callsign             VARCHAR(20),                     -- indicatif de vol (ex: AFR1234)
    origin_country       VARCHAR(100),                    -- pays d'immatriculation (source OpenSky)
    current_country      VARCHAR(100),                    -- pays survolé (reverse geocoding GPS)
    current_country_code CHAR(2),                         -- code ISO-3166-1 alpha-2
    destination_country  VARCHAR(100),                    -- pays de destination (AeroAPI, NULL pour l'instant)
    longitude            DOUBLE PRECISION,                -- longitude actuelle (degrés)
    latitude             DOUBLE PRECISION,                -- latitude actuelle (degrés)
    baro_altitude        DOUBLE PRECISION,                -- altitude barométrique (mètres)
    geo_altitude         DOUBLE PRECISION,                -- altitude géométrique (mètres)
    on_ground            BOOLEAN,
    velocity             DOUBLE PRECISION,                -- vitesse sol (m/s)
    true_track           DOUBLE PRECISION,                -- cap vrai (degrés, 0=nord, sens horaire)
    vertical_rate        DOUBLE PRECISION,                -- taux monte/descente (m/s)
    squawk               VARCHAR(10),                     -- code transpondeur
    position_source      SMALLINT,                        -- 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
    data_timestamp       TIMESTAMPTZ,                     -- horodatage du snapshot OpenSky
    ingested_at          TIMESTAMPTZ DEFAULT NOW()        -- date d'insertion en base
);

-- Index pour les requêtes analytiques fréquentes
CREATE INDEX IF NOT EXISTS idx_as_icao24           ON aircraft_states(icao24);
CREATE INDEX IF NOT EXISTS idx_as_callsign         ON aircraft_states(callsign);
CREATE INDEX IF NOT EXISTS idx_as_origin_country   ON aircraft_states(origin_country);
CREATE INDEX IF NOT EXISTS idx_as_current_country  ON aircraft_states(current_country);
CREATE INDEX IF NOT EXISTS idx_as_data_timestamp   ON aircraft_states(data_timestamp);
