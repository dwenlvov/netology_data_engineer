import sys
sys.path.append('/opt/airflow/dags/conn_db')

from db import engine_conn
from sqlalchemy import text

engine = engine_conn()[0]
query = '''
CREATE SCHEMA IF NOT EXISTS diplom_raw;
CREATE TABLE IF NOT EXISTS diplom_raw.spotify_streaming_raw (
    id_raw BIGSERIAL PRIMARY KEY,
    spotify_track_uri VARCHAR(25) NOT NULL,
    ts TIMESTAMP NOT NULL,
    platform VARCHAR(20) NOT NULL,
    ms_played INTEGER NOT NULL,
    track_name VARCHAR(700) NOT NULL,
    artist_name VARCHAR(100) NOT NULL,
    album_name VARCHAR(400) NOT NULL,
    reason_start VARCHAR(30),
    reason_end VARCHAR(30),
    shuffle BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL,
    processed_nds BOOLEAN DEFAULT FALSE,
	processed_dds BOOLEAN DEFAULT FALSE
);

CREATE SCHEMA IF NOT EXISTS diplom_nds;
CREATE TABLE IF NOT EXISTS diplom_nds.artist (
    id_artist BIGSERIAL PRIMARY KEY,
    artist_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS diplom_nds.album (
    id_album BIGSERIAL PRIMARY KEY,
    album_name VARCHAR(400) NOT NULL,
    id_artist BIGINT NOT NULL,
    CONSTRAINT fk_album_artist FOREIGN KEY (id_artist) REFERENCES diplom_nds.artist(id_artist)
);

CREATE TABLE IF NOT EXISTS diplom_nds.track (
    id_track BIGSERIAL PRIMARY KEY,
    spotify_track_uri VARCHAR(25) NOT NULL,
    track_name VARCHAR(700) NOT NULL,
    id_album BIGINT NOT NULL,
    CONSTRAINT fk_track_album FOREIGN KEY (id_album) REFERENCES diplom_nds.album(id_album)
);

CREATE TABLE IF NOT EXISTS diplom_nds.streaming_history (
    id_stream BIGSERIAL PRIMARY KEY,
    id_track BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    platform VARCHAR(20) NOT NULL,
    ms_played INTEGER NOT NULL,
    CONSTRAINT fk_streaming_track FOREIGN KEY (id_track) REFERENCES diplom_nds.track(id_track)
);

CREATE TABLE IF NOT EXISTS diplom_nds.user_interaction (
    id_interaction BIGSERIAL PRIMARY KEY,
    id_stream BIGINT NOT NULL,
    reason_start VARCHAR(30),
    reason_end VARCHAR(30),
    shuffle BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL,
    CONSTRAINT fk_interaction_stream FOREIGN KEY (id_stream) REFERENCES diplom_nds.streaming_history(id_stream)
);

CREATE OR REPLACE FUNCTION diplom_nds.streaming_nds()
RETURNS TRIGGER AS $$
DECLARE
    v_id_artist BIGINT;
    v_id_album BIGINT;
    v_id_track BIGINT;
    v_id_stream BIGINT;
BEGIN
    -- ARTIST
    SELECT id_artist 
	INTO v_id_artist 
	FROM diplom_nds.artist 
	WHERE artist_name = NEW.artist_name;
    IF v_id_artist IS NULL THEN
        INSERT INTO diplom_nds.artist (artist_name) 
		VALUES (NEW.artist_name) 
		RETURNING id_artist INTO v_id_artist;
    END IF;

    -- ALBUM
    SELECT id_album 
	INTO v_id_album 
	FROM diplom_nds.album 
	WHERE album_name = NEW.album_name AND id_artist = v_id_artist;
    IF v_id_album IS NULL THEN
        INSERT INTO diplom_nds.album (album_name, id_artist) 
		VALUES (NEW.album_name, v_id_artist) 
		RETURNING id_album INTO v_id_album;
    END IF;

    -- TRACK
    SELECT id_track 
	INTO v_id_track 
	FROM diplom_nds.track 
	WHERE spotify_track_uri = NEW.spotify_track_uri;
    IF v_id_track IS NULL THEN
        INSERT INTO diplom_nds.track (spotify_track_uri, track_name, id_album) 
        VALUES (NEW.spotify_track_uri, NEW.track_name, v_id_album) 
        RETURNING id_track INTO v_id_track;
    END IF;

    -- STREAMING_HISTORY
    INSERT INTO diplom_nds.streaming_history (id_track, ts, platform, ms_played)
    VALUES (v_id_track, NEW.ts, NEW.platform, NEW.ms_played)
    RETURNING id_stream INTO v_id_stream;

    -- USER_INTERACTION
    INSERT INTO diplom_nds.user_interaction (id_stream, reason_start, reason_end, shuffle, skipped)
    VALUES (v_id_stream, NEW.reason_start, NEW.reason_end, NEW.shuffle, NEW.skipped);

    -- PROCESSED TRUE
    UPDATE diplom_raw.spotify_streaming_raw SET processed_nds = TRUE WHERE id_raw = NEW.id_raw;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_streaming_nds
AFTER INSERT ON diplom_raw.spotify_streaming_raw
FOR EACH ROW
WHEN (NEW.processed_nds = FALSE)
EXECUTE FUNCTION diplom_nds.streaming_nds();

CREATE SCHEMA IF NOT EXISTS diplom_dds;
CREATE TABLE IF NOT EXISTS diplom_dds.dim_artist (
    id_artist BIGSERIAL PRIMARY KEY,
    artist_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS diplom_dds.dim_album (
    id_album BIGSERIAL PRIMARY KEY,
    album_name VARCHAR(400) NOT NULL
);

CREATE TABLE IF NOT EXISTS diplom_dds.dim_platform (
    id_platform SMALLSERIAL PRIMARY KEY,
    platform VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS diplom_dds.dim_reason_start (
    id_reason_start SMALLSERIAL PRIMARY KEY,
    reason_start VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS diplom_dds.dim_reason_end (
    id_reason_end SMALLSERIAL PRIMARY KEY,
    reason_end VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS diplom_dds.dim_track (
    id_track BIGSERIAL PRIMARY KEY,
    spotify_track_uri VARCHAR(25) NOT NULL,
    track_name VARCHAR(700) NOT NULL,
    id_artist BIGINT NOT NULL,
    id_album BIGINT NOT NULL,
    CONSTRAINT fk_dim_track_artist FOREIGN KEY (id_artist) REFERENCES diplom_dds.dim_artist(id_artist),
    CONSTRAINT fk_dim_track_album FOREIGN KEY (id_album) REFERENCES diplom_dds.dim_album(id_album)
);

CREATE TABLE IF NOT EXISTS diplom_dds.fact_streaming (
    id_streaming BIGSERIAL PRIMARY KEY,
    id_track BIGINT NOT NULL,
    id_platform SMALLINT NOT NULL,
    id_reason_start SMALLINT,
    id_reason_end SMALLINT,
    ts TIMESTAMP NOT NULL,
    ms_played INTEGER NOT NULL,
    shuffle BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL,
    CONSTRAINT fk_fact_streaming_track FOREIGN KEY (id_track) REFERENCES diplom_dds.dim_track(id_track),
    CONSTRAINT fk_fact_streaming_platform FOREIGN KEY (id_platform) REFERENCES diplom_dds.dim_platform(id_platform),
    CONSTRAINT fk_fact_streaming_reason_start FOREIGN KEY (id_reason_start) REFERENCES diplom_dds.dim_reason_start(id_reason_start),
    CONSTRAINT fk_fact_streaming_reason_end FOREIGN KEY (id_reason_end) REFERENCES diplom_dds.dim_reason_end(id_reason_end)
);

CREATE INDEX idx_dim_track_artist ON diplom_dds.dim_track(id_artist);
CREATE INDEX idx_dim_track_album ON diplom_dds.dim_track(id_album);
CREATE INDEX idx_fact_streaming_track ON diplom_dds.fact_streaming(id_track);
CREATE INDEX idx_fact_streaming_platform ON diplom_dds.fact_streaming(id_platform);

CREATE OR REPLACE FUNCTION diplom_dds.streaming_dds()
RETURNS TRIGGER AS $$
DECLARE
    v_id_artist BIGINT;
    v_id_album BIGINT;
    v_id_track BIGINT;
    v_id_platform SMALLINT;
    v_id_reason_start SMALLINT;
    v_id_reason_end SMALLINT;
    v_id_streaming BIGINT;
BEGIN
    -- ARTIST
    SELECT id_artist 
    INTO v_id_artist 
    FROM diplom_dds.dim_artist 
    WHERE artist_name = NEW.artist_name;

    IF v_id_artist IS NULL THEN
        INSERT INTO diplom_dds.dim_artist (artist_name)
        VALUES (NEW.artist_name) 
        RETURNING id_artist INTO v_id_artist;
    END IF;

    -- ALBUM
    SELECT id_album 
    INTO v_id_album 
    FROM diplom_dds.dim_album 
	WHERE album_name = NEW.album_name;

    IF v_id_album IS NULL THEN
        INSERT INTO diplom_dds.dim_album (album_name)
        VALUES (NEW.album_name) 
        RETURNING id_album INTO v_id_album;
    END IF;
		
    -- TRACK
    SELECT id_track 
    INTO v_id_track 
    FROM diplom_dds.dim_track 
    WHERE spotify_track_uri = NEW.spotify_track_uri;

    IF v_id_track IS NULL THEN
        INSERT INTO diplom_dds.dim_track (spotify_track_uri, track_name, id_artist, id_album)
        VALUES (NEW.spotify_track_uri, NEW.track_name, v_id_artist, v_id_album) 
        RETURNING id_track INTO v_id_track;
    END IF;

    -- PLATFORM
    SELECT id_platform 
    INTO v_id_platform 
    FROM diplom_dds.dim_platform 
    WHERE platform = NEW.platform;

    IF v_id_platform IS NULL THEN
        INSERT INTO diplom_dds.dim_platform (platform)
        VALUES (NEW.platform)
        RETURNING id_platform INTO v_id_platform;
    END IF;

    -- REASON_START
    IF NEW.reason_start IS NOT NULL THEN
        SELECT id_reason_start 
        INTO v_id_reason_start 
        FROM diplom_dds.dim_reason_start 
        WHERE reason_start = NEW.reason_start;

        IF v_id_reason_start IS NULL THEN
            INSERT INTO diplom_dds.dim_reason_start (reason_start)
            VALUES (NEW.reason_start)
            RETURNING id_reason_start INTO v_id_reason_start;
        END IF;
    END IF;

    -- REASON_END
    IF NEW.reason_end IS NOT NULL THEN
        SELECT id_reason_end 
        INTO v_id_reason_end 
        FROM diplom_dds.dim_reason_end 
        WHERE reason_end = NEW.reason_end;

        IF v_id_reason_end IS NULL THEN
            INSERT INTO diplom_dds.dim_reason_end (reason_end)
            VALUES (NEW.reason_end)
            RETURNING id_reason_end INTO v_id_reason_end;
        END IF;
    END IF;

    -- STREAMING FACT TABLE
    INSERT INTO diplom_dds.fact_streaming (id_track, id_platform, id_reason_start, id_reason_end, ts, ms_played, shuffle, skipped)
    VALUES (v_id_track, v_id_platform, v_id_reason_start, v_id_reason_end, NEW.ts, NEW.ms_played, NEW.shuffle, NEW.skipped)
    RETURNING id_streaming INTO v_id_streaming;

    -- PROCESSED TRUE
    UPDATE diplom_raw.spotify_streaming_raw SET processed_dds = TRUE WHERE id_raw = NEW.id_raw;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_streaming_dds
AFTER INSERT ON diplom_raw.spotify_streaming_raw
FOR EACH ROW
WHEN (NEW.processed_dds = FALSE)
EXECUTE FUNCTION diplom_dds.streaming_dds();
'''

with engine.connect() as connection:
    try:
        with connection.begin():
            connection.execute(text(query))
        print('Схемы, таблицы и триггерные функции созданы')
    except Exception as e:
        print(f'Выполнение скрипта упало с ошибкой: {e}')

