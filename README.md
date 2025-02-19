# **Дипломная работа** 
# 1. Контейнеризация и инфраструктура
## Вводная
Для выполнения данной работы требуется поднять два контейнера.
С docker-compose можно ознакомиться [ЗДЕСЬ](https://github.com/dwenlvov/netology_data_engineer/blob/main/docker-compose.yml). 

Image:
* postgres:13
* apache/airflow:latest-python3.12

## Структура
К контейнеру с airflow подключены следующие папки: dags, logs, plugins.
Созданы дополнительные папки в dags:
* conn_db - содержит файл с db.py. Требуется для подключения к БД
* tests - Содержит файлы с тестами
* export - Папка для экспорта таблиц из DDS для последующего использования в Tableau

```bash
netology_data_engineer
├── airflow
│   ├── dags
│   │   ├── conn_db
│   │   │   └── db.py
│   │   ├── etl_spotify.py
│   │   ├── export
│   │   └── tests
│   │       ├── test_spotify_dds.py
│   │       ├── test_spotify_nds.py
│   │       └── test_spotify_raw.py
│   ├── plugins
│   └── logs
├── docker-compose.yml
├── requirements.txt
└── create_structure_db.py
```

Также подключен файл `requirements.txt` для установки следующих библиотек
```txt
numpy>=1.24.0
pandas>=2.0.0
kagglehub==0.3.7
kaggle==1.6.17
pytest==8.3.4
```

И `create_structure_db.py` для создания всей структуры в БД.
## Первые шаги
### Поднимаем контейнер

```bash
docker-compose up -d
```

Заходим в контейнер:
```bash
docker exec -it airflow /bin/bash
```

### Создаем пользователя
Логин пароль можно посмотреть при поднятии контейнера в логах или просто создать нового юзера (самый не замороченный вариант).


Создать юзера (все на свое усмотрение):
```bash
airflow users create \
	--username user \
	--password user \
	--firstname Первый \
	--lastname Второй \
	--role Admin \
	--email user@example.com
```
### Создаем коннект к БД
Также эту команду выполнить внутри контейнера
```bash
airflow connections add postgres_conn --conn-uri="postgresql://airflow:airflow@postgres:5432/airflow"
```

### Создаем структуру в  БД
```python
python /tmp/create_structure_db.py
```
Если все Ок, то в консоли будет сообщение: "Схемы, таблицы и триггерные функции созданы"

И выходим из контейнера
```bash
exit
```

### В Airflow
Все. Можно открывать браузер и переходить в [Airflow](http://localhost:8080/)

# 2. Источник данных
## Описание данных

Сайт источник [Kaggle](https://www.kaggle.com/).  
[Датасет](https://www.kaggle.com/datasets/sgoutami/spotify-streaming-history/data) представляет собой историю воспроизведения треков в Spotify.


## Структура данных

| Название колонки    | Тип данных     | Возможные значения                                                                                                                                                                                                                                         | Not Null | Описание                                                                                                                               |
| ------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `spotify_track_uri` | `varchar(25)`  |                                                                                                                                                                                                                                                            | Yes      | Уникальный идентификатор для каждого трека на Spotify                                                                                  |
| `ts`                | `Timestamp`    |                                                                                                                                                                                                                                                            | Yes      | Временная метка, указывающая, когда закончилось воспроизведение трека                                                                  |
| `platform`          | `varchar(20)`  | - `web player`<br>- `windows`<br>- `android`<br>- `iOS`<br>- `cast to device`<br>- `mac`                                                                                                                                                                   | Yes      | Устройство или платформа, используемые для потоковой передачи трека (например, мобильный, настольный компьютер, веб-плеер)             |
| `ms_played`         | `integer`      |                                                                                                                                                                                                                                                            | Yes      | Продолжительность воспроизводимого трека в миллисекундах                                                                               |
| `track_name`        | `varchar(700)` |                                                                                                                                                                                                                                                            | Yes      | Название трека                                                                                                                         |
| `artist_name`       | `varchar(100)` |                                                                                                                                                                                                                                                            | Yes      | Имя исполнителя, исполнившего трек                                                                                                     |
| `album_name`        | `varchar(400)` |                                                                                                                                                                                                                                                            | Yes      | Название альбома, к которому принадлежит трек                                                                                          |
| `reason_start`      | `varchar(30)`  | - `trackdone`<br>- `playbtn`<br>- `clickrow`<br>- `nextbtn`<br>- `remote`<br>- `popup`<br>- `appload`<br>- `fwdbtn`<br>- `backbtn`<br>- `trackerror`<br>- `unknown`<br>- `autoplay`<br>- `endplay`                                                         |          | Причина, по которой трек начал воспроизводиться (например, выбор пользователя, автозапуск, рекомендация)                               |
| `reason_end`        | `varchar(30)`  | - `trackdone`<br>- `clickrow`<br>- `nextbtn`<br>- `remote`<br>- `popup`<br>- `fwdbtn`<br>- `appload`<br>- `backbtn`<br>- `logout`<br>- `trackerror`<br>- `unknown`<br>- `reload`<br>- `endplay`<br>- `unexpected-exit-while-paused`<br>- `unexpected-exit` |          | Причина, по которой трек закончил воспроизводиться (например, завершение трека, переход к следующему треку, остановка воспроизведения) |
| `shuffle`           | `bool`         |                                                                                                                                                                                                                                                            | Yes      | Был ли трек воспроизведен в режиме перемешивания                                                                                       |
| `skipped`           | `bool`         |                                                                                                                                                                                                                                                            | Yes      | Был ли трек пропущен                                                                                                                   |

# 3. Проектирование хранилища
Все ниже приведенные скрипты мы выполнили на [ЭТОМ](#создаем-структуру-в-бд) этапе

## Очищенный слой
```sql
CREATE SCHEMA diplom_raw;
CREATE TABLE diplom_raw.spotify_streaming_raw (
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
```

`processed_nds` и `processed_dds` - поле проверки, прошла строчка триггерную функцию или нет.

## NDS (Normalized Data Storage)

### Таблицы:
#### `artist`
Информация об исполнителях.

| Колонка       | Тип данных      | PK  | Not Null | Описание                    |
| ------------- | --------------- | --- | -------- | --------------------------- |
| `id_artist`   | `BIGSERIAL`     | Yes | Yes      | Уникальный ID исполнителя   |
| `artist_name` | `VARCHAR(100)`  |     | Yes      | Имя исполнителя             |

#### `album`
Информация об альбомах.

| Колонка       | Тип данных      | PK  | Not Null | Описание                                           |
| ------------- | --------------- | --- | -------- | -------------------------------------------------- |
| `album_id`    | `VARCHAR(25)`   | Yes | Yes      | Уникальный ID альбома                              |
| `album_name`  | `VARCHAR(400)`  |     | Yes      | Название альбома                                   |
| `id_artist`   | `BIGINT`        |     | Yes      | ID исполнителя. Внешний ключ на `artist.artist_id` |
#### `track`
Информация о треках.

| Колонка             | Тип данных       | PK  | Not Null | Описание                                     |
| ------------------- | ---------------- | --- | -------- | -------------------------------------------- |
| `id_track`          | `BIGSERIAL`      | Yes | Yes      | Уникальный ID трека                          |
| `spotify_track_uri` | `VARCHAR(25)`    |     | Yes      | Уникальный ID трека Spotify                  |
| `track_name`        | `VARCHAR(700)`   |     | Yes      | Название трека                               |
| `id_album`          | `BIGINT`         |     | Yes      | ID альбома. Внешний ключ на `album.album_id` |

#### `streaming_history`
История прослушиваний.

| Колонка     | Тип данных     | Not Null | PK  | Описание                                    |
| ----------- | -------------- | -------- | --- | ------------------------------------------- |
| `id_stream` | `BIGSERIAL`    | Yes      | Yes | Уникальный идентификатор записи             |
| `id_track`  | `BIGINT`       | Yes      |     | ID трека. Внешний ключ на `track.track_id`  |
| `ts`        | `TIMESTAMP`    | Yes      |     | Временная метка окончания воспроизведения   |
| `platform`  | `VARCHAR(20)`  | Yes      |     | Платформа, с которой воспроизводился трек   |
| `ms_played` | `INTEGER`      | Yes      |     | Длительность воспроизведения (мс)           |

#### `user_interaction`
Информация о пользовательских взаимодействиях.

| Колонка          | Тип данных      | Not Null | PK  | Описание                                      |
| ---------------- | --------------- | -------- | --- | --------------------------------------------- |
| `id_interaction` | `BIGSERIAL`     | Yes      | Yes | Уникальный идентификатор записи               |
| `id_stream`      | `BIGINT`        | Yes      |     | Внешний ключ на `streaming_history.stream_id` |
| `reason_start`   | `VARCHAR(30)`   | Yes      |     | Причина начала воспроизведения                |
| `reason_end`     | `VARCHAR(30)`   | Yes      |     | Причина окончания воспроизведения             |
| `shuffle`        | `BOOLEAN`       | Yes      |     | Было ли воспроизведение в shuffle-режиме      |
| `skipped`        | `BOOLEAN`       | Yes      |     | Был ли трек пропущен                          |
### ERD схема NDS в **dbdiagram.io**
Ссылка для просмотра схемы в [dbdiagram.io](https://dbdiagram.io/d/NDS-Spotify-history-67a7bfae263d6cf9a07ce0d1)

Скрипт для построения:
```
Table track {
id_track BIGSERIAL [primary key, not null]
spotify_track_uri VARCHAR(25) [not null]
track_name VARCHAR(700) [not null]
id_album BIGINT [not null]
}

Table artist {
id_artist BIGSERIAL [primary key, not null]
artist_name VARCHAR(100) [not null]
}

Table album {
id_album BIGSERIAL [primary key, not null]
album_name VARCHAR(400) [not null]
id_artist BIGINT [not null]
}

Table streaming_history {
id_stream BIGSERIAL [primary key, not null]
id_track BIGINT [not null]
ts TIMESTAMP [not null]
platform VARCHAR(20) [not null]
ms_played INTEGER [not null]
}

Table user_interaction {
id_interaction BIGSERIAL [primary key, not null]
id_stream BIGINT [not null]
reason_start VARCHAR(30)
reason_end VARCHAR(30)
shuffle BOOLEAN [not null]
skipped BOOLEAN [not null]
}

Ref: track.id_album > album.id_album
Ref: album.id_artist > artist.id_artist
Ref: streaming_history.id_track > track.id_track
Ref: user_interaction.id_stream > streaming_history.id_stream
```

### Скрипт создания таблиц и связей в БД
```sql
CREATE SCHEMA diplom_nds;
CREATE TABLE diplom_nds.artist (
    id_artist BIGSERIAL PRIMARY KEY,
    artist_name VARCHAR(100) NOT NULL
);

CREATE TABLE diplom_nds.album (
    id_album BIGSERIAL PRIMARY KEY,
    album_name VARCHAR(400) NOT NULL,
    id_artist BIGINT NOT NULL,
    CONSTRAINT fk_album_artist FOREIGN KEY (id_artist) REFERENCES diplom_nds.artist(id_artist)
);

CREATE TABLE diplom_nds.track (
    id_track BIGSERIAL PRIMARY KEY,
    spotify_track_uri VARCHAR(25) NOT NULL,
    track_name VARCHAR(700) NOT NULL,
    id_album BIGINT NOT NULL,
    CONSTRAINT fk_track_album FOREIGN KEY (id_album) REFERENCES diplom_nds.album(id_album)
);

CREATE TABLE diplom_nds.streaming_history (
    id_stream BIGSERIAL PRIMARY KEY,
    id_track BIGINT NOT NULL,
    ts TIMESTAMP NOT NULL,
    platform VARCHAR(20) NOT NULL,
    ms_played INTEGER NOT NULL,
    CONSTRAINT fk_streaming_track FOREIGN KEY (id_track) REFERENCES diplom_nds.track(id_track)
);

CREATE TABLE diplom_nds.user_interaction (
    id_interaction BIGSERIAL PRIMARY KEY,
    id_stream BIGINT NOT NULL,
    reason_start VARCHAR(30),
    reason_end VARCHAR(30),
    shuffle BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL,
    CONSTRAINT fk_interaction_stream FOREIGN KEY (id_stream) REFERENCES diplom_nds.streaming_history(id_stream)
);
```

### Скрипт триггерной функции для заполнения NDS
Данная часть будет являться частью ETL, а именно сохранения данных в БД.
Идея данного скрипта: Очищенные данные поступают в схему `diplom_raw` **После** записи срабатывает триггерная функция, которая разбивает данные для NDS хранилища и пишет в схему `diplom_nds`

```sql
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
```
## DDS (Data Delivery Storage)

### Измерения

#####  `dim_track`
Справочник треков

| Поле                | Тип данных      | PK  | Not Null | Описание                                               |
| ------------------- | --------------- | --- | -------- | ------------------------------------------------------ |
| `id_track`          | `BIGSERIAL`     | Yes | Yes      | Уникальный ID трека                                    |
| `spotify_track_uri` | `VARCHAR(25)`   | Yes | Yes      | Уникальный ID трека Spotify                            |
| `track_name`        | `VARCHAR(700)`  |     | Yes      | Название трека                                         |
| `id_artist`         | `BIGINT`        |     | Yes      | ID исполнителя. Внешний ключ на `dim_artist.id_artist` |
| `id_album`          | `BIGINT`        |     | Yes      | ID альбома. Внешний ключ на `dim_album.id_album`       |

##### `dim_artist`
Справочник исполнителей

| Поле          | Тип данных     | PK  | Not Null | Описание                  |
| ------------- | -------------- | --- | -------- | ------------------------- |
| `id_artist`   | `BIGSERIAL`    | Yes | Yes      | Уникальный ID исполнителя |
| `artist_name` | `VARCHAR(100)` |     | Yes      | Имя исполнителя           |

##### `dim_album`
Справочник альбомов

| Поле         | Тип данных     | PK  | Not Null | Описание              |
| ------------ | -------------- | --- | -------- | --------------------- |
| `id_album`   | `BIGSERIAL`    | Yes | Yes      | Уникальный ID альбома |
| `album_name` | `VARCHAR(400)` |     | Yes      | Название альбома      |

##### `dim_platform`
Справочник платформ

| Поле          | Тип данных     | PK  | Not Null | Описание                                  |
| ------------- | -------------- | --- | -------- | ----------------------------------------- |
| `id_platform` | `SMALLSERIAL`  | Yes | Yes      | Уникальный ID платформы                   |
| `platform`    | `VARCHAR(20)`  |     | Yes      | Платформа, с которой воспроизводился трек |

##### `dim_reason_start`
Справочник причин старта  

| Поле              | Тип данных     | PK  | Not Null  | Описание                       |
| ----------------- | -------------- | --- | --------- | ------------------------------ |
| `id_reason_start` | `SMALLSERIAL`  | Yes | Yes       | Уникальный ID причины старта   |
| `reason_start`    | `VARCHAR(30)`  |     |           | Причина начала воспроизведения |

##### `dim_reason_end`
Справочник причин завершения

| Поле            | Тип данных     | PK  | Not Null  | Описание                          |
| --------------- | -------------- | --- | --------- | --------------------------------- |
| `id_reason_end` | `SMALLSERIAL`  | Yes | Yes       | Уникальный ID причины завершения  |
| `reason_end`    | `VARCHAR(30)`  |     |           | Причина окончания воспроизведения |

### Таблица фактов  

#### `fact_streaming`
Факты воспроизведения треков

Главная таблица, содержащая данные о проигрывании треков.

| Поле              | Тип данных  | PK  | Not Null  | Описание                                                              |
| ----------------- | ----------- | --- | --------- | --------------------------------------------------------------------- |
| `id_streaming`    | `BIGSERIAL` | Yes | Yes       | Уникальный ID записи                                                  |
| `id_track`        | `BIGINT`    |     | Yes       | ID трека. Внешний ключ на `dim_track.id_track`                        |
| `id_platform`     | `SMALLINT`  |     | Yes       | ID платформы. Внешний ключ на `dim_platform.id_platform`              |
| `id_reason_start` | `SMALLINT`  |     |           | ID причины старта. Внешний ключ на `dim_reason_start.id_reason_start` |
| `id_reason_end`   | `SMALLINT`  |     |           | ID причины завершения. Внешний ключ на `dim_reason_end.id_reason_end` |
| `ts`              | `TIMESTAMP` |     | Yes       | Временная метка окончания воспроизведения                             |
| `ms_played`       | `INTEGER`   |     | Yes       | Длительность воспроизведения (мс)                                     |
| `shuffle`         | `BOOLEAN`   |     | Yes       | Было ли воспроизведение в shuffle-режиме                              |
| `skipped`         | `BOOLEAN`   |     | Yes       | Был ли трек пропущен                                                  |
### ERD схема DDS в **dbdiagram.io**  
Ссылка для просмотра схемы в [dbdiagram.io](https://dbdiagram.io/d/DDS-Spotify-history-67a87785263d6cf9a08312f1)

Скрипт для построения:

```
Table dim_track {
id_track BIGSERIAL [pk, not null]
spotify_track_uri VARCHAR(25) [not null]
track_name VARCHAR(700) [not null]
id_artist BIGINT [not null]
id_album BIGINT [not null]
}

Table dim_artist {
id_artist BIGSERIAL [pk, not null]
artist_name VARCHAR(100) [not null]
}

Table dim_album {
id_album BIGSERIAL [pk, not null]
album_name VARCHAR(400) [not null]
}

Table dim_platform {
id_platform SMALLSERIAL [pk, not null]
platform VARCHAR(20) [not null]
}

Table dim_reason_start {
id_reason_start SMALLSERIAL [pk, not null]
reason_start VARCHAR(30)
}

Table dim_reason_end {
id_reason_end SMALLSERIAL [pk, not null]
reason_end VARCHAR(30)
}

Table fact_streaming {
id_streaming BIGSERIAL [pk, not null]
id_track BIGINT [not null]
id_platform SMALLINT [not null]
id_reason_start SMALLINT
id_reason_end SMALLINT
ts TIMESTAMP [not null]
ms_played INTEGER [not null]
shuffle BOOLEAN [not null]
skipped BOOLEAN [not null]
}

Ref: dim_track.id_artist > dim_artist.id_artist
Ref: dim_track.id_album > dim_album.id_album
Ref: fact_streaming.id_track > dim_track.id_track
Ref: fact_streaming.id_platform > dim_platform.id_platform
Ref: fact_streaming.id_reason_start > dim_reason_start.id_reason_start
Ref: fact_streaming.id_reason_end > dim_reason_end.id_reason_end
```

### Скрипт создания таблиц и связей в БД
```sql
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
);
```

### Скрипт триггерной функции для заполнения DDS
Идея данного скрипта: Очищенные данные поступают в схему `diplom_raw` **После** записи срабатывает триггерная функция, которая разбивает данные для DDS хранилища и пишет в схему `diplom_dds`

```sql
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
```

## Схема записи данных в БД
```mermaid
graph TD
    A[DAG] --> B[diplom_raw.spotify_streaming_raw]
    B --> C[trigger_streaming_nds]
    C --> D[func: streaming_nds]
	D --> E[diplom_nds.artist]
	D --> F[diplom_nds.album]
	D --> G[diplom_nds.track]
	D --> H[diplom_nds.streaming_history]
	D --> I[diplom_nds.user_interaction]
	D --Update column: processed_nds--> B
	B --> C2[trigger_streaming_dds]
	C2 --> D2[func: streaming_dds]
	D2 --> J[diplom_dds.dim_artist]
	D2 --> K[diplom_dds.dim_album]
	D2 --> L[diplom_dds.dim_track]
	D2 --> M[diplom_dds.dim_platform]
	D2 --> N[diplom_dds.dim_reason_start]
	D2 --> O[diplom_dds.dim_reason_end]
	D2 --> P[diplom_dds.fact_streaming]
	D2 --Update column: processed_dds--> B
```

# 4. ETL-процесс 
С самим DAG файлом можно ознакомиться [ЗДЕСЬ](https://github.com/dwenlvov/netology_data_engineer/blob/main/airflow/dags/etl_spotify.py)

## Технические функции
Вспомогательные функции, которые не участвуют в бизнес-логике ETL, но упрощают код и его поддержку.
### `push_to_xcom(value, key='df', to_json=True, **kwargs)`
Запись в XCom.
Принимает 4 аргумента:

| Аргумент  | Type      | Описание                                                                    |
| --------- | --------- | --------------------------------------------------------------------------- |
| value     | DataFrame | Данные, которые нужно записать в XCom                                       |
| key       | string    | Ключ(по умолчанию 'df') под которым будет записаны данные                   |
| to_json   | bool      | Флаг(по умолчанию True) требуется или нет приводить датафрейм в json тип    |
| **kwargs |           | Контекст об отрабатываемой таски, передается самим Airflow.<br>Обязательный |

### `pull_from_xcom(task_id, key, to_pandas=True, **kwargs)`

| Аргумент  | Type      | Описание                                                                                                                                                                                                                                                        |
| --------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| task_id   | DataFrame | В XCom передается через kwargs автоматически. Указывать при записи не требуется. ID таски под, которым был записан в XCom. <br>Пример: данные записали в функции под названием "Таск1", читаем уже в следующей функции - в аргументе указываем task_id="Таск1". |
| key       | string    | Ключ под которым записали в XCom                                                                                                                                                                                                                                |
| to_pandas | bool      | Флаг(по умолчанию True) требуется или нет приводить json в dataframe                                                                                                                                                                                            |
| **kwargs |           | Контекст об отрабатываемой таски, передается самим Airflow.<br>Обязательный                                                                                                                                                                                     |

### `engine_conn()`
Функция соединения с БД. Креды о подключении берет из PostgresHook Airflow по `postgres_conn_id='postgres_conn'`

## Целевые функции
Функции, непосредственно участвующие в обработке данных в рамках ETL-процесса (извлечение, преобразование, загрузка)
### `load_data(**kwargs)`
Датасет был загружен с `Kaggle Datasets` с использованием Python-библиотеки `kagglehub` предварительно создав токен API в настройках сайта `Kaggle`.  И сохранен в XCom Airflow как dataframe pandas.

### `deduplicate(**kwargs)`
Дедубликация потворов в данных достигается путем применения стандартной функции в pandas drop_duplicates.

### `fill_missing_values(**kwargs)`
Цикл по всем колонкам, через apply отправляет значение в `clean_missing_values`. Полученный ответ применяет к той же колонке.

### `clean_missing_values(value)`
Работает в паре с `fill_missing_values(**kwargs)`. Принимает значение колонки. 
Если NaN, '' или ' ', то функция возвращает None.

### `convert_types(**kwargs)`
Переопределяет тип данных для колонок:   
`ts` в тип datetime  
`ms_played` в тип integer.

### `check_checksum_before(**kwargs)`
Сохраняет в XCom контрольные суммы:
* сумма по колонке `ms_played`
* общее колво строк в dataframe

### `save_data(**kwargs)`
Сохранение в БД методом `pandas.to_sql`. С аргументов append - вставить в существующую таблицу.
Данные пишет в `diplom_raw.spotify_streaming_raw`

На стороне БД отрабатывают триггерные функции для записи в NDS и DDS слои.

### `check_checksum_after(**kwargs)`
Загрузка датасета из БД по скрипту:
```sql 
SELECT ms_played FROM diplom_raw.spotify_streaming_raw
```
Сумма поля `ms_played` и сравнение со значениями записанными в XCom.

### `test_raw()`
Функция запускает тесты на основе библиотеки `pytest` для `diplom_raw.spotify_streaming_raw`

| Функция тест             | Столбец       | Описание                                            |
| ------------------------ | ------------- | --------------------------------------------------- |
| test_platform_values     | platform      | Платформа, с которой происходило прослушивание      |
| test_reason_start_values | reason_start  | Причина начала воспроизведения                      |
| test_reason_end_values   | reason_end    | Причина завершения воспроизведения                  |
| test_ms_played_range     | ms_played     | Адекватное возможное количество миллисекунд в треке |
| test_processed_nds       | processed_nds | Флаг обработки строки для nds                       |
| test_processed_dds       | processed_dds | Флаг обработки строки для dds                       |

При падении хоть одно теста весь блок считается проваленным - DAG падает с ошибкой

### `test_nds()`
Функция запускает тест на основе библиотеки `pytest` для схемы `diplom_nds`

| Функция тест                     | Таблица                  | Столбец               | Описание                                            |
| -------------------------------- | ------------------------ | --------------------- | --------------------------------------------------- |
| test_platform_values             | streaming_history        | platform              | Платформа, с которой происходило прослушивание      |
| test_ms_played_range             | streaming_history        | ms_played             | Адекватное возможное количество миллисекунд в треке |
| test_reason_start_values         | user_interaction         | reason_start          | Причина начала воспроизведения                      |
| test_reason_end_values           | user_interaction         | reason_end            | Причина завершения воспроизведения                  |
| test_track_album_artist_relation | artist<br>album<br>track | id_album<br>id_artist | Проверка ссылочной целостности                      |

При падении хоть одно теста весь блок считается проваленным - DAG падает с ошибкой

### `test_dds()`

Функция запускает тест на основе библиотеки `pytest` для схемы `diplom_dds`

| Функция тест             | Таблица                                                | Столбец                                                                              | Описание                                            |
| ------------------------ | ------------------------------------------------------ | ------------------------------------------------------------------------------------ | --------------------------------------------------- |
| test_ms_played_range     | fact_streaming                                         | ms_played                                                                            | Адекватное возможное количество миллисекунд в треке |
| test_platform_values     | dim_platform                                           | platform                                                                             | Платформа, с которой происходило прослушивание      |
| test_reason_start_values | dim_reason_start                                       | reason_start                                                                         | Причина начала воспроизведения                      |
| test_reason_end_values   | dim_reason_end                                         | reason_end                                                                           | Причина завершения воспроизведения                  |
| test_foreign_keys_valid  | dim_track<br>dim_album<br>dim_artist<br>fact_streaming | id_album<br>id_artist<br>id_track<br>id_platform<br>id_reason_start<br>id_reason_end | Проверка ссылочной целостности                      |

При падении хоть одно теста весь блок считается проваленным - DAG падает с ошибкой

### `export_data()`
Экспорт данных из слоя DDS с дополнительной типизацией в Integer колонок с префиксом "id_". Дальше файлы используются для для построения дашборда в Tableau

# 4. Визуализация в Tableau
На основе данных сохраненных в `.csv` был собран [Дашборд](https://public.tableau.com/views/_17399598530790/Spotify?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)
![CleanShot 2025-02-19 at 14 30 02@2x](https://github.com/user-attachments/assets/fc1af9fe-a71c-452b-bf43-b30bc8a8e651)


# 5.Итог
1. **Разработана архитектура хранилища данных**, включающая NDS (нормализованный слой) и DDS (схема “звезда”).
2. **Контейнеризация**
3. **Реализованы ETL-процесс**, обеспечивающие загрузку, обработку и трансформацию данных с проверкой качества.
4. **Использованы инструменты:**  
    * Airflow   
    * PostgreSQL  
    * pytest
5. **Проверки качества данных (DQ) включают:**  
    * Дедубликацию  
    * Обработка пропусков  
    * Контроль типов данных  
    * Сравнение контрольных сумм 
6. **Собран и опубликован дашборд на Tableau**
7. **Возможные улучшения:**
    * Внедрение dbt
