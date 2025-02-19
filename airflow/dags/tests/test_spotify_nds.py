import pytest
import pandas as pd
from conn_db.db import engine_conn
import logging


logger = logging.getLogger(__name__)

def test_platform_values(load_data):  # Платформа, с которой происходило прослушивание
    accepted_values = {'web player', 'windows', 'android', 'iOS', 'cast to device', 'mac'}
    assert load_data['streaming_history']['platform'].isin(accepted_values).all(), '❌ Некорректные значения в колонке platform'
    logger.info('✅ Тест test_platform_values пройден!')

def test_reason_start_values(load_data):  # Причина начала воспроизведения трека
    accepted_values = {'autoplay', 'clickrow', 'trackdone', 'nextbtn', 'backbtn', 'unknown', 'popup', 'appload', 'fwdbtn', 'trackerror', 'remote', 'endplay', 'playbtn', None}
    assert load_data['user_interaction']['reason_start'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_start'
    logger.info('✅ Тест test_reason_start_values пройден!')

def test_reason_end_values(load_data):  # Причина окончания трека
    accepted_values = {'clickrow', 'unknown', 'nextbtn', 'trackdone', 'backbtn', 'reload', 'popup', 'endplay', 'fwdbtn', 'appload', 'unexpected-exit-while-paused', 'unexpected-exit', 'logout', 'remote', 'trackerror', None}
    assert load_data['user_interaction']['reason_end'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_end'
    logger.info('✅ Тест test_reason_end_values пройден!')

def test_ms_played_range(load_data):  # Адекватное возможное количество миллисекунд в треке
    assert (load_data['streaming_history']['ms_played'] >= 0).all(), '❌ Некорректные значения в колонке ms_played'
    logger.info('✅ Тест test_ms_played_range пройден!')

def test_track_album_artist_relation(load_data):  # Проверка ссылочной целостности
    track = load_data['track']
    album = load_data['album']
    artist = load_data['artist']

    assert track['id_album'].isin(album['id_album']).all(), '❌ В таблице track есть записи с несуществующими альбомами'
    assert album['id_artist'].isin(artist['id_artist']).all(), '❌ В таблице album есть записи с несуществующими артистами'
    logger.info('✅ Тест test_track_album_artist_relation пройден!')




@pytest.fixture
def load_data():
    engine = engine_conn()[0]
    
    tables = {
        'streaming_history': 'SELECT * FROM diplom_nds.streaming_history',
        'user_interaction': 'SELECT * FROM diplom_nds.user_interaction',
        'artist': 'SELECT * FROM diplom_nds.artist',
        'album': 'SELECT * FROM diplom_nds.album',
        'track': 'SELECT * FROM diplom_nds.track'
    }

    data = {table: pd.read_sql(query, engine) for table, query in tables.items()}

    for table_name, df in data.items():
        assert not df.empty, f'❌ Данные из {table_name} не были переданы в тесты!'
    
    return data

def run_tests():
    exit_code = pytest.main(['--tb=line', '--maxfail=1', __file__])
    if exit_code != 0:
        raise Exception('❌ Тесты провалены!')