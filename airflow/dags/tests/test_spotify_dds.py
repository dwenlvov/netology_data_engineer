import pytest
import pandas as pd
from conn_db.db import engine_conn
import logging


logger = logging.getLogger(__name__)

def test_ms_played_range(load_data):  # Адекватное возможное количество миллисекунд в треке
    assert (load_data['fact_streaming']['ms_played'] >= 0).all(), '❌ Некорректные значения в колонке ms_played'
    logger.info('✅ Тест test_ms_played_range пройден!')

def test_platform_values(load_data):  # Платформа, с которой происходило прослушивание
    accepted_values = {'web player', 'windows', 'android', 'iOS', 'cast to device', 'mac'}
    assert load_data['dim_platform']['platform'].isin(accepted_values).all(), '❌ Некорректные значения в колонке platform'
    logger.info('✅ Тест test_platform_values пройден!')

def test_reason_start_values(load_data):  # Причина начала воспроизведения
    accepted_values = {'autoplay', 'clickrow', 'trackdone', 'nextbtn', 'backbtn', 'unknown', 'popup', 'appload', 'fwdbtn', 'trackerror', 'remote', 'endplay', 'playbtn', None}
    assert load_data['dim_reason_start']['reason_start'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_start'
    logger.info('✅ Тест test_reason_start_values пройден!')

def test_reason_end_values(load_data):  # Причина окончания воспроизведения
    accepted_values = {'clickrow', 'unknown', 'nextbtn', 'trackdone', 'backbtn', 'reload', 'popup', 'endplay', 'fwdbtn', 'appload', 'unexpected-exit-while-paused', 'unexpected-exit', 'logout', 'remote', 'trackerror', None}
    assert load_data['dim_reason_end']['reason_end'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_end'
    logger.info('✅ Тест test_reason_end_values пройден!')

def test_foreign_keys_valid(load_data):  # Проверка ссылочной целостности
    assert load_data['dim_track']['id_album'].isin(load_data['dim_album']['id_album']).all(), '❌ Нарушена ссылочная целостность в dim_track -> dim_album'
    assert load_data['dim_track']['id_artist'].isin(load_data['dim_artist']['id_artist']).all(), '❌ Нарушена ссылочная целостность в dim_album -> dim_artist'
    assert load_data['fact_streaming']['id_track'].isin(load_data['dim_track']['id_track']).all(), '❌ Нарушена ссылочная целостность в fact_streaming -> dim_track'
    assert load_data['fact_streaming']['id_platform'].isin(load_data['dim_platform']['id_platform']).all(), '❌ Нарушена ссылочная целостность в fact_streaming -> dim_platform'
    assert load_data['fact_streaming'].loc[load_data['fact_streaming']['id_reason_start'].notnull(), 'id_reason_start'].isin(load_data['dim_reason_start']['id_reason_start']).all(), '❌ Нарушена ссылочная целостность в fact_streaming -> id_reason_start'
    assert load_data['fact_streaming'].loc[load_data['fact_streaming']['id_reason_end'].notnull(), 'id_reason_end'].isin(load_data['dim_reason_end']['id_reason_end']).all(), '❌ Нарушена ссылочная целостность в fact_streaming -> id_reason_end'
    logger.info('✅ Тест test_foreign_keys_valid пройден!')


@pytest.fixture
def load_data():
    engine = engine_conn()[0]
    
    tables = {
        'dim_artist': 'SELECT * FROM diplom_dds.dim_artist',
        'dim_album': 'SELECT * FROM diplom_dds.dim_album',
        'dim_track': 'SELECT * FROM diplom_dds.dim_track',
        'dim_platform': 'SELECT * FROM diplom_dds.dim_platform',
        'dim_reason_start': 'SELECT * FROM diplom_dds.dim_reason_start',
        'dim_reason_end': 'SELECT * FROM diplom_dds.dim_reason_end',
        'fact_streaming': 'SELECT * FROM diplom_dds.fact_streaming'
    }

    data = {}
    for table, query in tables.items():
        df = pd.read_sql(query, engine)
        assert not df.empty, f'❌ Таблица {table} пуста!'
        data[table] = df

    return data

def run_tests():
    exit_code = pytest.main(['--tb=line', '--maxfail=1', __file__])
    if exit_code != 0:
        raise Exception('❌ Тесты провалены!')