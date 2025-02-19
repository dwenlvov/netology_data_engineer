import pytest
import pandas as pd
from conn_db.db import engine_conn
import logging


logger = logging.getLogger(__name__)

def test_platform_values(load_data): #Платформа, с которой происходило прослушивание
    accepted_values = {'web player', 'windows', 'android', 'iOS', 'cast to device', 'mac'}
    assert load_data['platform'].isin(accepted_values).all(), '❌ Некорректные значения в колонке platform'
    logger.info('✅ Тест test_platform_values пройден!')

def test_reason_start_values(load_data): #Причина начала воспроизведения трека
    accepted_values = {'autoplay', 'clickrow', 'trackdone', 'nextbtn', 'backbtn', 'unknown', 'popup', 'appload', 'fwdbtn', 'trackerror', 'remote', 'endplay', 'playbtn', None}
    assert load_data['reason_start'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_start'
    logger.info('✅ Тест test_reason_start_values пройден!')

def test_reason_end_values(load_data): #Причина завершения трека
    accepted_values = {'clickrow', 'unknown', 'nextbtn', 'trackdone', 'backbtn', 'reload', 'popup', 'endplay', 'fwdbtn', 'appload', 'unexpected-exit-while-paused', 'unexpected-exit', 'logout', 'remote', 'trackerror', None}
    assert load_data['reason_end'].isin(accepted_values).all(), '❌ Некорректные значения в колонке reason_end'
    logger.info('✅ Тест test_reason_end_values пройден!')

def test_ms_played_range(load_data): #Адекватное возможное количество миллисекунд в треке
    assert (load_data['ms_played'] >= 0).all(), '❌ Некорректные значения в колонке ms_played'
    logger.info('✅ Тест test_ms_played_range пройден!')

def test_processed_nds(load_data): #Флаг обработки строки для nds
    assert load_data['processed_nds'].eq(True).all(), '❌ Некорректные значения в колонке processed'
    logger.info('✅ Тест test_processed_flag пройден!')

def test_processed_dds(load_data): #Флаг обработки строки для dds
    assert load_data['processed_dds'].eq(True).all(), '❌ Некорректные значения в колонке processed'
    logger.info('✅ Тест test_processed_flag пройден!')

@pytest.fixture
def load_data():
    engine = engine_conn()[0]
    query = 'SELECT * FROM diplom_raw.spotify_streaming_raw'
    test_df = pd.read_sql(query, engine)
    
    assert not test_df.empty, '❌ Данные не были переданы в тесты!'
    return test_df

def run_tests():
    exit_code = pytest.main(['--tb=line', '--maxfail=1', __file__])
    if exit_code != 0:
        raise Exception('❌ Тесты провалены!')