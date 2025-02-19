from airflow import DAG
from airflow.operators.python import PythonOperator
from conn_db.db import engine_conn
from datetime import datetime
import pandas as pd
import logging
import kagglehub
from kagglehub import KaggleDatasetAdapter
import tests.test_spotify_raw as ts_raw
import tests.test_spotify_nds as ts_nds
import tests.test_spotify_dds as ts_dds


def push_to_xcom(value, key='df', to_json=True, **kwargs):
    if to_json:
        kwargs['ti'].xcom_push(key=key, value=value.to_json())
    else:
        kwargs['ti'].xcom_push(key=key, value=value)

def pull_from_xcom(task_id, key, to_pandas=True, **kwargs):
    if to_pandas:
        return pd.read_json(kwargs['ti'].xcom_pull(task_ids=task_id, key=key))
    else:
        return kwargs['ti'].xcom_pull(task_ids=task_id, key=key)

def load_data(**kwargs):
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        'sgoutami/spotify-streaming-history',
        'spotify_history.csv',
    )
    push_to_xcom(df, **kwargs)

def deduplicate(**kwargs):
    try:
        df = pull_from_xcom('load_data', 'df', **kwargs)
        df_deduplicated = df.drop_duplicates()

        push_to_xcom(df_deduplicated, **kwargs)

    except Exception as e:
        logging.error(f'❌ Дедубликация пала: {e}')
        raise

def clean_missing_values(value):
    if pd.isna(value) or value in ['', ' ']:
        return None
    return value

def fill_missing_values(**kwargs):
    try:
        df = pull_from_xcom('deduplicate', 'df', **kwargs)
        for col in df.columns:
            df[col] = df[col].apply(clean_missing_values)

        push_to_xcom(df, **kwargs)

    except Exception as e:
        logging.error(f'❌ Функция замены на None пала: {e}')
        raise

def convert_types(**kwargs):
    try:
        df = pull_from_xcom('fill_missing_values', 'df', **kwargs)
        df['ts'] = pd.to_datetime(df['ts']).astype(str)
        df['ms_played'] = df['ms_played'].astype(int)

        push_to_xcom(df, **kwargs)

    except Exception as e:
        logging.error(f'❌ Преобразовании типов пало: {e}')
        raise

def check_checksum_before(**kwargs):
    try:
        df = pull_from_xcom('convert_types', 'df', **kwargs)
        checksum = df['ms_played'].sum()
        row_count = len(df)
        push_to_xcom(checksum, 'checksum_before', to_json=False, **kwargs)
        push_to_xcom(row_count, 'row_count_before', to_json=False, **kwargs)

    except Exception as e:
        logging.error(f'❌ Контрольную сумму не прошли: {e}')
        raise

def save_data(**kwargs):
    try:
        df = pull_from_xcom('convert_types', 'df', **kwargs)
        engine = engine_conn()[0]
        df.to_sql('spotify_streaming_raw', engine, schema='diplom_raw', if_exists='append', index=False)

    except Exception as e:
        logging.error(f'❌ Запись данных в БД raw пало: {e}')
        raise

def check_checksum_after(**kwargs):
    try:
        engine = engine_conn()[0]
        query = 'SELECT ms_played FROM diplom_raw.spotify_streaming_raw'

        df_after = pd.read_sql(query, engine)
        checksum_before = pull_from_xcom('check_checksum_before', 'checksum_before', to_pandas=False, **kwargs)
        row_count_before = pull_from_xcom('check_checksum_before', 'row_count_before', to_pandas=False, **kwargs)

        checksum_after = df_after['ms_played'].sum()
        row_count_after = len(df_after)

        if checksum_before != checksum_after:
            raise ValueError('❌ Контрольные суммы не совпадают!')
        if row_count_before != row_count_after:
            raise ValueError('❌ Количество строк изменилось!')

    except Exception as e:
        logging.error(f'❌ Что-то пошло не так при проверке контрольной суммы: {e}')
        raise

def test_raw():
    ts_raw.run_tests()

def test_nds():
    ts_nds.run_tests()

def test_dds():
    ts_dds.run_tests()


def export_data():
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
    for table, query in tables.items():
        df = pd.read_sql(query, engine)
        for col in df.columns:
            if col.find('id_')!=-1:
                df[col] = df[col].astype('Int64') #Для нормального экспорта целочисленных id
        
        df.to_csv(f'/opt/airflow/dags/export/{table}.csv', sep=',', index=False, encoding='utf-8')



default_args = {
    'owner': 'Denis_Lvov',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 8),
    'retries': 1,
}

dag = DAG(
    'etl_spotify',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    retries=0,
    provide_context=True,
    dag=dag,
)

dd = PythonOperator(
    task_id='deduplicate',
    python_callable=deduplicate,
    retries=0,
    provide_context=True,
    dag=dag,
)

fill_none = PythonOperator(
    task_id='fill_missing_values',
    python_callable=fill_missing_values,
    retries=0,
    provide_context=True,
    dag=dag,
)

types = PythonOperator(
    task_id='convert_types',
    python_callable=convert_types,
    retries=0,
    provide_context=True,
    dag=dag,
)

checksum_bf = PythonOperator(
    task_id='check_checksum_before',
    python_callable=check_checksum_before,
    retries=0,
    provide_context=True,
    dag=dag,
)

save = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    retries=0,
    provide_context=True,
    dag=dag,
)

checksum_aft = PythonOperator(
    task_id='check_checksum_after',
    python_callable=check_checksum_after,
    retries=0,
    provide_context=True,
    dag=dag,
)

pytest_raw = PythonOperator(
    task_id='pytest_raw',
    python_callable=test_raw,
    retries=0,
    provide_context=True,
    dag=dag,
)

pytest_nds = PythonOperator(
    task_id='pytest_nds',
    python_callable=test_nds,
    retries=0,
    provide_context=True,
    dag=dag,
)

pytest_dds = PythonOperator(
    task_id='pytest_dds',
    python_callable=test_dds,
    retries=0,
    provide_context=True,
    dag=dag,
)

export = PythonOperator(
    task_id='export',
    python_callable=export_data,
    retries=0,
    provide_context=True,
    dag=dag,
)

load >> dd >> fill_none >> types >> checksum_bf >> save >> checksum_aft >> [pytest_raw, pytest_nds, pytest_dds] >> export