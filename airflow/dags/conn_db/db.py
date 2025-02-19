from airflow.providers.postgres.hooks.postgres import PostgresHook


def engine_conn():
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_connection(hook.postgres_conn_id)
    return engine, conn