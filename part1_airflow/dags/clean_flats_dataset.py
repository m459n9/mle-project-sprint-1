import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, UniqueConstraint, inspect

from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.clean_flats import fill_missing_values, remove_duplicates, remove_outliers


def create_table():
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()
    clean_flats_dataset = Table(
        'clean_flats_dataset',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('flat_id', Integer),
        Column('building_id', Integer),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', Boolean),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', Float),
        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', Boolean),
        UniqueConstraint('flat_id', name='unique_clean_flat_id_constraint'),
    )

    if not inspect(engine).has_table('clean_flats_dataset'):
        metadata.create_all(engine)


def extract(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    # order by, чтобы при перезапуске из одинаковых строк оставалась та же
    sql = """
    select
        flat_id,
        building_id,
        floor,
        kitchen_area,
        living_area,
        rooms,
        is_apartment,
        studio,
        total_area,
        price,
        build_year,
        building_type_int,
        latitude,
        longitude,
        ceiling_height,
        flats_count,
        floors_total,
        has_elevator
    from flats_dataset
    order by flat_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()

    print(f'Извлечено строк: {len(data)}')
    kwargs['ti'].xcom_push(key='extracted_data', value=data)


def transform(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract', key='extracted_data')
    print(f'Пришло строк: {len(data)}')

    # сначала пробовал наоборот - дубликаты после fillna так не ловились
    data = fill_missing_values(data)
    data = remove_duplicates(data)
    data = remove_outliers(data)
    print(f'Осталось строк: {len(data)}')

    # после fillna медианой целые колонки стали float
    for col in ['flat_id', 'building_id', 'floor', 'rooms', 'build_year',
                'building_type_int', 'flats_count', 'floors_total']:
        data[col] = data[col].round().astype('Int64')

    # пропусков уже нет, хватит обычного bool
    for col in ['is_apartment', 'studio', 'has_elevator']:
        data[col] = data[col].astype(bool)

    data = data[['flat_id', 'building_id', 'floor', 'kitchen_area', 'living_area', 'rooms',
                 'is_apartment', 'studio', 'total_area', 'price', 'build_year', 'building_type_int',
                 'latitude', 'longitude', 'ceiling_height', 'flats_count', 'floors_total', 'has_elevator']]
    kwargs['ti'].xcom_push(key='transformed_data', value=data)


def load(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')

    rows = data.astype(object).where(pd.notnull(data), None).values.tolist()

    columns = ', '.join(data.columns)
    updates = ', '.join(f'{col} = excluded.{col}' for col in data.columns if col != 'flat_id')
    sql = f'insert into clean_flats_dataset ({columns}) values %s on conflict (flat_id) do update set {updates}'

    conn = hook.get_conn()
    with conn.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=5000)
    conn.commit()
    conn.close()
    print(f'Загружено строк: {len(rows)}')


with DAG(
    dag_id='clean_flats_dataset',
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
    tags=['ETL', 'flats', 'cleaning'],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> transform_step >> load_step
