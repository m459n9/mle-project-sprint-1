# первый DAG: flats + buildings -> flats_dataset

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, UniqueConstraint, inspect

from steps.messages import send_telegram_success_message, send_telegram_failure_message


def create_table():
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()
    flats_dataset = Table(
        'flats_dataset',
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
        # на этот констрейнт завязан on conflict в load
        UniqueConstraint('flat_id', name='unique_flat_id_constraint'),
    )

    if not inspect(engine).has_table('flats_dataset'):
        metadata.create_all(engine)


def extract(**kwargs):
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    # left join, иначе потеряются квартиры, у которых дома в buildings нет
    sql = """
    select
        f.id as flat_id,
        f.building_id,
        f.floor,
        f.kitchen_area,
        f.living_area,
        f.rooms,
        f.is_apartment,
        f.studio,
        f.total_area,
        f.price,
        b.build_year,
        b.building_type_int,
        b.latitude,
        b.longitude,
        b.ceiling_height,
        b.flats_count,
        b.floors_total,
        b.has_elevator
    from flats as f
    left join buildings as b on f.building_id = b.id
    """
    data = pd.read_sql(sql, conn)
    conn.close()

    print(f'Извлечено строк: {len(data)}')
    kwargs['ti'].xcom_push(key='extracted_data', value=data)


def transform(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract', key='extracted_data')

    # из-за пропусков pandas отдаёт эти колонки как float, возвращаем целые
    for col in ['flat_id', 'building_id', 'floor', 'rooms', 'build_year',
                'building_type_int', 'flats_count', 'floors_total']:
        data[col] = data[col].astype('Int64')

    for col in ['is_apartment', 'studio', 'has_elevator']:
        data[col] = data[col].astype('boolean')

    # порядок колонок как в create_table
    data = data[['flat_id', 'building_id', 'floor', 'kitchen_area', 'living_area', 'rooms',
                 'is_apartment', 'studio', 'total_area', 'price', 'build_year', 'building_type_int',
                 'latitude', 'longitude', 'ceiling_height', 'flats_count', 'floors_total', 'has_elevator']]
    kwargs['ti'].xcom_push(key='transformed_data', value=data)


def load(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')

    # без astype(object) пропуски уедут в базу как NaN, а не NULL
    rows = data.astype(object).where(pd.notnull(data), None).values.tolist()

    # on conflict - чтобы повторный запуск обновлял строку, а не плодил дубли
    columns = ', '.join(data.columns)
    updates = ', '.join(f'{col} = excluded.{col}' for col in data.columns if col != 'flat_id')
    sql = f'insert into flats_dataset ({columns}) values %s on conflict (flat_id) do update set {updates}'

    conn = hook.get_conn()
    with conn.cursor() as cursor:
        # пачками: построчно 140 тысяч строк грузились бесконечно
        execute_values(cursor, sql, rows, page_size=5000)
    conn.commit()
    conn.close()
    print(f'Загружено строк: {len(rows)}')


with DAG(
    dag_id='prepare_flats_dataset',
    schedule='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
    tags=['ETL', 'flats'],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:
    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> transform_step >> load_step
