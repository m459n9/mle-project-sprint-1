"""DAG prepare_flats_dataset: собирает квартиры и дома из общей БД в одну таблицу flats_dataset в личной БД."""

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, UniqueConstraint, inspect

from steps.messages import send_telegram_success_message, send_telegram_failure_message


def create_table():
    """Создаёт таблицу flats_dataset в личной БД, если её там ещё нет."""
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
        # по flat_id строки не должны повторяться, на этом же ограничении работает обновление в load
        UniqueConstraint('flat_id', name='unique_flat_id_constraint'),
    )

    # если таблица уже есть, второй раз её не создаём и данные не теряем
    if not inspect(engine).has_table('flats_dataset'):
        metadata.create_all(engine)


def extract(**kwargs):
    """Читает из общей БД квартиры вместе с характеристиками их домов."""
    hook = PostgresHook('source_db')
    conn = hook.get_conn()

    # id квартиры переименовываем в flat_id, чтобы он не путался с id строки в новой таблице.
    # left join - чтобы не потерять квартиры, у которых в buildings нет дома
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
    """Приводит типы колонок к типам таблицы и выстраивает колонки в нужном порядке."""
    data = kwargs['ti'].xcom_pull(task_ids='extract', key='extracted_data')

    # целые колонки с пропусками приходят из БД как вещественные;
    # Int64 - это целый тип pandas, который умеет хранить пропуски
    for col in ['flat_id', 'building_id', 'floor', 'rooms', 'build_year',
                'building_type_int', 'flats_count', 'floors_total']:
        data[col] = data[col].astype('Int64')

    # boolean - такой же тип с поддержкой пропусков, но для True/False
    for col in ['is_apartment', 'studio', 'has_elevator']:
        data[col] = data[col].astype('boolean')

    # порядок колонок тот же, что в SELECT выше и в create_table: так проще сверять глазами
    data = data[['flat_id', 'building_id', 'floor', 'kitchen_area', 'living_area', 'rooms',
                 'is_apartment', 'studio', 'total_area', 'price', 'build_year', 'building_type_int',
                 'latitude', 'longitude', 'ceiling_height', 'flats_count', 'floors_total', 'has_elevator']]
    kwargs['ti'].xcom_push(key='transformed_data', value=data)


def load(**kwargs):
    """Записывает датасет в flats_dataset личной БД."""
    data = kwargs['ti'].xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')

    # пропуски превращаем в None, иначе в БД вместо NULL уедет строка NaN
    rows = data.astype(object).where(pd.notnull(data), None).values.tolist()

    # replace=True: если строка с таким flat_id уже есть, она обновляется,
    # поэтому повторный запуск DAG не плодит дубликаты
    hook.insert_rows(
        table='flats_dataset',
        rows=rows,
        target_fields=data.columns.tolist(),
        replace=True,
        replace_index=['flat_id'],
    )
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
