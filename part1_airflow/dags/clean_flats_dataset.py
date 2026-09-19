"""DAG clean_flats_dataset: чистит таблицу flats_dataset и складывает результат в clean_flats_dataset."""

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, Integer, Float, Boolean, UniqueConstraint, inspect

from steps.messages import send_telegram_success_message, send_telegram_failure_message
from steps.clean_flats import fill_missing_values, remove_duplicates, remove_outliers


def create_table():
    """Создаёт таблицу clean_flats_dataset в личной БД, если её там ещё нет."""
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
    """Читает собранный датасет из таблицы flats_dataset личной БД."""
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()

    # служебный id не берём: в новой таблице он свой.
    # сортировка нужна, чтобы при повторном запуске из одинаковых строк оставалась та же самая
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
    """Заполняет пропуски, удаляет дубликаты и выбросы."""
    data = kwargs['ti'].xcom_pull(task_ids='extract', key='extracted_data')
    print(f'Пришло строк: {len(data)}')

    # порядок важен: после заполнения пропусков строки, отличавшиеся только пропуском,
    # становятся одинаковыми, и их надо поймать шагом с дубликатами
    data = fill_missing_values(data)
    data = remove_duplicates(data)
    data = remove_outliers(data)
    print(f'Осталось строк: {len(data)}')

    # медиана могла сделать целые колонки вещественными, возвращаем им целый тип
    for col in ['flat_id', 'building_id', 'floor', 'rooms', 'build_year',
                'building_type_int', 'flats_count', 'floors_total']:
        data[col] = data[col].round().astype('Int64')

    # пропусков уже нет, поэтому булевы колонки приводим к обычному bool
    for col in ['is_apartment', 'studio', 'has_elevator']:
        data[col] = data[col].astype(bool)

    # порядок колонок тот же, что в SELECT выше и в create_table
    data = data[['flat_id', 'building_id', 'floor', 'kitchen_area', 'living_area', 'rooms',
                 'is_apartment', 'studio', 'total_area', 'price', 'build_year', 'building_type_int',
                 'latitude', 'longitude', 'ceiling_height', 'flats_count', 'floors_total', 'has_elevator']]
    kwargs['ti'].xcom_push(key='transformed_data', value=data)


def load(**kwargs):
    """Записывает очищенный датасет в clean_flats_dataset личной БД."""
    data = kwargs['ti'].xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')

    rows = data.astype(object).where(pd.notnull(data), None).values.tolist()

    # как и в первом DAG, повторный запуск обновляет строки по flat_id
    hook.insert_rows(
        table='clean_flats_dataset',
        rows=rows,
        target_fields=data.columns.tolist(),
        replace=True,
        replace_index=['flat_id'],
    )
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
