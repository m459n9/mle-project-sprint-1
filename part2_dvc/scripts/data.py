"""Шаг get_data: выгружаем таблицу clean_flats_dataset из личной БД в data/initial_data.csv."""
import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_data():
    # доступы к базе лежат в файле .env, в коде их не храним
    load_dotenv()
    host = os.environ['DB_DESTINATION_HOST']
    port = os.environ['DB_DESTINATION_PORT']
    db_name = os.environ['DB_DESTINATION_NAME']
    user = os.environ['DB_DESTINATION_USER']
    # в пароле могут быть символы @ : /, из-за них строка подключения разберётся неверно,
    # поэтому пропускаем пароль через quote_plus
    password = quote_plus(os.environ['DB_DESTINATION_PASSWORD'])

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    # order by id: без сортировки база может отдать строки в любом порядке, тогда csv будет
    # каждый раз разным, DVC посчитает новый хеш и пересоберёт весь пайплайн на тех же данных
    data = pd.read_sql('select * from clean_flats_dataset order by id', engine)
    engine.dispose()

    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=False)
    print(f'Выгружено {len(data)} строк и {data.shape[1]} колонок в data/initial_data.csv')


if __name__ == '__main__':
    get_data()
