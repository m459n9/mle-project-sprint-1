"""Тянем clean_flats_dataset из личной БД в csv."""
import os
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def get_data():
    load_dotenv()
    host = os.environ['DB_DESTINATION_HOST']
    port = os.environ['DB_DESTINATION_PORT']
    db_name = os.environ['DB_DESTINATION_NAME']
    user = os.environ['DB_DESTINATION_USER']
    # в пароле есть спецсимволы, без quote_plus create_engine не разбирал строку
    password = quote_plus(os.environ['DB_DESTINATION_PASSWORD'])

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    # без order by ловил перезапуск всего пайплайна на тех же данных - порядок строк плавал
    data = pd.read_sql('select * from clean_flats_dataset order by id', engine)
    engine.dispose()

    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=False)
    print(f'Выгружено {len(data)} строк и {data.shape[1]} колонок в data/initial_data.csv')


if __name__ == '__main__':
    get_data()
