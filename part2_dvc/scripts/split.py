"""Шаг split_data: делим data/initial_data.csv на обучающую и тестовую выборки."""
import os

import pandas as pd
import yaml
from sklearn.model_selection import train_test_split


def split_data():
    params = yaml.safe_load(open('params.yaml', 'r'))
    data = pd.read_csv('data/initial_data.csv')

    # index_col - это id квартиры. Если разных квартир столько же, сколько строк, значит одна и та же
    # квартира не попадёт одновременно и в train, и в test, и тест останется честным
    print('Строк:', len(data), '| разных квартир:', data[params['index_col']].nunique())

    # перемешиваем строки и откладываем часть данных, на них потом проверим модель
    train, test = train_test_split(
        data,
        test_size=params['test_size'],
        random_state=params['random_state'],
        shuffle=True,
    )

    os.makedirs('data', exist_ok=True)
    train.to_csv('data/train.csv', index=False)
    test.to_csv('data/test.csv', index=False)
    print(f'Всего {len(data)} строк: train {len(train)}, test {len(test)}')


if __name__ == '__main__':
    split_data()
