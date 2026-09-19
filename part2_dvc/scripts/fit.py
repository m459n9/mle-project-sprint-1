"""Шаг fit_model: обучаем пайплайн (кодирование признаков + CatBoost) на train.csv и сохраняем его."""
import os

import joblib
import pandas as pd
import yaml
from catboost import CatBoostRegressor
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def fit_model():
    params = yaml.safe_load(open('params.yaml', 'r'))
    train = pd.read_csv('data/train.csv')

    # признаки - все колонки, кроме id-шников и цены; цена это ответ, который учим предсказывать
    X = train.drop(columns=params['drop_cols'] + [params['target_col']])
    y = train[params['target_col']]
    # building_type_int - это код типа дома, а не число: дом типа 4 не в два раза "больше" дома типа 2.
    # Перевожу все категориальные колонки в строки, тогда OneHotEncoder точно закодирует их как категории
    for col in params['cat_cols']:
        X[col] = X[col].astype(str)

    cat_cols = params['cat_cols']
    num_cols = [col for col in X.columns if col not in cat_cols]

    # категории кодируем one-hot, числа приводим к одному масштабу
    preprocessor = ColumnTransformer([
        ('cat', OneHotEncoder(drop=params['one_hot_drop'], handle_unknown='ignore', sparse_output=False), cat_cols),
        ('num', StandardScaler(), num_cols),
    ])
    model = CatBoostRegressor(**params['model'], random_seed=params['random_state'])

    # складываем предобработку и модель в один объект, чтобы потом не повторять эти шаги вручную
    pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
    pipeline.fit(X, y)

    os.makedirs('models', exist_ok=True)
    joblib.dump(pipeline, 'models/fitted_model.pkl')
    print(f'Обучились на {len(X)} строках, признаков {X.shape[1]}')
    print('Модель сохранена в models/fitted_model.pkl')


if __name__ == '__main__':
    fit_model()
