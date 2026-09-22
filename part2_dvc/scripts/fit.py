"""Обучение: OneHot + StandardScaler + CatBoost, всё одним пайплайном."""
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

    X = train.drop(columns=params['drop_cols'] + [params['target_col']])
    y = train[params['target_col']]
    # building_type_int - код типа дома, а не величина; строками он одинаково читается и из БД, и из csv
    for col in params['cat_cols']:
        X[col] = X[col].astype(str)

    cat_cols = params['cat_cols']
    num_cols = [col for col in X.columns if col not in cat_cols]

    preprocessor = ColumnTransformer([
        ('cat', OneHotEncoder(drop=params['one_hot_drop'], handle_unknown='ignore', sparse_output=False), cat_cols),
        ('num', StandardScaler(), num_cols),
    ])
    model = CatBoostRegressor(**params['model'], random_seed=params['random_state'])

    # препроцессор внутри пайплайна, иначе на cv будет утечка
    pipeline = Pipeline([('preprocessor', preprocessor), ('model', model)])
    pipeline.fit(X, y)

    os.makedirs('models', exist_ok=True)
    joblib.dump(pipeline, 'models/fitted_model.pkl')
    print(f'Обучились на {len(X)} строках, признаков {X.shape[1]}')
    print('Модель сохранена в models/fitted_model.pkl')


if __name__ == '__main__':
    fit_model()
