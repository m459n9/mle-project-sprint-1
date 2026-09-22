import json
import os

import joblib
import pandas as pd
import yaml
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import KFold, cross_validate


def evaluate_model():
    params = yaml.safe_load(open('params.yaml', 'r'))
    pipeline = joblib.load('models/fitted_model.pkl')

    train = pd.read_csv('data/train.csv')
    test = pd.read_csv('data/test.csv')

    # признаки собираю так же, как в fit.py
    X_train = train.drop(columns=params['drop_cols'] + [params['target_col']])
    y_train = train[params['target_col']]
    X_test = test.drop(columns=params['drop_cols'] + [params['target_col']])
    y_test = test[params['target_col']]
    for col in params['cat_cols']:
        X_train[col] = X_train[col].astype(str)
        X_test[col] = X_test[col].astype(str)

    # cv на train, чтобы не смотреть только на один сплит
    cv = KFold(n_splits=params['n_splits'], shuffle=True, random_state=params['random_state'])
    cv_res = cross_validate(
        pipeline,
        X_train,
        y_train,
        cv=cv,
        scoring=params['metrics'],
        n_jobs=params['n_jobs'],
    )

    y_pred = pipeline.predict(X_test)

    # neg_ метрики приходят с минусом (sklearn максимизирует), возвращаю знак
    # float() - иначе json давится numpy-числами
    result = {
        'cv_mae': round(float(-cv_res['test_neg_mean_absolute_error'].mean()), 2),
        'cv_rmse': round(float(-cv_res['test_neg_root_mean_squared_error'].mean()), 2),
        'cv_mape': round(float(-cv_res['test_neg_mean_absolute_percentage_error'].mean()), 4),
        'cv_r2': round(float(cv_res['test_r2'].mean()), 4),
        'test_mae': round(float(mean_absolute_error(y_test, y_pred)), 2),
        'test_rmse': round(float(mean_squared_error(y_test, y_pred)) ** 0.5, 2),
        'test_mape': round(float(mean_absolute_percentage_error(y_test, y_pred)), 4),
        'test_r2': round(float(r2_score(y_test, y_pred)), 4),
    }

    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

    print('Метрики (cv_ - среднее по фолдам, test_ - отложенная выборка):')
    for name, value in result.items():
        print(f'  {name}: {value}')


if __name__ == '__main__':
    evaluate_model()
