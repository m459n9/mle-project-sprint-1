# чистка flats_dataset, вызывается в clean_flats_dataset.py именно в этом порядке

import pandas as pd


def fill_missing_values(data):
    data = data.copy()

    for col in data.columns:
        # id-шники медианой заполнять бессмысленно
        if col in ['id', 'flat_id'] or not data[col].isnull().any():
            continue

        if pd.api.types.is_numeric_dtype(data[col]) and not pd.api.types.is_bool_dtype(data[col]):
            data[col] = data[col].fillna(data[col].median())
        else:
            # для bool медианы нет, поэтому мода
            data[col] = data[col].fillna(data[col].mode()[0])

    return data


def remove_duplicates(data):
    # одно и то же объявление может лежать с разными id, поэтому id в сравнение не берём
    feature_cols = [col for col in data.columns if col not in ['id', 'flat_id']]
    is_duplicated = data.duplicated(subset=feature_cols, keep='first')
    return data[~is_duplicated].reset_index(drop=True)


def remove_outliers(data, threshold=1.5):
    """IQR-фильтр, threshold - сколько размахов допускаем."""
    # нулевая цена или площадь - это мусор в данных, IQR его не ловит
    data = data[(data['price'] > 0) & (data['total_area'] > 0)]

    for col in ['total_area', 'living_area', 'kitchen_area', 'ceiling_height', 'price', 'build_year']:
        q1 = data[col].quantile(0.25)
        q3 = data[col].quantile(0.75)
        iqr = q3 - q1
        data = data[data[col].between(q1 - threshold * iqr, q3 + threshold * iqr)]

    return data.reset_index(drop=True)
