"""Функции очистки датасета квартир.

Применять по порядку: fill_missing_values -> remove_duplicates -> remove_outliers.
Эти же функции разобраны в ноутбуке notebooks/2_data_cleaning.ipynb.
"""

import pandas as pd


def fill_missing_values(data):
    """Заполняет пропуски: числовые колонки медианой, булевы и остальные модой."""
    data = data.copy()

    for col in data.columns:
        # id и flat_id - это идентификаторы, их заполнять нельзя
        if col in ['id', 'flat_id'] or not data[col].isnull().any():
            continue

        if pd.api.types.is_numeric_dtype(data[col]) and not pd.api.types.is_bool_dtype(data[col]):
            data[col] = data[col].fillna(data[col].median())
        else:
            # для булевых и текстовых колонок медианы нет, берём самое частое значение
            data[col] = data[col].fillna(data[col].mode()[0])

    return data


def remove_duplicates(data):
    """Удаляет строки, которые совпадают по всем признакам (id и flat_id не считаем)."""
    # одна и та же квартира может быть выложена дважды с разными id, поэтому сравниваем только признаки
    feature_cols = [col for col in data.columns if col not in ['id', 'flat_id']]
    is_duplicated = data.duplicated(subset=feature_cols, keep='first')
    return data[~is_duplicated].reset_index(drop=True)


def remove_outliers(data, threshold=1.5):
    """Убирает строки с нулевой ценой или площадью и выбросы по методу межквартильного размаха."""
    # цена и общая площадь меньше или равные нулю - это ошибка в данных, а не выброс
    data = data[(data['price'] > 0) & (data['total_area'] > 0)]

    # выброс - значение дальше, чем на threshold межквартильных размахов от границ ящика
    for col in ['total_area', 'living_area', 'kitchen_area', 'ceiling_height', 'price', 'build_year']:
        q1 = data[col].quantile(0.25)
        q3 = data[col].quantile(0.75)
        iqr = q3 - q1
        data = data[data[col].between(q1 - threshold * iqr, q3 + threshold * iqr)]

    return data.reset_index(drop=True)
