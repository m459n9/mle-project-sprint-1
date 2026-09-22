# Проект 1 спринта

## Задача

Исходные данные лежат в двух таблицах Postgres:

* `buildings` - дома: `id`, `build_year`, `building_type_int`, `latitude`, `longitude`, `ceiling_height`,
  `flats_count`, `floors_total`, `has_elevator`;
* `flats` - квартиры: `id`, `building_id`, `floor`, `kitchen_area`, `living_area`, `rooms`,
  `is_apartment`, `studio`, `total_area`, `price`.

Таблицы связаны один-ко-многим: `buildings.id = flats.building_id`. Предсказывать нужно `price`,
то есть это регрессия.

Работа разбита на три этапа: DAG в Airflow собирает данные из двух таблиц в один датасет, второй DAG
его чистит, а DVC-пайплайн обучает на очищенных данных модель и отправляет её в S3.

## База данных

Исходные таблицы `buildings` и `flats` лежат в личной базе. Туда же пишутся и результаты:
сначала `flats_dataset`, потом `clean_flats_dataset`. То есть всё работает с одной базой.

В Airflow она подключена через соединение `destination_db`. Строка подключения собирается
в `part1_airflow/docker-compose.yaml` из переменных `DB_DESTINATION_*` файла `.env`
(переменная `AIRFLOW_CONN_DESTINATION_DB`), поэтому в коде DAG достаточно написать
`PostgresHook('destination_db')`. Сам `.env` в git не уходит, доступы только оттуда.

Вторая часть проекта читает `clean_flats_dataset` по тем же переменным `DB_DESTINATION_*`
из своего файла `part2_dvc/.env`.

## Бакет S3

Бакет: `s3-student-mle-20260908-7056ef1b44`. В нём лежат:

* `dvc/` - удалённое хранилище DVC (туда уходят данные и модель после `dvc push`);
* `mle-project-sprint-1/models/fitted_model.pkl` - копия обученной модели с понятным именем,
  её кладёт скрипт `part2_dvc/scripts/upload_model.py`.

## Структура репозитория

```
mle-project-sprint-1/
├── README.md
├── requirements.txt                   # общий список зависимостей из шаблона курса
├── part1_airflow/                     # Airflow: этапы 1 и 2
│   ├── README.md
│   ├── .env_template
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── dags/
│   │   ├── flats_dataset.py
│   │   └── clean_flats_dataset.py
│   ├── plugins/steps/
│   │   ├── messages.py                # уведомления в Telegram
│   │   └── clean_flats.py             # функции очистки данных
│   ├── notebooks/
│   │   ├── 1_explore_source_tables.ipynb
│   │   └── 2_data_cleaning.ipynb
│   └── logs/                          # логи Airflow, в git не попадают
└── part2_dvc/                         # этап 3: DVC
    ├── README.md
    ├── .env_template
    ├── requirements.txt
    ├── params.yaml
    ├── dvc.yaml
    ├── dvc.lock                       # появляется после dvc repro
    ├── .dvc/config                    # адрес удалённого хранилища
    ├── .dvcignore
    ├── scripts/
    │   ├── data.py
    │   ├── split.py
    │   ├── fit.py
    │   ├── evaluate.py
    │   └── upload_model.py
    ├── notebooks/
    │   └── 3_model_experiments.ipynb
    ├── data/
    ├── models/
    ├── cv_results/                    # cv_res.json с метриками
    └── mlruns/                        # папка из шаблона, MLflow не использую
```

Папки `data/`, `models/` и `logs/` в репозитории пустые: данные и модель хранятся в S3, логи в git не нужны.

## Этап 1. Сбор данных

DAG `prepare_flats_dataset` лежит в `part1_airflow/dags/flats_dataset.py`, задачи
`create_table`, `extract`, `transform`, `load`, на выходе таблица `flats_dataset` в личной БД.
Разведка данных - в `part1_airflow/notebooks/1_explore_source_tables.ipynb`.

`create_table` заводит `flats_dataset`, если её нет; на `flat_id` стоит уникальность, поэтому
повторный запуск не плодит дубли. `extract` забирает квартиры вместе с характеристиками домов одним
запросом с `left join` по `flats.building_id = buildings.id`. В `transform` главное - выставить
колонки в том же порядке, что в таблице-приёмнике, иначе вставка их перепутает.

Функции `send_telegram_success_message` и `send_telegram_failure_message` из
`part1_airflow/plugins/steps/messages.py` подключены к DAG как `on_success_callback`
и `on_failure_callback` и шлют сообщение после каждого запуска.

## Этап 2. Очистка данных

DAG `clean_flats_dataset` лежит в `part1_airflow/dags/clean_flats_dataset.py`, задачи те же четыре,
результат - таблица `clean_flats_dataset`. Разбор данных и черновик функций -
в `part1_airflow/notebooks/2_data_cleaning.ipynb`: там таблица `flats_dataset` проверяется
на пропуски, дубликаты и выбросы (describe, boxplot, границы по межквартильному размаху)
и на аномалии вроде нулевой цены или площади.

По итогам написаны три функции, они лежат в `part1_airflow/plugins/steps/clean_flats.py`:

* `fill_missing_values(data)` - числовые колонки заполняет медианой, остальные модой;
* `remove_duplicates(data)` - убирает строки, у которых совпали все признаки, кроме идентификаторов;
* `remove_outliers(data, threshold=1.5)` - выбрасывает строки с неположительной ценой или площадью,
  а затем выбросы по межквартильному размаху.

Задача `transform` вызывает их именно в таком порядке. Заполнение пропусков делает одинаковыми строки,
которые до этого отличались только пропуском, поэтому дубликаты ищутся уже после него.

## Этап 3. DVC-пайплайн

| Что | Где |
|---|---|
| Стадия `get_data` | `part2_dvc/scripts/data.py` |
| Стадия `split_data` | `part2_dvc/scripts/split.py` |
| Стадия `fit_model` | `part2_dvc/scripts/fit.py` |
| Стадия `evaluate_model` | `part2_dvc/scripts/evaluate.py` |
| Описание стадий | `part2_dvc/dvc.yaml` |
| Параметры | `part2_dvc/params.yaml` |
| Lock-файл | `part2_dvc/dvc.lock` (появляется после `dvc repro`) |
| Метрики | `part2_dvc/cv_results/cv_res.json` (появляется после `dvc repro`) |
| Загрузка модели в S3 | `part2_dvc/scripts/upload_model.py` |
| Ноутбук с экспериментами | `part2_dvc/notebooks/3_model_experiments.ipynb` |

Внутри `fit_model` - `ColumnTransformer` (`OneHotEncoder` на категории, `StandardScaler` на числа)
и `CatBoostRegressor`, а `evaluate_model` считает метрики на кросс-валидации по train
и на отложенной выборке test.

### Метрики

Основная метрика - MAE в рублях, ещё считаю RMSE, MAPE и R2. Почему именно MAE - в ноутбуке
`part2_dvc/notebooks/3_model_experiments.ipynb`, там гистограмма цены с длинным правым хвостом.

### Где лежит модель

После `dvc push` модель и данные попадают в кэш DVC в бакете (`s3://<bucket>/dvc/files/md5/...`,
какой объект соответствует модели - записано в `part2_dvc/dvc.lock`). Скрипт
`part2_dvc/scripts/upload_model.py` кладёт её же по ключу
`s3://<bucket>/mle-project-sprint-1/models/fitted_model.pkl`.

## Как запустить

Обе части заводятся одинаково: `cp .env_template .env` и заполнить своими значениями.

Дальше в `part1_airflow` - `docker compose up --build -d`, Airflow на http://localhost:8080
(`admin` / `admin`), сначала DAG `prepare_flats_dataset`, после него `clean_flats_dataset`.
В `part2_dvc` - venv на Python 3.11, `pip install -r requirements.txt`, потом `dvc repro`,
`dvc push` и `python scripts/upload_model.py`.

Полные команды и настройка DVC-remote - в `part1_airflow/README.md` и `part2_dvc/README.md`.

## Итоговые метрики

Значения из `part2_dvc/cv_results/cv_res.json`, получены запуском `dvc repro`.

| Метрика | Кросс-валидация на train (5 фолдов) | Отложенная выборка test |
|---|---|---|
| MAE (основная), руб. | 1 680 766 | 1 666 729 |
| RMSE, руб. | 2 080 089 | 2 063 365 |
| MAPE | 0.163 | 0.162 |
| R2 | 0.643 | 0.648 |

Модель - `CatBoostRegressor` с параметрами из секции `model` файла `part2_dvc/params.yaml`.

Цифры на кросс-валидации и на тесте почти совпадают, значит модель не переобучилась. В среднем она
ошибается примерно на 1,7 млн рублей, это около 16% от цены квартиры. Для первого захода сойдёт.
Что пробовать дальше - скорее признаки: из координат можно вытащить расстояние до центра,
сейчас модель про район ничего не знает.
