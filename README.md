# Проект 1 спринта

## 1. Задача

Исходные данные лежат в двух таблицах Postgres:

* `buildings` - дома: `id`, `build_year`, `building_type_int`, `latitude`, `longitude`, `ceiling_height`,
  `flats_count`, `floors_total`, `has_elevator`;
* `flats` - квартиры: `id`, `building_id`, `floor`, `kitchen_area`, `living_area`, `rooms`,
  `is_apartment`, `studio`, `total_area`, `price`.

Таблицы связаны один-ко-многим: `buildings.id = flats.building_id`. Предсказать нужно `price`,
то есть это задача регрессии.

Работа разбита на три этапа:

1. DAG в Airflow собирает данные из двух таблиц в один датасет.
2. Второй DAG чистит этот датасет: пропуски, дубликаты, выбросы.
3. DVC-пайплайн обучает на очищенных данных модель и сохраняет её в S3.

## 2. Какие базы данных используются

Таблицы `buildings` и `flats` лежат в **общей базе** курса, доступ к ней только на чтение.
Результаты обеих частей пишутся в **личную базу**: сначала `flats_dataset`, потом `clean_flats_dataset`.

В Airflow это два разных соединения:

| Соединение | База | Переменные в `.env` | Зачем |
|---|---|---|---|
| `source_db` | общая, читаем | `DB_SOURCE_*` | читаем `buildings` и `flats` |
| `destination_db` | личная, пишем | `DB_DESTINATION_*` | пишем `flats_dataset` и `clean_flats_dataset` |

Обе строки подключения собираются в `part1_airflow/docker-compose.yaml` из переменных файла `.env`
(`AIRFLOW_CONN_SOURCE_DB` и `AIRFLOW_CONN_DESTINATION_DB`), поэтому в коде DAG хватает
`PostgresHook('source_db')` и `PostgresHook('destination_db')`. Сам `.env` в репозиторий не попадает,
в коде паролей нет.

Вторая часть проекта читает `clean_flats_dataset` из личной базы по тем же переменным
`DB_DESTINATION_*` из своего файла `part2_dvc/.env`.

## 3. Бакет S3

Здесь укажите имя вашего бакета: **s3-student-mle-20260908-7056ef1b44**

В бакете лежат:

* `dvc/` - удалённое хранилище DVC (туда уходят данные и модель после `dvc push`);
* `mle-project-sprint-1/models/fitted_model.pkl` - копия обученной модели с понятным именем,
  её кладёт скрипт `part2_dvc/scripts/upload_model.py`.

## 4. Структура репозитория

```
mle-project-sprint-1/
├── README.md
├── requirements.txt                   # общий список зависимостей из шаблона курса
├── part1_airflow/                     # этапы 1 и 2: Airflow
│   ├── README.md                      # как поднять Airflow и запустить DAG
│   ├── .env_template                  # шаблон переменных окружения (без секретов)
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   ├── dags/
│   │   ├── flats_dataset.py           # DAG prepare_flats_dataset (этап 1)
│   │   └── clean_flats_dataset.py     # DAG clean_flats_dataset (этап 2)
│   ├── plugins/steps/
│   │   ├── messages.py                # уведомления в Telegram
│   │   └── clean_flats.py             # функции очистки данных
│   ├── notebooks/
│   │   ├── 1_explore_source_tables.ipynb
│   │   └── 2_data_cleaning.ipynb
│   └── logs/                          # логи Airflow, в git не попадают
└── part2_dvc/                         # этап 3: DVC
    ├── README.md                      # как запустить пайплайн
    ├── .env_template
    ├── requirements.txt
    ├── params.yaml                    # параметры пайплайна
    ├── dvc.yaml                       # описание стадий
    ├── dvc.lock                       # появляется после dvc repro
    ├── .dvc/config                    # настройки DVC: адрес удалённого хранилища
    ├── .dvcignore
    ├── scripts/
    │   ├── data.py                    # стадия get_data
    │   ├── split.py                   # стадия split_data
    │   ├── fit.py                     # стадия fit_model
    │   ├── evaluate.py                # стадия evaluate_model
    │   └── upload_model.py            # загрузка модели в S3 (запускается вручную)
    ├── notebooks/
    │   └── 3_model_experiments.ipynb
    ├── data/                          # csv-файлы, версионируются DVC
    ├── models/                        # fitted_model.pkl, версионируется DVC
    ├── cv_results/                    # cv_res.json с метриками, появляется после dvc repro
    └── mlruns/                        # папка из шаблона курса, MLflow здесь не используется
```

Папки `data/`, `models/` и `logs/` в репозитории пустые: данные и модель хранятся в S3, логи в git не нужны.

## 5. Этап 1. Сбор данных

| Что | Где |
|---|---|
| Код DAG | `part1_airflow/dags/flats_dataset.py` |
| DAG | `prepare_flats_dataset` |
| Функции задач | `create_table`, `extract`, `transform`, `load` |
| Уведомления в Telegram | `part1_airflow/plugins/steps/messages.py` |
| Ноутбук с разведкой данных | `part1_airflow/notebooks/1_explore_source_tables.ipynb` |
| Результат | таблица `flats_dataset` в личной БД |

Что делают задачи:

* `create_table` - создаёт в личной БД таблицу `flats_dataset`, если её ещё нет. На `flat_id` стоит
  ограничение уникальности, поэтому повторный запуск не наплодит дублей;
* `extract` - читает из общей БД квартиры вместе с характеристиками домов одним запросом
  с `left join` по `flats.building_id = buildings.id`;
* `transform` - приводит колонки к нужным типам и ставит их в том же порядке, что и в таблице-приёмнике;
* `load` - пишет данные в `flats_dataset` через `insert_rows` с обновлением строк по `flat_id`.

Функции `send_telegram_success_message` и `send_telegram_failure_message` из
`part1_airflow/plugins/steps/messages.py` подключены к DAG как `on_success_callback`
и `on_failure_callback` и шлют сообщение в Telegram после каждого запуска.

## 6. Этап 2. Очистка данных

| Что | Где |
|---|---|
| Код DAG | `part1_airflow/dags/clean_flats_dataset.py` |
| DAG | `clean_flats_dataset` |
| Функции задач | `create_table`, `extract`, `transform`, `load` |
| Функции очистки | `part1_airflow/plugins/steps/clean_flats.py` |
| Ноутбук с анализом | `part1_airflow/notebooks/2_data_cleaning.ipynb` |
| Результат | таблица `clean_flats_dataset` в личной БД |

В ноутбуке `2_data_cleaning.ipynb` таблица `flats_dataset` проверяется на пропуски, дубликаты
и выбросы (describe, boxplot, границы по межквартильному размаху) и на доменные аномалии вроде
нулевой цены или площади. По итогам написаны три функции, которые лежат в
`part1_airflow/plugins/steps/clean_flats.py`:

* `fill_missing_values(data)` - заполняет пропуски: числовые колонки медианой, остальные модой;
* `remove_duplicates(data)` - убирает строки, у которых совпадают все признаки, кроме идентификаторов;
* `remove_outliers(data, threshold=1.5)` - выбрасывает строки с неположительной ценой или площадью,
  а затем выбросы по межквартильному размаху.

Задача `transform` применяет их именно в таком порядке: сначала `fill_missing_values`, потом
`remove_duplicates`, потом `remove_outliers`. Заполнение пропусков делает одинаковыми строки,
которые до этого отличались только пропуском, поэтому дубликаты ищем уже после него.

## 7. Этап 3. DVC-пайплайн

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

Стадии идут по цепочке:

1. `get_data` - выгружает `clean_flats_dataset` из личной БД в `data/initial_data.csv`;
2. `split_data` - делит данные на `data/train.csv` и `data/test.csv`;
3. `fit_model` - обучает пайплайн `ColumnTransformer` (OneHotEncoder для категорий, StandardScaler
   для чисел) + `CatBoostRegressor` и сохраняет его в `models/fitted_model.pkl`;
4. `evaluate_model` - считает метрики на кросс-валидации по train и на отложенной выборке test,
   результат пишет в `cv_results/cv_res.json`.

### Метрики

Основная метрика - **MAE** в рублях, дополнительно считаются **RMSE**, **MAPE** и **R2**.
Почему основной выбрана именно MAE, разобрано в ноутбуке
`part2_dvc/notebooks/3_model_experiments.ipynb`: там есть гистограмма цены, из-за которой
этот выбор и сделан.

В `cv_results/cv_res.json` лежат средние по фолдам кросс-валидации (`cv_mae`, `cv_rmse`, `cv_mape`,
`cv_r2`) и метрики на тесте (`test_mae`, `test_rmse`, `test_mape`, `test_r2`). Посмотреть их можно
командой `dvc metrics show`.

### Где лежит модель

1. После `dvc push` модель и данные попадают в кэш DVC в бакете: `s3://<bucket>/dvc/files/md5/...`.
   Какой объект соответствует модели, записано в `part2_dvc/dvc.lock`.
2. Скрипт `part2_dvc/scripts/upload_model.py` кладёт тот же файл по понятному ключу
   `s3://<bucket>/mle-project-sprint-1/models/fitted_model.pkl`, чтобы модель было легко найти в бакете.

## 8. Как запустить

В папках `part1_airflow/` и `part2_dvc/` лежат файлы `.env_template`. В каждой из них нужно скопировать
шаблон в `.env` и подставить свои значения (файлы `.env` в git не попадают).

Этапы 1 и 2:

```bash
cd part1_airflow
cp .env_template .env          # заполнить значениями
docker compose up --build -d
```

Веб-интерфейс Airflow: http://localhost:8080, логин и пароль `admin` / `admin`. Сначала запускается
DAG `prepare_flats_dataset`, после него - `clean_flats_dataset`. Подробности в `part1_airflow/README.md`.

Этап 3:

```bash
cd part2_dvc
python3.11 -m venv .venv_mle_dvc
source .venv_mle_dvc/bin/activate
pip install -r requirements.txt
cp .env_template .env          # заполнить значениями
dvc repro                      # get_data -> split_data -> fit_model -> evaluate_model
dvc push                       # отправить данные и модель в S3
python scripts/upload_model.py # копия модели по читаемому ключу
```

Настройка DVC-remote описана в `part2_dvc/README.md`.

## 9. Итоговые метрики

Значения берутся из `part2_dvc/cv_results/cv_res.json` после запуска `dvc repro`.
Пока пайплайн на реальных данных не запускался, поэтому в таблице стоят прочерки.

| Метрика | Кросс-валидация на train (5 фолдов) | Отложенная выборка test |
|---|---|---|
| MAE (основная), руб. | - | - |
| RMSE, руб. | - | - |
| MAPE | - | - |
| R2 | - | - |

Модель - `CatBoostRegressor` с параметрами из секции `model` файла `part2_dvc/params.yaml`.
