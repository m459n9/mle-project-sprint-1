# Часть 2. DVC-пайплайн обучения модели

Пайплайн берёт очищенную таблицу `clean_flats_dataset` из личной БД (её собрал второй DAG первой части),
делит данные на train и test, обучает `CatBoostRegressor` для предсказания цены квартиры, считает метрики
и отправляет модель в S3. Шаги описаны в `dvc.yaml`, параметры - в `params.yaml`.

```
part2_dvc/
  params.yaml          # параметры пайплайна
  dvc.yaml             # описание стадий
  dvc.lock             # появляется после dvc repro
  .dvc/config          # настройки DVC: адрес удалённого хранилища
  .dvcignore           # что DVC не должен просматривать
  requirements.txt     # зависимости (Python 3.11)
  .env_template        # шаблон переменных окружения
  scripts/
    data.py            # стадия get_data:        clean_flats_dataset -> data/initial_data.csv
    split.py           # стадия split_data:      data/train.csv, data/test.csv
    fit.py             # стадия fit_model:       models/fitted_model.pkl
    evaluate.py        # стадия evaluate_model:  cv_results/cv_res.json
    upload_model.py    # загрузка модели в S3 по читаемому ключу, запускается вручную
  notebooks/
    3_model_experiments.ipynb   # эксперименты: бейзлайн, CatBoost, метрики, важность признаков
  data/                # csv-файлы, в git не попадают, версионируются DVC
  models/              # fitted_model.pkl, тоже под DVC
  cv_results/          # cv_res.json с метриками, появляется после dvc repro и хранится в git
```

## 1. Окружение

Все команды выполняются из папки `part2_dvc`.

```bash
cd part2_dvc
python3.11 -m venv .venv_mle_dvc
source .venv_mle_dvc/bin/activate
pip install -r requirements.txt
cp .env_template .env
```

В `.env` нужны переменные личной БД `DB_DESTINATION_*` (из неё читается `clean_flats_dataset`)
и доступы к бакету: `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
Файл `.env` в git не попадает, в скриптах никаких паролей нет: они сами читают `.env`
через `load_dotenv()`.

## 2. Настройка DVC

DVC уже инициализирован, настройки лежат в репозитории: `.dvc/config` и `.dvcignore`.
Делается это один раз такими командами (флаг `--subdir` нужен потому, что корень git-репозитория
находится уровнем выше, а удалённое хранилище - бакет в Yandex Object Storage):

```bash
dvc init --subdir
dvc remote add -d my_storage s3://$S3_BUCKET_NAME/dvc
dvc remote modify my_storage endpointurl https://storage.yandexcloud.net
```

Ключей доступа в `.dvc/config` нет, и это специально: файл попадает в git. DVC берёт ключи из
переменных окружения, поэтому перед `dvc push` достаточно выгрузить в сессию переменные из `.env`:

```bash
set -a; source .env; set +a
```

Второй вариант - положить ключи в локальный конфиг `.dvc/config.local`, он в git не попадает:

```bash
dvc remote modify --local my_storage access_key_id $AWS_ACCESS_KEY_ID
dvc remote modify --local my_storage secret_access_key $AWS_SECRET_ACCESS_KEY
```

## 3. Запуск пайплайна

```bash
dvc repro      # get_data -> split_data -> fit_model -> evaluate_model
dvc push       # данные и модель уезжают в s3://$S3_BUCKET_NAME/dvc/
```

| Стадия | Команда | Результат |
|---|---|---|
| `get_data` | `python scripts/data.py` | `data/initial_data.csv` |
| `split_data` | `python scripts/split.py` | `data/train.csv`, `data/test.csv` |
| `fit_model` | `python scripts/fit.py` | `models/fitted_model.pkl` |
| `evaluate_model` | `python scripts/evaluate.py` | `cv_results/cv_res.json` |

После `dvc repro` появляется `dvc.lock` с хешами входов и выходов каждой стадии. Если поменять
параметр в `params.yaml`, DVC пересчитает только те стадии, которых это касается.

## 4. Метрики

Основная метрика - **MAE** в рублях, рядом с ней считаются **RMSE**, **MAPE** и **R2**.
Почему выбрана MAE, разобрано в `notebooks/3_model_experiments.ipynb`, раздел
"Какие метрики считаю".

Стадия `evaluate_model` считает метрики двумя способами: на кросс-валидации по обучающей выборке
(`cv_mae`, `cv_rmse`, `cv_mape`, `cv_r2` - средние по фолдам) и на отложенной выборке
(`test_mae`, `test_rmse`, `test_mape`, `test_r2`). Всё это лежит в `cv_results/cv_res.json`,
посмотреть можно командой `dvc metrics show`.

## 5. Модель в S3

`dvc push` кладёт модель в кэш DVC, где имя файла - это хеш содержимого, найти её без DVC неудобно.
Поэтому после `dvc repro` дополнительно запускается скрипт:

```bash
python scripts/upload_model.py
```

Он загружает `models/fitted_model.pkl` по ключу
`s3://$S3_BUCKET_NAME/mle-project-sprint-1/models/fitted_model.pkl` и печатает список объектов
по этому префиксу, чтобы сразу видеть, что файл на месте.

## 6. Ноутбук

`notebooks/3_model_experiments.ipynb` - это черновик пайплайна: там я подключаюсь к личной БД,
смотрю данные, выбираю признаки, сравниваю бейзлайн `DummyRegressor(strategy='median')` с CatBoost
и смотрю важность признаков. Рабочий код из ноутбука разложен по скриптам в `scripts/`.
