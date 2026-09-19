# Часть 1. Airflow: сбор и очистка датасета квартир

Здесь лежат этапы 1 и 2 проекта: два DAG, плагины с функциями и ноутбуки, в которых эти функции
сначала разбирались руками.

```
part1_airflow/
  dags/
    flats_dataset.py         # DAG prepare_flats_dataset: flats + buildings -> flats_dataset
    clean_flats_dataset.py   # DAG clean_flats_dataset: flats_dataset -> clean_flats_dataset
  plugins/steps/
    messages.py              # уведомления в Telegram об успехе и об ошибке
    clean_flats.py           # функции очистки: пропуски, дубликаты, выбросы
  notebooks/
    1_explore_source_tables.ipynb   # разведка таблиц buildings и flats, проверка JOIN-запроса
    2_data_cleaning.ipynb           # анализ пропусков, дубликатов, выбросов и функции очистки
  logs/                      # логи Airflow, монтируются в контейнеры
  Dockerfile                 # образ apache/airflow:2.7.3-python3.10 + requirements.txt
  docker-compose.yaml        # postgres + airflow-init + webserver + scheduler
  requirements.txt           # что доставляется в образ
  .env_template              # шаблон переменных окружения
```

## 1. Переменные окружения

Скопируйте шаблон и заполните его своими доступами (`.env` в git не попадает):

```bash
cd part1_airflow
cp .env_template .env
```

Что нужно заполнить:

* `DB_SOURCE_*` - общая БД курса, в ней лежат исходные таблицы `buildings` и `flats`, читаем только оттуда;
* `DB_DESTINATION_*` - личная БД, в неё пишем `flats_dataset` и `clean_flats_dataset`;
* `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID` - бот и чат для уведомлений;
* `AIRFLOW_UID=50000` - пользователь, от которого работают контейнеры (иначе будут проблемы с правами
  на папки `dags`, `logs`, `plugins`).

Из этих переменных `docker-compose.yaml` собирает два соединения Airflow:

| Переменная в compose | Соединение в коде | База |
|---|---|---|
| `AIRFLOW_CONN_SOURCE_DB` | `source_db` | общая, только чтение |
| `AIRFLOW_CONN_DESTINATION_DB` | `destination_db` | личная, запись |

В коде DAG поэтому достаточно написать `PostgresHook('source_db')` или `PostgresHook('destination_db')`,
никаких паролей в файлах проекта нет. Если в пароле есть спецсимволы (`@`, `/`, `#`, `:`), запишите их
в `.env` в URL-кодированном виде, например `@` как `%40`.

## 2. Запуск Airflow

```bash
cd part1_airflow
docker compose build
docker compose up airflow-init      # миграции метаданных и пользователь admin / admin
docker compose up -d                # webserver + scheduler
```

Веб-интерфейс: http://localhost:8080, логин `admin`, пароль `admin`.

Если порт 8080 занят другим окружением Airflow, остановите его (`docker compose down` в той папке)
или поменяйте проброс порта в секции `ports` сервиса `airflow-webserver`.

Полезные команды:

```bash
docker compose logs -f airflow-scheduler   # логи планировщика
docker compose down                        # остановить контейнеры
docker compose down -v                     # остановить и удалить БД метаданных
```

## 3. Запуск DAG

DAG создаются на паузе, поэтому их нужно включить переключателем в интерфейсе и запустить кнопкой
Trigger DAG. Порядок важен: второй DAG читает таблицу, которую создаёт первый.

1. `prepare_flats_dataset` (файл `dags/flats_dataset.py`), задачи
   `create_table -> extract -> transform -> load`. Читает `flats` и `buildings` из общей БД одним
   запросом с `left join` и складывает результат в таблицу `flats_dataset` личной БД. Повторный запуск
   не создаёт дублей: строки обновляются по `flat_id`.
2. `clean_flats_dataset` (файл `dags/clean_flats_dataset.py`), задачи те же четыре. Читает
   `flats_dataset` из личной БД, чистит данные и пишет результат в `clean_flats_dataset` там же.
   Задача `transform` применяет функции из `plugins/steps/clean_flats.py` по порядку:
   `fill_missing_values`, `remove_duplicates`, `remove_outliers`.

То же самое из консоли:

```bash
docker compose exec airflow-scheduler airflow dags unpause prepare_flats_dataset
docker compose exec airflow-scheduler airflow dags trigger prepare_flats_dataset
# когда первый DAG отработал
docker compose exec airflow-scheduler airflow dags unpause clean_flats_dataset
docker compose exec airflow-scheduler airflow dags trigger clean_flats_dataset
```

После каждого запуска в Telegram приходит сообщение об успехе или об ошибке
(`plugins/steps/messages.py`, функции `send_telegram_success_message` и `send_telegram_failure_message`).
Логи задач лежат в папке `logs/`.

## 4. Ноутбуки

Ноутбуки запускаются локально, не в контейнере, и подключаются к базам напрямую через SQLAlchemy.
Доступы они берут из того же файла `.env`. Нужны пакеты `pandas`, `sqlalchemy`, `psycopg2-binary`,
`python-dotenv`, `matplotlib`, `seaborn` - все они есть в окружении второй части
(`part2_dvc/requirements.txt`), так что можно работать в нём.

* `1_explore_source_tables.ipynb` - подключается к общей БД (`DB_SOURCE_*`): смотрю таблицы
  `buildings` и `flats`, проверяю связь между ними и собираю JOIN-запрос, который потом ушёл
  в задачу `extract` первого DAG.
* `2_data_cleaning.ipynb` - подключается к личной БД (`DB_DESTINATION_*`): ищу в `flats_dataset`
  пропуски, дубликаты и выбросы, пишу и проверяю функции очистки. Готовые функции вынесены
  в `plugins/steps/clean_flats.py` и используются вторым DAG.
