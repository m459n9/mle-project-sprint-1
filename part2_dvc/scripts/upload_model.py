"""Загрузка обученной модели в S3 по понятному пути (запускать после dvc repro)."""
import os

import boto3
from dotenv import load_dotenv


def upload_model():
    # ключи доступа к бакету берём из .env
    load_dotenv()
    bucket = os.environ['S3_BUCKET_NAME']

    # S3 в Яндекс Облаке, поэтому обязательно указываем свой endpoint
    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    )

    key = 'mle-project-sprint-1/models/fitted_model.pkl'
    s3.upload_file('models/fitted_model.pkl', bucket, key)
    print(f'Модель загружена: s3://{bucket}/{key}')

    # проверяем, что файл действительно появился в бакете
    objects = s3.list_objects_v2(Bucket=bucket, Prefix='mle-project-sprint-1/models/')
    print(f'Содержимое бакета {bucket} по этому пути:')
    for obj in objects.get('Contents', []):
        print(f"  {obj['Key']}, {obj['Size']} байт")


if __name__ == '__main__':
    upload_model()
