#!/usr/bin/env python3
import os
import datetime
import boto3
import logging
from pathlib import Path
from dotenv import load_dotenv

# === Логи ===
LOG_FILE = "/opt/leads_postback/upload.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# === Настройки ===
load_dotenv()
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

DATA_DIR = Path("/opt/leads_postback/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

today = datetime.datetime.now().strftime("%d.%m.%Y")
filename = DATA_DIR / f"leads_sub6_{today}.txt"

if filename.exists():
    s3_key = f"leads_sub6/leads_sub6_{today}.txt"
    try:
        s3.upload_file(str(filename), S3_BUCKET, s3_key)
        logging.info(f"✅ Файл {filename} успешно загружен в S3 как {s3_key}")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке в S3: {e}")
else:
    logging.info("⚠️ Файл за день отсутствует — пропуск загрузки.")

# Создаём новый файл на завтра
tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%d.%m.%Y")
new_file = DATA_DIR / f"leads_sub6_{tomorrow}.txt"
new_file.touch(exist_ok=True)
logging.info(f"🆕 Создан файл на завтра: {new_file}")
