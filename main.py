from fastapi import FastAPI, Request
from dotenv import load_dotenv
import os
import datetime
import boto3
import logging
from pathlib import Path

# === Логи ===
LOG_FILE = "/opt/leads_postback/postback.log"
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

app = FastAPI()

# === S3 ===
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

def get_today_filename() -> Path:
    today = datetime.datetime.now().strftime("%d.%m.%Y")
    return DATA_DIR / f"leads_sub6_{today}.txt"


@app.post("/postback")
async def receive_postback(request: Request):
    params = dict(request.query_params)
    sub6 = params.get("sub6")

    # Сохраняем только если это цифры
    if sub6 and sub6.isdigit():
        filename = get_today_filename()
        with open(filename, "a") as f:
            f.write(f"{sub6}\n")
        logging.info(f"Получен и сохранён sub6: {sub6}")
    else:
        logging.warning(f"Некорректный sub6 (пропущен): {sub6}")

    return {"status": "ok"}


@app.get("/")
async def root():
    return {"status": "running"}
