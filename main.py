from fastapi import FastAPI, Request
from dotenv import load_dotenv
import os
import datetime
import boto3
import logging
from pathlib import Path
import json

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

KROLIK_FILE = DATA_DIR / "krolik.json"

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

def save_krolik(sub1: str, sub5: str, sum_value: str):
    """Сохраняет данные в krolik.json, сгруппированные по дате."""

    today = datetime.datetime.now().strftime("%d.%m.%Y")

    # Загружаем текущие данные, если файл существует
    data = []
    if KROLIK_FILE.exists():
        try:
            with open(KROLIK_FILE, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logging.warning("Файл krolik.json повреждён, пересоздаём его.")
            data = []

    # Находим блок для сегодняшнего дня
    today_block = next((item for item in data if item["day"] == today), None)
    if not today_block:
        today_block = {"day": today, "data": {}}
        data.append(today_block)

    # Добавляем или обновляем запись
    today_block["data"][sub5] = sum_value

    # Сохраняем обратно
    with open(KROLIK_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logging.info(f"Сохранено в krolik.json: {sub5} -> {sum_value}")

@app.api_route("/postback", methods=["GET", "POST"])
async def receive_postback(request: Request):
    params = dict(request.query_params)
    sub1 = params.get("sub1")
    sub5 = params.get("sub5")
    sub6 = params.get("sub6")
    sum_value = params.get("sum") or params.get("payout") or "0"

    # === Обработка sub6 ===
    if sub6 and sub6.isdigit():
        filename = get_today_filename()
        with open(filename, "a") as f:
            f.write(f"{sub6}\n")
        logging.info(f"Получен и сохранён sub6: {sub6}")
    else:
        logging.warning(f"Некорректный sub6 (пропущен): {sub6}")

    # === Обработка krolik (если слово встречается в sub1) ===
    if sub1 and "krolik" in sub1.lower() and sub5:
        save_krolik(sub1, sub5, sum_value)

    return {"status": "ok"}

@app.get("/")
async def root():
    return {"status": "running"}

# === VK Checker Mini App ===
import sys
sys.path.append("/opt")  # добавляем корень, где лежит vk_checker

try:
    from vk_checker.webapp.app import app as vk_checker_app
    app.mount("/dashboard", vk_checker_app)
    logging.info("VK Checker подключён к /dashboard")
except Exception as e:
    logging.warning(f"VK Checker не найден или не загружен: {e}")
