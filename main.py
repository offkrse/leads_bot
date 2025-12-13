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

STAT_INCOME_FILE = DATA_DIR / "stat_lt_income.json"
KROLIK_FILE = DATA_DIR / "krolik.json"
KARAKOZ_FILE = DATA_DIR / "karakoz_karas.json"
INSTA_FILE = DATA_DIR / "insta.json"
UTKAVALUTKA_FILE = DATA_DIR / "utkavalutkarf.json"
MONZI_FILE = DATA_DIR / "monzi.json"
LISICKA_FILE = DATA_DIR / "lisicka.json"
PTICHKA_FILE = DATA_DIR / "ptichka.json"
KUPR_FILE = DATA_DIR / "kupr.json"
ZAYMDOZP_FILE = DATA_DIR / "zaymdozp.json"
PCHELKA_FILE = DATA_DIR / "pchelkazaim.json"
NALICKINRF_FILE = DATA_DIR / "nalickinrf.json"
BANKNOTA_FILE = DATA_DIR / "banknota.json"
ONERUSS_FILE = DATA_DIR / "1russ.json"

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

def save_daily_sum(file_path: Path, sub5: str, sum_value: str):
    """Сохраняет данные в JSON по дням. Если sub5 повторяется — суммирует."""
    today = datetime.datetime.now().strftime("%d.%m.%Y")

    # Загружаем существующие данные
    data = []
    if file_path.exists():
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Файл {file_path.name} повреждён, пересоздаём.")
            data = []

    # Находим блок для сегодняшнего дня
    today_block = next((item for item in data if item["day"] == today), None)
    if not today_block:
        today_block = {"day": today, "data": {}}
        data.append(today_block)

    # Преобразуем сумму
    try:
        sum_float = float(sum_value)
    except ValueError:
        logging.warning(f"Некорректное значение sum: {sum_value}")
        return

    # Суммируем при повторном sub5
    if sub5 in today_block["data"]:
        try:
            old_sum = float(today_block["data"][sub5])
        except ValueError:
            old_sum = 0
        new_sum = old_sum + sum_float
    else:
        new_sum = sum_float

    today_block["data"][sub5] = round(new_sum, 2)

    # Сохраняем обратно
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logging.info(f"[{file_path.name}] {sub5} -> {new_sum}")

def save_stat_income(sub1_name: str, sub5: str, date_str: str, sum_value: str, sub6: str):
    """Сохраняет группу (sub1), sub5, дату, сумму и sub6 в stat_lt_income.json"""

    record = {
        "sub1": sub1_name,
        "sub5": sub5,
        "sum": sum_value,
        "sub6": sub6,
        "date": date_str or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    data = []
    if STAT_INCOME_FILE.exists():
        try:
            with open(STAT_INCOME_FILE, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logging.warning("Файл stat_lt_income.json повреждён, пересоздаём.")
            data = []

    data.append(record)

    with open(STAT_INCOME_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logging.info(f"[stat_lt_income.json] Добавлена запись: {record}")


@app.api_route("/postback", methods=["GET", "POST"])
async def receive_postback(request: Request):
    params = dict(request.query_params)
    sub1 = params.get("sub1")
    sub5 = params.get("sub5")
    sub6 = params.get("sub6")
    sum_value = params.get("sum") or "0"
    status = str(params.get("status"))
    date_str = params.get("date") or ""
    #date_str = params.get("date") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # === Обработка sub6 ===
    if sub6 and sub6.isdigit():
        filename = get_today_filename()
        with open(filename, "a") as f:
            f.write(f"{sub6}\n")
        logging.info(f"Получен и сохранён sub6: {sub6}")
    else:
        logging.warning(f"Некорректный sub6 (пропущен): {sub6}")

    # === Обработка krolik (если слово встречается в sub1) ===
    if (
        sub1
        and sub5
        and sub5.isdigit()          # sub5 — только цифры
        and sum_value not in ("0", "0.0", "0.00")  # sum не равен 0
        and status == "1"
    ):
        sub1_lower = sub1.lower()
    
        if "krolik" in sub1_lower or "banknota" in sub1_lower:
            save_daily_sum(KROLIK_FILE, sub5, sum_value)
            save_stat_income("krolik", sub5, date_str, sum_value, sub6)
        elif "karakoz" in sub1_lower or "karas" in sub1_lower:
            save_daily_sum(KARAKOZ_FILE, sub5, sum_value)
        elif "1russ" in sub1_lower or "darya" in sub1_lower or "vadimtop" in sub1_lower:
            save_daily_sum(ONERUSS_FILE, sub5, sum_value)
            save_stat_income("1russ", sub5, date_str, sum_value, sub6)
        elif "insta" in sub1_lower or "kud" in sub1_lower:
            save_daily_sum(INSTA_FILE, sub5, sum_value)
            save_stat_income("insta", sub5, date_str, sum_value, sub6)
        elif "utkavalutkarf" in sub1_lower:
            save_daily_sum(UTKAVALUTKA_FILE, sub5, sum_value)
            save_stat_income("utkavalutkarf", sub5, date_str, sum_value, sub6)
        elif "monzi" in sub1_lower:
            save_daily_sum(MONZI_FILE, sub5, sum_value)
            save_stat_income("monzi", sub5, date_str, sum_value, sub6)
        elif "lisicka" in sub1_lower:
            save_daily_sum(LISICKA_FILE, sub5, sum_value)
            save_stat_income("lisicka", sub5, date_str, sum_value, sub6)
        elif "ptichka" in sub1_lower:
            save_daily_sum(PTICHKA_FILE, sub5, sum_value)
            save_stat_income("ptichka", sub5, date_str, sum_value, sub6)
        elif "kupr" in sub1_lower:
            save_daily_sum(KUPR_FILE, sub5, sum_value)
            save_stat_income("kupr", sub5, date_str, sum_value, sub6)
        elif "nalickinrf" in sub1_lower:
            save_daily_sum(NALICKINRF_FILE, sub5, sum_value)
            save_stat_income("nalickinrf", sub5, date_str, sum_value, sub6)
        elif "zaymdozp" in sub1_lower:
            save_daily_sum(ZAYMDOZP_FILE, sub5, sum_value)
            save_stat_income("zaymdozp", sub5, date_str, sum_value, sub6)
        elif "pchelkazaim" in sub1_lower:
            save_daily_sum(PCHELKA_FILE, sub5, sum_value)
            save_stat_income("pchelkazaim", sub5, date_str, sum_value, sub6)
    else:
        logging.warning(
            f"Пропущен постбэк: sub1={sub1}, sub5={sub5}, sum={sum_value}, status={status}"
        )

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

# === SKY ADS ===
try:
    from auto_ads.app import app as auto_ads_app
    app.mount("/auto_ads", auto_ads_app)
    logging.info("Auto ADS подключён к /auto_ads/api")
except Exception as e:
    logging.warning(f"Auto ADS не найден или ошибка загрузки: {e}")

