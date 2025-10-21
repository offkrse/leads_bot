from fastapi import FastAPI, Request
from dotenv import load_dotenv
import os
import datetime
import asyncio
import boto3

# Загрузка .env
load_dotenv()

# Настройки
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

DATA_DIR = "/opt/leads_postback/data"
os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI()

# Инициализация S3
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

def get_today_filename():
    today = datetime.datetime.now().strftime("%d.%m.%Y")
    return os.path.join(DATA_DIR, f"leads_sub6_{today}.txt")

@app.post("/postback")
async def receive_postback(request: Request):
    params = dict(request.query_params)
    sub6 = params.get("sub6")
    if sub6:
        filename = get_today_filename()
        with open(filename, "a") as f:
            f.write(f"{sub6}\n")
        print(f"[+] Получен sub6: {sub6}")
    return {"status": "ok"}

async def upload_and_rotate():
    """Каждую ночь загружает файл в S3 и создаёт новый"""
    while True:
        now = datetime.datetime.now()
        # Проверяем полночь по системному времени (лучше поставить UTC+3)
        if now.hour == 0 and now.minute == 0:
            today = now.strftime("%d.%m.%Y")
            filename = os.path.join(DATA_DIR, f"leads_sub6_{today}.txt")

            if os.path.exists(filename):
                s3_key = f"leads_sub6_{today}.txt"
                try:
                    s3.upload_file(filename, S3_BUCKET, s3_key)
                    print(f"[✅] Файл {filename} успешно загружен в S3 как {s3_key}")
                except Exception as e:
                    print(f"[❌] Ошибка при загрузке в S3: {e}")
            else:
                print("[ℹ️] Файл за день отсутствует")

            # Создаём новый файл для следующего дня
            tomorrow = (now + datetime.timedelta(days=1)).strftime("%d.%m.%Y")
            new_file = os.path.join(DATA_DIR, f"leads_sub6_{tomorrow}.txt")
            open(new_file, "a").close()
            print(f"[🆕] Создан файл на завтра: {new_file}")

            await asyncio.sleep(60)  # чтобы не зациклилось на той же минуте
        await asyncio.sleep(30)

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(upload_and_rotate())

@app.get("/")
async def root():
    return {"status": "running"}
