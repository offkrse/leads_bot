import os
import asyncio
import datetime
import aiohttp
import boto3
from dotenv import load_dotenv
import pytz
import logging

# Загрузка .env
load_dotenv()

# --- Настройки S3 ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

# --- Настройки основного бота ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Настройки бота для ошибок ---
ERROR_BOT_TOKEN = os.getenv("ERROR_BOT_TOKEN")
ERROR_CHAT_ID = os.getenv("ERROR_CHAT_ID")

# --- Настройки времени ---
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

# --- Логирование ---
LOG_FILE = "/opt/leads_postback/bot_send.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Инициализация S3 ---
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

# --- Функции ---
async def send_file(file_path: str):
    """Отправка файла в Telegram без текста"""
    async with aiohttp.ClientSession() as session:
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field("document", f)
            await session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data=form
            )

async def send_error(message: str):
    """Отправка ошибок через отдельного бота (в личку)"""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.error("Ошибка: не заданы ERROR_BOT_TOKEN или ERROR_CHAT_ID")
        return

    async with aiohttp.ClientSession() as session:
        await session.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            data={"chat_id": ERROR_CHAT_ID, "text": message}
        )

async def daily_send():
    """Каждый день в 9:00 по МСК отправляет файл из S3 в Telegram"""
    tz = pytz.timezone(TIMEZONE)

    while True:
        now = datetime.datetime.now(tz)
        if now.hour == 9 and now.minute == 0:
            yesterday = (now - datetime.timedelta(days=1)).strftime("%d.%m.%Y")
            filename = f"leads_sub6_{yesterday}.txt"
            local_path = f"/opt/leads_postback/{filename}"

            try:
                # Скачиваем файл из S3
                s3.download_file(S3_BUCKET, filename, local_path)
                # Отправляем в Telegram
                await send_file(local_path)
                logging.info(f"[✅] Файл {filename} успешно отправлен в Telegram")

                # После успешной отправки — удаляем локальный файл
                if os.path.exists(local_path):
                    os.remove(local_path)
                    logging.info(f"[🧹] Локальный файл {local_path} удалён")

            except Exception as e:
                error_message = f"Error bot_send.py: {str(e)}"
                logging.error(error_message)
                await send_error(error_message)

            await asyncio.sleep(60)  # чтобы не повторял в ту же минуту
        await asyncio.sleep(30)

if __name__ == "__main__":
    logging.info("=== bot_send.py запущен ===")
    try:
        asyncio.run(daily_send())
    except Exception as e:
        logging.critical(f"Фатальная ошибка: {e}")
        try:
            asyncio.run(send_error(f"Error bot_send.py: {e}"))
        except:
            pass
