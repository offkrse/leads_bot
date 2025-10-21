import os
import asyncio
import datetime
import aiohttp
import boto3
from dotenv import load_dotenv
import pytz

# Загрузка .env
load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Moscow")

# Инициализация S3
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

async def send_file(file_path, caption):
    async with aiohttp.ClientSession() as session:
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field("caption", caption)
            form.add_field("document", f)
            await session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data=form
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
                s3.download_file(S3_BUCKET, filename, local_path)
                caption = f"📊 Файл sub6 за {yesterday}"
                await send_file(local_path, caption)
                print(f"[✅] Отчёт {filename} отправлен в Telegram")
            except Exception as e:
                print(f"[❌] Ошибка при загрузке или отправке файла: {e}")

            await asyncio.sleep(60)
        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(daily_send())
