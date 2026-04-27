from fastapi import FastAPI, Request
from dotenv import load_dotenv
import os
import datetime
import boto3
import logging
from pathlib import Path
import json
from typing import Optional
import threading

VERSION="1.21"

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

STAT_INCOME_DIR = DATA_DIR / "stat_lt_income"
STAT_INCOME_DIR.mkdir(parents=True, exist_ok=True)

def get_stat_income_file() -> Path:
    """Возвращает путь к файлу stat_lt_income с еженедельной ротацией.
    Неделя считается по ISO (понедельник = начало недели).
    Формат имени: stat_lt_income_YYYY_W<номер_недели>.json
    """
    now = datetime.datetime.now()
    year, week, _ = now.isocalendar()
    return STAT_INCOME_DIR / f"stat_lt_income_{year}_W{week:02d}.json"
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
VYDAVAYKA_FILE = DATA_DIR / "vydavayka.json"
ZARPLATKINRF_FILE = DATA_DIR / "zarplatkinrf.json"
OREL_FILE = DATA_DIR / "orel.json"

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

def save_stat_income(sub1_name: str, sub5: str, date_str: str, sum_value: str, sub6: str, sub2: str = "", status: str = ""):
    """Сохраняет все постбэки в stat_lt_income (еженедельный файл в отдельной директории).
    Поля: sub1, sub2, sub5, sub6, sum, status, date.
    """
    stat_file = get_stat_income_file()

    record = {
        "sub1": sub1_name,
        "sub2": sub2,
        "sub5": sub5,
        "sub6": sub6,
        "sum": sum_value,
        "status": status,
        "date": date_str or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    data = []
    if stat_file.exists():
        try:
            with open(stat_file, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Файл {stat_file.name} повреждён, пересоздаём.")
            data = []

    data.append(record)

    with open(stat_file, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logging.info(f"[{stat_file.name}] Добавлена запись: {record}")


@app.api_route("/postback", methods=["GET", "POST"])
async def receive_postback(request: Request):
    params = dict(request.query_params)
    sub1 = params.get("sub1")
    sub2 = params.get("sub2") or ""
    sub5 = params.get("sub5")
    sub6 = params.get("sub6")
    sum_value = params.get("sum") or "0"
    status = str(params.get("status"))
    date_str = params.get("date") or ""
    #date_str = params.get("date") or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # === Сохраняем ВСЕ постбэки в stat_lt_income (еженедельная ротация) ===
    save_stat_income(
        sub1_name=sub1 or "",
        sub5=sub5 or "",
        date_str=date_str,
        sum_value=sum_value,
        sub6=sub6 or "",
        sub2=sub2,
        status=status,
    )

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
        elif "karakoz" in sub1_lower or "karas" in sub1_lower:
            save_daily_sum(KARAKOZ_FILE, sub5, sum_value)
        elif "1russ" in sub1_lower or "darya" in sub1_lower or "vadimtop" in sub1_lower or "clickchirik" in sub1_lower:
            save_daily_sum(ONERUSS_FILE, sub5, sum_value)
        elif "vydavayka" in sub1_lower:
            save_daily_sum(VYDAVAYKA_FILE, sub5, sum_value)
        elif "insta" in sub1_lower or "kud" in sub1_lower:
            save_daily_sum(INSTA_FILE, sub5, sum_value)
        elif "utkavalutkarf" in sub1_lower:
            save_daily_sum(UTKAVALUTKA_FILE, sub5, sum_value)
        elif "monzi" in sub1_lower:
            save_daily_sum(MONZI_FILE, sub5, sum_value)
        elif "lisicka" in sub1_lower:
            save_daily_sum(LISICKA_FILE, sub5, sum_value)
        elif "ptichka" in sub1_lower:
            save_daily_sum(PTICHKA_FILE, sub5, sum_value)
        elif "kupr" in sub1_lower:
            save_daily_sum(KUPR_FILE, sub5, sum_value)
        elif "nalickinrf" in sub1_lower:
            save_daily_sum(NALICKINRF_FILE, sub5, sum_value)
        elif "zarplatkinrf" in sub1_lower:
            save_daily_sum(ZARPLATKINRF_FILE, sub5, sum_value)
        elif "zaymdozp" in sub1_lower:
            save_daily_sum(ZAYMDOZP_FILE, sub5, sum_value)
        elif "pchelkazaim" in sub1_lower:
            save_daily_sum(PCHELKA_FILE, sub5, sum_value)
        elif "orel" in sub1_lower:
            save_daily_sum(OREL_FILE, sub5, sum_value)
    else:
        logging.warning(
            f"Пропущен постбэк: sub1={sub1}, sub5={sub5}, sum={sum_value}, status={status}"
        )

    return {"status": "ok"}

# --- TRAFFIC_BH endpoint/helpers ---

TRAFFIC_DIR = DATA_DIR / "traffic_bh"
TRAFFIC_DIR.mkdir(parents=True, exist_ok=True)

def _get_traffic_filename() -> Path:
    """Ежедневный файл для трафика (jsonl)."""
    # Формат: traffic_bh_YYYY-MM-DD.jsonl
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return TRAFFIC_DIR / f"traffic_bh_{today}.jsonl"

def append_traffic_record(banner_id: str, user_id: str, extra: Optional[dict] = None) -> None:
    """Добавляет одну запись в jsonl файл с временной меткой."""
    if extra is None:
        extra = {}

    # Временная метка с часовым поясом сервера (ISO 8601)
    ts = datetime.datetime.now().astimezone().isoformat()

    record = {
        "timestamp": ts,
        "banner_id": banner_id,
        "user_id": user_id,
    }
    # добавляем дополнительные поля, если передали
    record.update(extra)

    file_path = _get_traffic_filename()
    try:
        # Открываем в append и пишем одну строку JSON
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                # fsync может не поддерживаться в некоторых FS — не критично
                pass
        logging.info(f"[traffic_bh] saved: {record}")
    except Exception as e:
        logging.exception(f"Ошибка записи в {file_path}: {e}")
        raise

@app.post("/traffic_bh")
async def receive_traffic_bh(request: Request):
    """
    Ожидает POST с banner_id и user_id.
    Поддерживается: JSON body, form-data/x-www-form-urlencoded и query params (на случай простого GET-теста).
    """
    # Попробуем несколько источников параметров (JSON -> form -> query)
    banner_id = None
    user_id = None
    extra = {}

    # 1) JSON body
    try:
        body = await request.json()
        if isinstance(body, dict):
            banner_id = body.get("banner_id") or body.get("bannerId")
            user_id = body.get("user_id") or body.get("userId")
            # любые дополнительные поля положим в extra
            for k, v in body.items():
                if k not in ("banner_id", "bannerId", "user_id", "userId"):
                    extra[k] = v
    except Exception:
        # не JSON — пропускаем
        pass

    # 2) если не в JSON — пробуем form data / x-www-form-urlencoded
    if not banner_id or not user_id:
        try:
            form = await request.form()
            if not banner_id:
                banner_id = form.get("banner_id") or form.get("bannerId")
            if not user_id:
                user_id = form.get("user_id") or form.get("userId")
            for k in form.keys():
                if k not in ("banner_id", "bannerId", "user_id", "userId"):
                    extra[k] = form.get(k)
        except Exception:
            pass

    # 3) query params (fallback)
    params = dict(request.query_params)
    if not banner_id:
        banner_id = params.get("banner_id") or params.get("bannerId")
    if not user_id:
        user_id = params.get("user_id") or params.get("userId")
    # добавим query params в extra (необязательно)
    for k, v in params.items():
        if k not in ("banner_id", "bannerId", "user_id", "userId"):
            extra.setdefault(k, v)

    # Валидация
    if not banner_id or not user_id:
        logging.warning(f"Пропущен traffic_bh постбэк: banner_id={banner_id}, user_id={user_id}")
        return {"status": "error", "message": "banner_id and user_id are required"}, 400

    # Сохраняем
    try:
        append_traffic_record(str(banner_id), str(user_id), extra if extra else None)
    except Exception as e:
        # логируем ошибку и возвращаем 500
        logging.exception(f"Не удалось сохранить traffic_bh запись: {e}")
        return {"status": "error", "message": "failed to save record"}, 500

    return {"status": "ok"}
# --- /TRAFFIC_BH end ---


# === A/B TEST для чат-ботов ===
AB_TEST_DIR = DATA_DIR / "ab_test"
AB_TEST_DIR.mkdir(parents=True, exist_ok=True)

# Счётчики для round-robin распределения веток (потокобезопасные)
# Ключ: "account_name:count" — отдельный счётчик для каждой комбинации
_branch_counters: dict[str, int] = {}
_branch_lock = threading.Lock()


def _get_ab_test_file(account_name: str) -> Path:
    """Возвращает путь к файлу для конкретного аккаунта."""
    # Очищаем имя от потенциально опасных символов
    safe_name = "".join(c for c in account_name if c.isalnum() or c in ("_", "-"))
    if not safe_name:
        safe_name = "default"
    return AB_TEST_DIR / f"{safe_name}.json"


def _get_next_branch(account_name: str, count: int) -> int:
    """
    Возвращает следующую ветку для round-robin распределения.
    Потокобезопасно.
    
    Счётчик хранится отдельно для каждой комбинации account_name + count,
    чтобы при смене count распределение начиналось заново с ветки 1.
    """
    # Ключ включает count, чтобы при смене количества веток счёт начинался заново
    counter_key = f"{account_name}:{count}"
    
    with _branch_lock:
        current = _branch_counters.get(counter_key, 0)
        next_branch = (current % count) + 1  # Ветки от 1 до count
        _branch_counters[counter_key] = current + 1
        return next_branch


# Часовой пояс UTC+4
from datetime import timezone
UTC_PLUS_4 = timezone(datetime.timedelta(hours=4))


def _load_ab_data(file_path: Path) -> dict:
    """
    Загружает данные A/B теста.
    Структура:
    {
        "users": {
            "<user_id>": {
                "banner_id": "...",
                "branch": 1,
                "steps": [
                    {"step": 0, "timestamp": "...", "time_from_prev": null},
                    {"step": 1, "timestamp": "...", "time_from_prev": 45.2},
                    {"step": 2.1, "timestamp": "...", "time_from_prev": 30.5}
                ],
                "first_seen": "...",
                "last_seen": "..."
            }
        },
        "stats": {
            "total_users": 100,
            "by_branch": {1: 50, 2: 50},
            "by_step": {"0": 100, "1": 80, "2.1": 30, "2.2": 25}
        }
    }
    """
    if file_path.exists():
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Файл {file_path} повреждён, пересоздаём.")
    return {"users": {}, "stats": {"total_users": 0, "by_branch": {}, "by_step": {}}}


def _save_ab_data(file_path: Path, data: dict) -> None:
    """Сохраняет данные A/B теста."""
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _update_stats(data: dict) -> None:
    """Пересчитывает статистику на основе данных пользователей."""
    users = data.get("users", {})
    
    total_users = len(users)
    by_branch: dict[int, int] = {}
    by_step: dict[str, int] = {}
    
    for user_data in users.values():
        branch = user_data.get("branch")
        if branch:
            by_branch[branch] = by_branch.get(branch, 0) + 1
        
        for step_record in user_data.get("steps", []):
            step_key = str(step_record["step"])
            by_step[step_key] = by_step.get(step_key, 0) + 1
    
    data["stats"] = {
        "total_users": total_users,
        "by_branch": by_branch,
        "by_step": by_step
    }


def process_ab_test_event(
    banner_id: str,
    user_id: str,
    step: float,
    account_name: str,
    count: Optional[int] = None
) -> dict:
    """
    Обрабатывает событие A/B теста.
    
    Возвращает:
    - branch: номер ветки (присваивается ТОЛЬКО при первом step=0, потом не меняется)
    - is_new_user: True если это новый пользователь
    """
    file_path = _get_ab_test_file(account_name)
    data = _load_ab_data(file_path)
    
    now = datetime.datetime.now(UTC_PLUS_4)
    now_iso = now.isoformat()
    
    is_new_user = user_id not in data["users"]
    
    if is_new_user:
        # Новый пользователь — присваиваем ветку только здесь
        if step == 0 and count and count > 0:
            branch = _get_next_branch(account_name, count)
        else:
            branch = 1  # По умолчанию ветка 1
        
        data["users"][user_id] = {
            "banner_id": banner_id,
            "branch": branch,
            "steps": [
                {"step": step, "timestamp": now_iso, "time_from_prev": None}
            ],
            "first_seen": now_iso,
            "last_seen": now_iso
        }
    else:
        # Существующий пользователь — ВСЕГДА сохраняем его изначальную ветку
        user_data = data["users"][user_id]
        branch = user_data.get("branch", 1)  # Ветка НЕ меняется, даже если пришёл step=0 с другим count
        
        # Вычисляем время от предыдущего шага
        time_from_prev = None
        if user_data["steps"]:
            last_step = user_data["steps"][-1]
            try:
                last_time = datetime.datetime.fromisoformat(last_step["timestamp"])
                time_from_prev = round((now - last_time).total_seconds(), 2)
            except Exception:
                pass
        
        # Добавляем новый шаг (включая повторный step=0 если вдруг пришёл)
        user_data["steps"].append({
            "step": step,
            "timestamp": now_iso,
            "time_from_prev": time_from_prev
        })
        user_data["last_seen"] = now_iso
        
        # Обновляем banner_id если изменился (не должно, но на всякий)
        if banner_id and banner_id != user_data.get("banner_id"):
            user_data["banner_id"] = banner_id
    
    # Пересчитываем статистику
    _update_stats(data)
    
    # Сохраняем
    _save_ab_data(file_path, data)
    
    logging.info(
        f"[ab_test/{account_name}] user={user_id}, banner={banner_id}, "
        f"step={step}, branch={branch}, new={is_new_user}"
    )
    
    return {"branch": branch, "is_new_user": is_new_user}


@app.post("/traffic_bh/ab_test")
async def receive_ab_test(request: Request):
    """
    A/B тест для чат-ботов.
    
    Параметры:
    - banner_id (str): ID баннера/источника
    - user_id (str): ID пользователя
    - step (float): Номер шага (0 = вход, 1, 2.1, 2.2 и т.д.)
    - account_name (str): Название файла для записи
    - count (int, опционально): Количество веток (только для step=0)
    
    Возвращает:
    - status: "ok"
    - branch: Номер ветки пользователя (int)
    - is_new_user: Новый ли пользователь (bool)
    """
    # Собираем все параметры в один словарь
    all_params = {}
    
    # 1) Query params (всегда доступны)
    all_params.update(dict(request.query_params))
    
    # 2) Определяем тип контента и читаем body
    content_type = request.headers.get("content-type", "")
    
    if "application/json" in content_type:
        # JSON body
        try:
            body = await request.json()
            if isinstance(body, dict):
                all_params.update(body)
        except Exception as e:
            logging.warning(f"[ab_test] JSON parse error: {e}")
    else:
        # Form data (multipart/form-data или x-www-form-urlencoded)
        try:
            form = await request.form()
            for key in form.keys():
                # Убираем лишние пробелы из ключей (на всякий случай)
                clean_key = key.strip()
                all_params[clean_key] = form.get(key)
        except Exception as e:
            logging.warning(f"[ab_test] Form parse error: {e}")
    
    # Извлекаем нужные параметры
    banner_id = all_params.get("banner_id") or all_params.get("bannerId")
    user_id = all_params.get("user_id") or all_params.get("userId")
    step = all_params.get("step")
    account_name = all_params.get("account_name") or all_params.get("accountName")
    count = all_params.get("count")

    # Преобразование типов
    try:
        step = float(step) if step is not None else None
    except (ValueError, TypeError):
        return {"status": "error", "message": "step must be a number"}, 400

    try:
        count = int(count) if count is not None else None
    except (ValueError, TypeError):
        count = None

    # Валидация
    if not banner_id or not user_id or step is None or not account_name:
        missing = []
        if not banner_id:
            missing.append("banner_id")
        if not user_id:
            missing.append("user_id")
        if step is None:
            missing.append("step")
        if not account_name:
            missing.append("account_name")
        
        logging.warning(f"Пропущен ab_test постбэк, отсутствуют: {missing}, all_params={all_params}")
        return {
            "status": "error",
            "message": f"Missing required parameters: {', '.join(missing)}"
        }, 400

    # Обрабатываем событие
    try:
        result = process_ab_test_event(
            banner_id=str(banner_id),
            user_id=str(user_id),
            step=step,
            account_name=str(account_name),
            count=count
        )
    except Exception as e:
        logging.exception(f"Ошибка обработки ab_test: {e}")
        return {"status": "error", "message": "failed to process event"}, 500

    return {
        "status": "ok",
        "branch": result["branch"],
        "is_new_user": result["is_new_user"]
    }


@app.get("/traffic_bh/ab_test/stats/{account_name}")
async def get_ab_test_stats(account_name: str):
    """
    Получить статистику A/B теста для аккаунта.
    """
    file_path = _get_ab_test_file(account_name)
    
    if not file_path.exists():
        return {"status": "error", "message": "account not found"}, 404
    
    data = _load_ab_data(file_path)
    
    return {
        "status": "ok",
        "account": account_name,
        "stats": data.get("stats", {}),
        "users_count": len(data.get("users", {}))
    }


@app.get("/traffic_bh/ab_test/user/{account_name}/{user_id}")
async def get_ab_test_user(account_name: str, user_id: str):
    """
    Получить данные конкретного пользователя.
    """
    file_path = _get_ab_test_file(account_name)
    
    if not file_path.exists():
        return {"status": "error", "message": "account not found"}, 404
    
    data = _load_ab_data(file_path)
    user_data = data.get("users", {}).get(user_id)
    
    if not user_data:
        return {"status": "error", "message": "user not found"}, 404
    
    return {
        "status": "ok",
        "user_id": user_id,
        "data": user_data
    }

# === /A/B TEST end ===


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

# === VK CHECKER V4 ===
try:
    from vk_checker.v4.webapp.app import app as vk_checker_v4_app
    app.mount("/vk_checker_v4", vk_checker_v4_app)
    logging.info("VK Checker подключён к /vk_checker_v4")
except Exception as e:
    logging.warning(f"VK Checker_v4 не найден или не загружен: {e}")
