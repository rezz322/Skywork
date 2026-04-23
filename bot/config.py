import os
import logging
import sys
import redis.asyncio as redis
from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

# Загружаем .env
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path, override=True)

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# Токен и URL
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL")

if not TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
    sys.exit(1)

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN)
dp = Dispatcher()
r = redis.from_url(REDIS_URL, decode_responses=True)

# PostgreSQL Config
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB")
}
