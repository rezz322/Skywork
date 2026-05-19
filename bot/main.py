import asyncio
import logging
import json
import os
import httpx
from aiogram.types import FSInputFile
from datetime import datetime

from config import bot, dp, r
from handlers import router
from utils import generate_html_report
import messages

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot.main")

async def main():
    logger.info("Starting bot...")
    
    # Чекаємо на Redis
    for i in range(10):
        try:
            await r.ping()  # type: ignore
            logger.info("Successfully connected to Redis")
            break
        except Exception as e:
            logger.warning(f"Waiting for Redis... (attempt {i+1}/10): {e}")
            await asyncio.sleep(2)
    else:
        logger.error("Could not connect to Redis after 10 attempts. Exiting.")
        return

    # Регистрация роутера с хендлерами
    dp.include_router(router)
    
    # Запуск поллинга
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
