import asyncio
import logging
import json
import os
import httpx
from aiogram.types import FSInputFile
from datetime import datetime

from config import bot, dp, r, DB_SERVICE_URL
from handlers import router
from utils import generate_html_report
import messages

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot.main")

async def listen_for_completions():
    logger.info("Background listener started...")
    while True:
        try:
            msg = await r.brpop("search_completions", timeout=5)
            if not msg: continue
            data = json.loads(msg[1])
            query, chat_id = data.get("query"), data.get("chat_id")
            
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{DB_SERVICE_URL}/search", params={"q": query}, timeout=60.0)
                results = response.json() if response.status_code == 200 else []
            
            if results:
                html_content = generate_html_report(query, results, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                filename = f"report_{chat_id}_{int(asyncio.get_event_loop().time())}.html"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(html_content)
                await bot.send_document(chat_id, FSInputFile(filename), caption=messages.REPORT_CAPTION.format(query=query), parse_mode="Markdown")
                if os.path.exists(filename): os.remove(filename)
        except Exception as e:
            logger.error(f"Error in listener: {e!r}")
            await asyncio.sleep(1)

async def main():
    logger.info("Starting bot...")
    
    # Чекаємо на Redis
    for i in range(10):
        try:
            await r.ping()
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
    
    # Запуск фонової задачі (відключено через помилки з'єднання з неіснуючим сервісом)
    # asyncio.create_task(listen_for_completions())

    
    # Запуск поллинга
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
