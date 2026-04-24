import os
import re
import asyncio
import logging
from aiogram import types, F, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile

import messages
from config import bot, r
from database import register_user
from utils import generate_html_report
from search_service import search_across_tables, service as search_service

router = Router()
logger = logging.getLogger("bot.handlers")

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await message.answer(messages.START_MESSAGE)

@router.message(Command("stats", "статс"))
async def cmd_stats(message: types.Message):
    try:
        # Запрашиваем только общее количество (это очень быстро)
        success, res_data = await search_service.execute_raw_sql("SELECT count() FROM global_search_n")
        
        if not success or not res_data:
            await message.answer("❌ Не вдалося отримати статистику.")
            return
        
        total_count = res_data[0][0]
        
        # Форматируем число с пробелами (например, 506 123 456)
        formatted_count = f"{total_count:,}".replace(",", " ")
        
        # Минималистичное сообщение без сложной разметки
        stats_msg = (
            f"📊 <b>Статистика бази даних</b>\n\n"
            f"📈 <b>Всього записів:</b> <code>{formatted_count}</code>\n"
            f"⚡️ <i>Система працює в штатному режимі</i>"
        )
        
        await message.answer(stats_msg, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error in cmd_stats: {e}")
        await message.answer("⚠️ Помилка при виведенні статистики.")
@router.message(F.text)
async def handle_search(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    raw_text = message.text.strip()
    logger.info(f"===> Поиск от {message.from_user.id}: '{raw_text}'")
    
    manual_field = None
    query_content = raw_text
    
    prefixes = {
        "адрес:": "address", "адреса:": "address",
        "авто:": "transport",
        "пасп:": "passport", "паспорт:": "passport",
        "инн:": "inn", "іпн:": "inn",
        "снилс:": "snils", "снілс:": "snils", "сн:": "snils", "sn:": "snils", "сс:": "snils",
        "тел:": "phone", "номер:": "phone",
        "email:": "email", "тг:": "tg_id",
        "id:": "tg_id", "дата:": "birth_date"
    }

    for prefix, field in prefixes.items():
        if raw_text.lower().startswith(prefix):
            manual_field = field
            query_content = raw_text[len(prefix):].strip()
            break

    pre_parts = [p.strip() for p in re.split(r'[,;]', query_content) if p.strip()]
    if len(pre_parts) > 1:
        types_detected = [search_service.detect_search_field(p) for p in pre_parts]
        if types_detected[0] == "fio" and any(t in ["birth_date", "phone", "passport", "inn", "snils"] for t in types_detected[1:]):
            query_parts = [query_content.replace(",", " ")]
        else:
            query_parts = pre_parts
    else:
        query_parts = pre_parts

    if not query_parts:
        if manual_field: await message.answer("⚠️ Пустой запрос.")
        return

    search_label = raw_text if len(query_parts) == 1 else f"{len(query_parts)} запитів"
    msg = await message.answer(messages.SEARCH_START.format(query=search_label), parse_mode="Markdown")

    master_grouped = {}
    for part in query_parts:
        results_matrix = await search_across_tables(part, manual_field=manual_field)
        for table_data in results_matrix:
            if len(table_data) > 1:
                source = table_data[0]
                if source not in master_grouped: master_grouped[source] = []
                seen_hashes = {hash(frozenset(r.items())) for r in master_grouped[source]}
                for row in table_data[1:]:
                    row_hash = hash(frozenset(row.items()))
                    if row_hash not in seen_hashes:
                        master_grouped[source].append(row)
                        seen_hashes.add(row_hash)

    combined_results_matrix = []
    for source, source_rows in master_grouped.items():
        combined_results_matrix.append([source] + source_rows)
    
    local_results_count = sum(len(rows) for rows in master_grouped.values())
    
    if local_results_count > 0:
        await msg.edit_text(messages.SEARCH_SUCCESS.format(count=local_results_count))
        
        from datetime import datetime
        html_content = generate_html_report(raw_text, combined_results_matrix, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        filename = f"report_{message.chat.id}_{int(asyncio.get_event_loop().time())}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        await bot.send_document(message.chat.id, FSInputFile(filename), caption=messages.REPORT_CAPTION.format(query=raw_text), parse_mode="Markdown")
        if os.path.exists(filename):
            os.remove(filename)
    else:
        await msg.edit_text(messages.SEARCH_NOT_FOUND)
