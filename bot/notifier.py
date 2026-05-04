import asyncio
import logging
import json
from config import bot, r, SUPER_ADMIN_IDS
from search_service import service as search_service

logger = logging.getLogger("bot.notifier")

async def notify_super_admins(text: str):
    for admin_id in SUPER_ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Failed to notify super admin {admin_id}: {e}")

async def run_manual_check(user_id: int):
    """
    Виконує перевірку наповнення бази даних і повертає звіт користувачу.
    """
    try:
        # Отримуємо поточні дані з ClickHouse
        current_counts = await search_service.get_all_source_counts()
        if not current_counts:
            return "❌ Не вдалося отримати дані з ClickHouse."
        
        # Отримуємо попередні дані з Redis
        last_counts_raw = await r.get("db_source_counts")
        last_counts = {}
        if last_counts_raw:
            try:
                last_counts = json.loads(last_counts_raw)
            except:
                pass
        
        changes = []
        all_stats = []
        
        for key, count in current_counts.items():
            prev_count = last_counts.get(key, 0)
            table_part, source = key.split(":", 1)
            region = "🇺🇦 UA" if "ua" in table_part else "🇷🇺 RU"
            
            if count > prev_count:
                diff = count - prev_count
                changes.append(f"• <b>{source}</b> ({region}): +{diff} (всього: {count})")
            
            all_stats.append(f"• {source} ({region}): <code>{count}</code>")
        
        # Оновлюємо дані в Redis
        await r.set("db_source_counts", json.dumps(current_counts))
        
        has_changes = len(changes) > 0
        if has_changes:
            msg = "🔔 <b>ЗНАЙДЕНО ПОПОВНЕННЯ!</b>\n\n" + "\n".join(changes)
        else:
            msg = "✅ <b>Змін не виявлено.</b>\n\n<b>Поточна статистика:</b>\n" + "\n".join(all_stats)
            
        return msg, has_changes
                
    except Exception as e:
        logger.error(f"Error in run_manual_check: {e}")
        return f"❌ Помилка при перевірці: {e}", False
