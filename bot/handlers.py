import os
import re
import asyncio
import logging
from aiogram import types, F, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

import messages
from config import bot, r, AUTH_PASSWORD
from database import register_user, check_subscription, get_user_info, authorize_user
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from utils import generate_html_report
from search_service import search_across_tables, service as search_service
import httpx

router = Router()
logger = logging.getLogger("bot.handlers")

def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(types.KeyboardButton(text="🔍 Пошук"), types.KeyboardButton(text="👤 Кабінет"))
    # builder.row(types.KeyboardButton(text="💳 Оплатити"))
    return builder.as_markup(resize_keyboard=True)

# def get_pay_keyboard():
#     builder = InlineKeyboardBuilder()
#     builder.button(text="1 Місяць ($10)", callback_data="pay_period_1")
#     builder.button(text="3 Місяці ($30)", callback_data="pay_period_3")
#     builder.button(text="6 Місяців ($60)", callback_data="pay_period_6")
#     builder.adjust(1)
#     return builder.as_markup()

# def get_coins_keyboard(period):
#     builder = InlineKeyboardBuilder()
#     # Список популярных монет в CryptoBot
#     coins = ["USDT", "TON", "TRX", "BTC", "ETH"]
#     for coin in coins:
#         builder.button(text=coin, callback_data=f"pay_coin_{period}_{coin}")
#     builder.button(text="⬅️ Назад", callback_data="pay_back_to_periods")
#     builder.adjust(2)
#     return builder.as_markup()

@router.message(Command("start"))
async def cmd_start(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    is_active, _ = check_subscription(message.from_user.id)
    
    if not is_active:
        await message.answer("⚠️ **Доступ обмежено!**\n\nБот захищений паролем. Будь ласка, введіть пароль для доступу до функцій пошуку.", parse_mode="Markdown")
    else:
        await message.answer(messages.START_MESSAGE, reply_markup=get_main_keyboard())

@router.message(Command("stats", "статс"))
async def cmd_stats(message: types.Message):
    is_active, _ = check_subscription(message.from_user.id)
    if not is_active:
        await message.answer("⚠️ **Доступ обмежено!**\nБудь ласка, введіть пароль.", parse_mode="Markdown")
        return
    try:
        success, res_data = await search_service.execute_raw_sql("SELECT count() FROM global_search_n")
        if not success or not res_data:
            return
        
        total_count = res_data[0][0]
        success, sources_data = await search_service.execute_raw_sql(
            "SELECT source_table, count() as cnt FROM global_search_n GROUP BY source_table ORDER BY cnt DESC LIMIT 15"
        )
        
        import html
        stats_msg = f"📊 <b>Статистика бази даних</b>\n\n"
        stats_msg += f"📈 <b>Всього записів:</b> <code>{total_count:,}</code>\n\n".replace(",", " ")
        stats_msg += f"🗂 <b>Топ-15 джерел:</b>\n"
        
        if success and isinstance(sources_data, list):
            for row in sources_data:
                source, count = row
                source_esc = html.escape(str(source))
                stats_msg += f"• {source_esc}: <code>{count:,}</code>\n".replace(",", " ")
        
        await message.answer(stats_msg, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Error in cmd_stats: {e}")

# @router.message(Command("pay"))
# async def cmd_pay(message: types.Message):
#     """Тестовая команда для создания платежа"""
#     try:
#         # Данные для платежа
#         payload = {
#             "price_amount": 10.0,
#             "price_currency": "usd",
#             "order_id": str(message.from_user.id),
#             "order_description": "Підписка на OSINT бот",
#             "success_url": "https://t.me/osrezz_3_bot", 
#             "cancel_url": "https://t.me/osrezz_3_bot"
#         }
#         
#         # Запрос к pay-service (внутри докера)
#         async with httpx.AsyncClient() as client:
#             response = await client.post("http://pay-service:8080/invoice/create", json=payload, timeout=10.0)
#             
#         if response.status_code == 200:
#             try:
#                 data = response.json()
#             except Exception:
#                 await message.answer(f"❌ Сервіс повернув некоректну відповідь: {response.text}")
#                 return
# 
#             pay_url = data.get("invoice_url") or data.get("payment_url")
#             
#             if not pay_url and "payment_id" in data:
#                 await message.answer(f"✅ Платіж створено!\nID: `{data['payment_id']}`\nСтатус: `{data['payment_status']}`\nСума: `{data['pay_amount']} {data['pay_currency']}`\nАдреса: `{data['pay_address']}`", parse_mode="Markdown")
#             elif pay_url:
#                 await message.answer(f"💳 **Посилання на оплату:**\n{pay_url}", parse_mode="Markdown")
#             else:
#                 await message.answer(f"✅ Результат: `{data}`", parse_mode="Markdown")
#         else:
#             await message.answer(f"❌ Помилка сервісу оплати ({response.status_code}):\n`{response.text}`", parse_mode="Markdown")
#             
#     except Exception as e:
#         logger.error(f"Error in cmd_pay: {e}")
#         await message.answer(f"❌ Критична помилка: {str(e)}")

@router.message(F.text == "👤 Кабінет")
async def show_cabinet(message: types.Message):
    is_active, expiry = check_subscription(message.from_user.id)
    if not is_active:
        await message.answer("⚠️ **Доступ обмежено!**\nБудь ласка, введіть пароль.", parse_mode="Markdown")
        return
    user_info = get_user_info(message.from_user.id)
    
    status_text = "✅ Активна" if is_active else "❌ Неактивна"
    expiry_date = expiry.strftime("%d.%m.%Y %H:%M") if expiry else "Немає даних"
    reg_date = user_info["registration_date"].strftime("%d.%m.%Y %H:%M") if user_info and user_info["registration_date"] else "Невідомо"
    
    await message.answer(
        messages.CABINET_MESSAGE.format(
            user_id=message.from_user.id,
            status=status_text,
            expiry=expiry_date,
            reg_date=reg_date
        ),
        parse_mode="Markdown"
    )

# @router.message(F.text == "💳 Оплатити")
# @router.message(Command("pay"))
# async def show_pay_options(message: types.Message):
#     await message.answer(messages.PAY_OPTIONS_MESSAGE, reply_markup=get_pay_keyboard(), parse_mode="Markdown")
# 
# @router.callback_query(F.data.startswith("pay_period_"))
# async def handle_pay_callback(callback: types.CallbackQuery):
#     period = int(callback.data.split("_")[-1])
#     await callback.message.edit_text(
#         f"💎 **Виберіть валюту для оплати {period} міс.:**", 
#         reply_markup=get_coins_keyboard(period),
#         parse_mode="Markdown"
#     )
#     await callback.answer()
# 
# @router.callback_query(F.data == "pay_back_to_periods")
# async def handle_back_to_periods(callback: types.CallbackQuery):
#     await callback.message.edit_text(
#         messages.PAY_OPTIONS_MESSAGE, 
#         reply_markup=get_pay_keyboard(), 
#         parse_mode="Markdown"
#     )
#     await callback.answer()
# 
# @router.callback_query(F.data.startswith("pay_coin_"))
# async def handle_coin_callback(callback: types.CallbackQuery):
#     # Format: pay_coin_{period}_{coin}
#     parts = callback.data.split("_")
#     period = int(parts[2])
#     coin = parts[3]
#     amount = period * 10.0
#     
#     await callback.message.edit_text(f"⏳ Створюю запит на оплату {period} міс. у {coin}...")
#     
#     try:
#         payload = {
#             "price_amount": amount,
#             "asset": coin,
#             "order_id": str(callback.from_user.id),
#             "order_description": f"Підписка на {period} міс. для OSINT бота ({coin})"
#         }
#         
#         async with httpx.AsyncClient() as client:
#             # Используем имя сервиса из docker-compose
#             response = await client.post("http://pay-service:8080/invoice/create", json=payload, timeout=15.0)
#             
#         if response.status_code == 200:
#             data = response.json()
#             pay_url = data.get("invoice_url") or data.get("payment_url")
#             if pay_url:
#                 keyboard = InlineKeyboardMarkup(inline_keyboard=[
#                     [InlineKeyboardButton(text="💎 Відкрити Crypto Bot", url=pay_url)]
#                 ])
#                 
#                 await callback.message.answer(
#                     f"💳 <b>Ваше посилання на оплату ({coin}):</b>\n\n" +
#                     f"<code>{pay_url}</code>\n\n" +
#                     f"<i>Після оплати подписка активується автоматично.</i>", 
#                     parse_mode="HTML",
#                     reply_markup=keyboard
#                 )
#             else:
#                 await callback.message.answer("✅ Платіж створено! Відкрийте Crypto Bot для оплати.")
#         else:
#             await callback.message.edit_text(f"❌ Помилка сервісу оплати ({response.status_code}): {response.text}")
#             
#     except Exception as e:
#         logger.error(f"Error in handle_coin_callback: {e}")
#         await callback.message.edit_text(f"❌ Помилка: {str(e)}")
#     
#     await callback.answer()


@router.message(F.text == "🔍 Пошук")
async def search_hint(message: types.Message):
    is_active, _ = check_subscription(message.from_user.id)
    if not is_active:
        await message.answer("⚠️ **Доступ обмежено!**\nБудь ласка, спочатку введіть пароль для доступу.", parse_mode="Markdown")
        return
    await message.answer("Просто введіть текст для пошуку (ПІБ, телефон, email тощо).")

@router.message(F.text, ~Command(re.compile(r".*")))
async def handle_search(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    
    # ПЕРЕВІРКА АВТОРИЗАЦІЇ
    is_active, expiry = check_subscription(message.from_user.id)
    if not is_active:
        if message.text.strip() == AUTH_PASSWORD:
            authorize_user(message.from_user.id)
            await message.answer("✅ **Авторизація успішна!**\nТепер ви можете користуватися пошуком.", parse_mode="Markdown")
            return
        
        await message.answer("⚠️ **Доступ обмежено!**\nБудь ласка, введіть пароль для доступу до бота.", parse_mode="Markdown")
        return

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
