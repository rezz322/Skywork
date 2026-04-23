import os
import sys
import re
import asyncio
import logging
import json
import redis.asyncio as redis
import httpx
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import FSInputFile
from dotenv import load_dotenv
from jinja2 import Template

import messages
import psycopg2
from psycopg2 import pool
from search_service import search_across_tables, service as search_service

# Загружаем .env с абсолютным путем (на уровень выше от папки bot)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
logging.info(f"Loading .env from: {env_path}")
load_dotenv(dotenv_path=env_path, override=True)

# Настройки модуля
logging.basicConfig(level=logging.INFO)
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL", "http://localhost:8000")

if not TOKEN:
    logging.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
    sys.exit(1)

bot = Bot(token=TOKEN)
dp = Dispatcher()
r = redis.from_url(REDIS_URL, decode_responses=True)

# PostgreSQL Configuration
PG_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "1234567890")
PG_DB = os.getenv("POSTGRES_DB", "user_bots")

try:
    pg_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB
    )
    logging.info("PostgreSQL connection pool created")
except Exception as e:
    logging.error(f"Error creating PostgreSQL connection pool: {e}")
    pg_pool = None

def register_user(user_id, username, first_name):
    if not pg_pool:
        return
    
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            # Create table if not exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id BIGINT PRIMARY KEY
                )
            """)
            
            # Ensure user_id is BIGINT (handle transformation from UUID if needed)
            cur.execute("""
                DO $$ 
                DECLARE 
                    pk_name text;
                BEGIN 
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND column_name = 'user_id' 
                        AND data_type = 'uuid'
                    ) THEN
                        -- Find primary key constraint name
                        SELECT conname INTO pk_name 
                        FROM pg_constraint 
                        WHERE conrelid = 'subscriptions'::regclass AND contype = 'p';

                        -- Drop old primary key constraint if it exists
                        IF pk_name IS NOT NULL THEN
                            EXECUTE 'ALTER TABLE subscriptions DROP CONSTRAINT ' || pk_name;
                        END IF;

                        -- Rename existing UUID column and make it nullable
                        ALTER TABLE subscriptions RENAME COLUMN user_id TO user_id_uuid_old;
                        ALTER TABLE subscriptions ALTER COLUMN user_id_uuid_old DROP NOT NULL;
                        
                        -- Add new BIGINT user_id as PRIMARY KEY
                        ALTER TABLE subscriptions ADD COLUMN user_id BIGINT PRIMARY KEY;
                    ELSIF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND column_name = 'user_id'
                    ) THEN
                        ALTER TABLE subscriptions ADD COLUMN user_id BIGINT PRIMARY KEY;
                    END IF;

                    -- Universal fixer: Drop NOT NULL from all other columns that don't have defaults
                    FOR pk_name IN (
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND is_nullable = 'NO' 
                        AND column_default IS NULL 
                        AND column_name != 'user_id'
                    ) LOOP
                        EXECUTE 'ALTER TABLE subscriptions ALTER COLUMN ' || quote_ident(pk_name) || ' DROP NOT NULL';
                    END LOOP;
                END $$;
            """)
            
            # Ensure other columns exist (robust check)
            for col, col_type in [("username", "TEXT"), ("first_name", "TEXT"), ("registration_date", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
                cur.execute(f"""
                    DO $$ 
                    BEGIN 
                        BEGIN
                            ALTER TABLE subscriptions ADD COLUMN {col} {col_type};
                        EXCEPTION
                            WHEN duplicate_column THEN NULL;
                        END;
                    END $$;
                """)
            
            cur.execute("""
                INSERT INTO subscriptions (user_id, username, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name
            """, (user_id, username, first_name))
            conn.commit()
    except Exception as e:
        logging.error(f"Error registering user {user_id}: {e}")
    finally:
        if conn:
            pg_pool.putconn(conn)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await message.answer(messages.START_MESSAGE)

@dp.message(Command("stats", "статс"))
async def cmd_stats(message: types.Message):
    try:
        # 1. Загальна кількість
        success, res_data = await search_service.execute_raw_sql("SELECT count() FROM global_search_n")
        if not success or not res_data:
            return
        
        total_count = res_data[0][0]
        
        # 2. Розбивка по джерелах
        success, sources_data = await search_service.execute_raw_sql(
            "SELECT source_table, count() as cnt FROM global_search_n GROUP BY source_table ORDER BY cnt DESC LIMIT 15"
        )
        
        stats_msg = f"📊 **Статистика бази даних**\n\n"
        stats_msg += f"📈 **Всього записів:** `{total_count:,}`\n\n".replace(",", " ")
        stats_msg += f"🗂 **Топ-15 джерел:**\n"
        
        if success and isinstance(sources_data, list):
            for row in sources_data:
                source, count = row
                stats_msg += f"• {source}: `{count:,}`\n".replace(",", " ")
        
        await message.answer(stats_msg, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error in cmd_stats: {e}")

@dp.message(F.text)
async def handle_search(message: types.Message):
    register_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    raw_text = message.text.strip()
    logging.info(f"===> Поиск от {message.from_user.id}: '{raw_text}'")
    
    # Обработка префиксов
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

    # Интеллектуальное разделение или объединение запросов
    # 1. Сначала пробуем определить типы всех потенциальных частей
    pre_parts = [p.strip() for p in re.split(r'[,;]', query_content) if p.strip()]
    
    if len(pre_parts) > 1:
        # Проверяем типы каждой части
        types_detected = [search_service.detect_search_field(p) for p in pre_parts]
        
        # Если первая часть ФИО, а последующие — атрибуты (дата, телефон, ИНН и т.д.),
        # то считаем это одним комбинированным запросом для повышения точности.
        if types_detected[0] == "fio" and any(t in ["birth_date", "phone", "passport", "inn", "snils"] for t in types_detected[1:]):
            query_parts = [query_content.replace(",", " ")] # Убираем запятую для search_service
        else:
            query_parts = pre_parts
    else:
        query_parts = pre_parts
    if not query_parts:
        if manual_field: await message.answer("⚠️ Пустой запрос.")
        return

    # Отображаем прогресс
    search_label = raw_text if len(query_parts) == 1 else f"{len(query_parts)} запитів"
    msg = await message.answer(messages.SEARCH_START.format(query=search_label), parse_mode="Markdown")

    combined_results_matrix = []
    # Группировка результатов по источникам для всех частей запроса
    master_grouped = {}

    for part in query_parts:
        results_matrix = await search_across_tables(part, manual_field=manual_field)
        for table_data in results_matrix:
            if len(table_data) > 1:
                source = table_data[0]
                if source not in master_grouped: master_grouped[source] = []
                # Дедупликация строк при объединении
                seen_hashes = {hash(frozenset(r.items())) for r in master_grouped[source]}
                for row in table_data[1:]:
                    row_hash = hash(frozenset(row.items()))
                    if row_hash not in seen_hashes:
                        master_grouped[source].append(row)
                        seen_hashes.add(row_hash)

    # Превращаем сгруппированные данные обратно в матрицу
    for source, source_rows in master_grouped.items():
        combined_results_matrix.append([source] + source_rows)
    
    local_results_count = sum(len(rows) for rows in master_grouped.values())
    
    if local_results_count > 0:
        await msg.edit_text(messages.SEARCH_SUCCESS.format(count=local_results_count))
        
        html_content = generate_html_report(raw_text, combined_results_matrix)
        filename = f"report_{message.chat.id}_{int(asyncio.get_event_loop().time())}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        await bot.send_document(message.chat.id, FSInputFile(filename), caption=messages.REPORT_CAPTION.format(query=raw_text), parse_mode="Markdown")
        if os.path.exists(filename):
            os.remove(filename)
    else:
        await msg.edit_text(messages.SEARCH_NOT_FOUND)

def generate_html_report(query, results):
    from datetime import datetime
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    cleaned_results = []
    has_letters = re.compile(r'[a-zA-Zа-яА-ЯёЁ]')

    for table_data in results:
        if len(table_data) <= 1:
            continue
            
        source_name = str(table_data[0]).lower()
        is_restricted_source = "відомості про фізичних осіб" in source_name

        new_table_data = [table_data[0]] 
        for row in table_data[1:]:
            new_row = {}
            
            # --- Обработка raw_data (расширение в обычные колонки) ---
            raw_val = row.get('raw_data', '')
            if raw_val and str(raw_val).strip() not in ["", "{}", "[]", "None", "null"]:
                try:
                    import json
                    # Пытаемся распарсить JSON
                    data = json.loads(str(raw_val))
                    if isinstance(data, dict):
                        # Рекурсивно раскрываем вложенные JSON если они в строках
                        for rk, rv in data.items():
                            val = rv
                            if isinstance(rv, str) and (rv.strip().startswith('{') or rv.strip().startswith('[')):
                                try: val = json.loads(rv)
                                except: pass
                            
                            # Переименовываем технические колонки в дружелюбные
                            display_key = rk
                            if rk == 'SQL_COL_13': display_key = "Доп. інформація"
                            elif rk.startswith('SQL_COL_'): display_key = f"Инфо ({rk.split('_')[-1]})"
                            
                            if isinstance(val, dict):
                                for ik, iv in val.items():
                                    if iv: new_row[f"{display_key}: {ik}"] = iv
                            else:
                                if val: new_row[display_key] = val
                    else:
                        new_row["Доп. інформація"] = raw_val
                except:
                    new_row["Доп. інформація"] = raw_val

            for k, v in row.items():
                if not v or k == 'raw_data': continue
                val_str = str(v).strip()
                
                # Скрываем стандартные пустые значения
                if val_str.lower() in ["", "none", "null", "nan", "undefined"]:
                    continue
                
                # Условие пользователя: скрывать для специфического источника
                if is_restricted_source and k.lower() in ['address', 'birth_date', 'адрес', 'дата народження']:
                    continue

                # Валидация специфических полей
                if k == 'birth_date' and has_letters.search(val_str):
                    continue
                
                # Аккуратное отображение нескольких номеров телефонов
                if k.lower() in ['phone', 'mobile', 'telephone', 'телефон', 'номер']:
                    if ',' in val_str or ';' in val_str:
                        parts = re.split(r'[,;]', val_str)
                        val_str = "<br>".join(p.strip() for p in parts if p.strip())
                
                if k in ['inn', 'snils', 'phone', 'tg_id']:
                    # Если есть <br>, чистим каждую часть для валидации цифр
                    parts_to_check = val_str.split("<br>")
                    valid_parts = []
                    for pt in parts_to_check:
                        clean_pt = re.sub(r'[\s\-\.\(\)\+]', '', pt)
                        if clean_pt.isdigit(): valid_parts.append(pt)
                    if not valid_parts: continue
                    val_str = "<br>".join(valid_parts)
                
                new_row[k] = val_str
            
            if new_row:
                new_table_data.append(new_row)
        
        if len(new_table_data) > 1:
            cleaned_results.append(new_table_data)

    total_records = sum(len(table_data) - 1 for table_data in cleaned_results)
    template = Template(messages.HTML_REPORT_TEMPLATE)
    return template.render(query=query, results=cleaned_results, total_records=total_records, current_time=current_time_str)

async def listen_for_completions():
    logging.info("Background listener started...")
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
                html_content = generate_html_report(query, results)
                filename = f"report_{chat_id}_{int(asyncio.get_event_loop().time())}.html"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(html_content)
                await bot.send_document(chat_id, FSInputFile(filename), caption=messages.REPORT_CAPTION.format(query=query), parse_mode="Markdown")
                if os.path.exists(filename): os.remove(filename)
        except Exception as e:
            logging.error(f"Error in listener: {e!r}")
            await asyncio.sleep(1)

async def main():
    logging.info("Starting bot...")
    asyncio.create_task(listen_for_completions())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
