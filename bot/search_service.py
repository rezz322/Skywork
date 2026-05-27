import os
import re
import time
import json
import hashlib
import logging
import asyncio
import clickhouse_connect
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("search_service")

if not os.getenv("DOCKER_ENV"):
    load_dotenv()

class ClickHouseSearchService:
    """
    Клас для високонавантаженого пошуку в ClickHouse (30+ RPS).
    Використовує нативний AsyncClient та семафори для управління конкурентністю.
    """
    
    def __init__(self):
        self._client = None
        # Позволяем до 50 одновременных запросов к БД
        self.semaphore = asyncio.Semaphore(50)
        self.table = "global_search_ua"
        # Кеш результатів: {cache_key: (timestamp, result)}
        self._cache: dict = {}
        self._cache_ttl = 120  # секунд
        
        # Настройки подключения
        self.config = {
            'host': os.getenv("CH_HOST"),
            'port': int(os.getenv("CH_PORT", 8123)),
            'username': os.getenv("CH_USER"),
            'password': os.getenv("CH_PASSWORD"),
            'database': os.getenv("CH_DATABASE")
        }

    async def get_client(self):
        """Лінива ініціалізація асинхронного клієнта."""
        if self._client is None:
            logger.info(f"Initializing Async ClickHouse client at {self.config['host']}")
            self._client = await clickhouse_connect.get_async_client(**self.config)
        return self._client

    def _cache_get(self, key: str):
        """Повертає кешований результат або None."""
        entry = self._cache.get(key)
        if entry and (time.time() - entry[0]) < self._cache_ttl:
            return entry[1]
        if key in self._cache:
            del self._cache[key]
        return None

    def _cache_set(self, key: str, value):
        """Зберігає результат у кеш, обмежуємо розмір до 500 записів."""
        if len(self._cache) >= 500:
            # Видаляємо найстаріший
            oldest = min(self._cache, key=lambda k: self._cache[k][0])
            del self._cache[oldest]
        self._cache[key] = (time.time(), value)

    def detect_search_field(self, query: str) -> str:
        """Определяет тип данных (Smart Dispatcher) для РФ и Украины."""
        query = query.strip()

        # Email
        if re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', query):
            return "email"

        # Дата рождения
        if re.match(r'^\d{2}[\.\-]\d{2}[\.\-]\d{4}$|^\d{4}-\d{2}-\d{2}$', query):
            return "birth_date"

        # Ник
        if re.match(r'^@[a-zA-Z0-9_]{3,32}$', query):
            return "nickname"

        # Паспорт РФ: 4 цифры пробел 6 цифр
        if re.search(r'\d{4}\s\d{6}', query):
            return "passport"

        # Если начинается с +, это точно телефон
        if query.startswith('+') and re.sub(r'[^\d]', '', query).isdigit():
            return "phone"

        # Только цифры (после удаления разделителей)
        q_clean = re.sub(r'[^\d]', '', query)
        length = len(q_clean)
        is_only_digits = re.sub(r'[\s\-\.\(\)\+\/]', '', query).isdigit()

        if is_only_digits:
            # ИНН физлица РФ (12 цифр) — проверяем ДО телефона
            if length == 12:
                return "inn"

            # СНИЛС РФ (11 цифр НЕ начинается с 7 или 8)
            if length == 11 and not q_clean.startswith(('7', '8')):
                return "snils"

            # Телефон РФ (11 цифр, начинается с 7 или 8)
            if length == 11 and q_clean.startswith(('7', '8')):
                return "phone"

            # Телефон Украины (12 цифр, начинается с 380)
            if length == 12 and q_clean.startswith('380'):
                return "phone"

            # ИНН юрлица РФ / РНОКПП Украины (10 цифр)
            if length == 10:
                return "inn"

            # Телефон (10-13 цифр, начинается с 9/38/7/8)
            if 10 <= length <= 13 and q_clean.startswith(('7', '8', '9', '38')):
                return "phone"

            # Telegram ID (5-15 цифр)
            if 5 <= length <= 15:
                return "tg_id"

            if length > 15:
                return "defect"

        # VIN
        if len(query) == 17 and re.match(r'^[A-HJ-NPR-Z0-9]{17}$', query.upper()):
            return "vin"

        # Ник без @
        if query.startswith('@'):
            return "nickname"

        # ФИО (2+ слова)
        if len(query.split()) >= 2:
            return "fio"

        return "fio"


    def phone_variants(self, raw: str) -> list:
        """
        Генерує всі можливі формати зберігання номера для пошуку через IN().
        Це дає можливість використовувати примарний індекс ORDER BY на повну швидкість.
        """
        d = re.sub(r'[^\d]', '', raw)
        variants = set()
        # Беремо останні 10 цифр як базу
        core = d[-10:] if len(d) >= 10 else d
        if len(core) == 10:
            variants.update([
                core,               # 0XXXXXXXXX або XXXXXXXXXX
                '7' + core,         # 7XXXXXXXXXX (РФ)
                '8' + core,         # 8XXXXXXXXXX (РФ старий)
                '+7' + core,        # +7XXXXXXXXXX
                '380' + core[1:] if core.startswith('0') else '',  # 380XXXXXXXXX (УА)
                '0' + core[1:] if not core.startswith('0') else core,  # 0XXXXXXXXX (УА локал)
                '380' + core,       # 38010 цифр
            ])
        elif len(d) == 11 and d.startswith(('7', '8')):
            core10 = d[1:]
            variants.update([
                d,          # 79001234567
                '7' + core10,
                '8' + core10,
                '+7' + core10,
                core10,
            ])
        elif len(d) == 12 and d.startswith('380'):
            core10 = d[2:]  # 0XXXXXXXXX
            variants.update([
                d,           # 380XXXXXXXXX
                '+' + d,     # +380XXXXXXXXX
                core10,      # 0XXXXXXXXX
                core10[1:],  # XXXXXXXXX (9 digits)
            ])
        # Додаємо сам оригінальний
        variants.add(d)
        # Чистимо порожняки
        return [v for v in variants if v and len(v) >= 7]

    def format_query(self, query: str, field: str) -> str:
        """Форматирование запроса."""
        query = query.strip()
        if field in ["phone", "inn", "snils", "tg_id"]:
            # Удаляем всё кроме цифр
            res = re.sub(r'[^\d]', '', query)
            if field == "phone" and len(res) == 11 and res.startswith('8'):
                res = '7' + res[1:]
            return res
        if field == "passport":
            match = re.search(r'\d{10}', re.sub(r'\s', '', query))
            if match:
                return match.group(0)
            return re.sub(r'[^\d]', '', query)
        if field == "fio":
            return query.lower()
        if field == "birth_date":
            return query.replace('-', '.')
        return query

    async def search(self, raw_query: str, manual_field: str = None, pivot_level: int = 0, table: str = None, return_field: bool = False):
        """Асинхронний пошук з автоматичним розпізнаванням кількох критеріїв одночасно."""
        target_table = table or self.table
        query = raw_query.strip()
        clean_query = query
        if not query: return []
        
        # Визначаємо поле автоматично (для звіту користувачу)
        auto_field = manual_field or self.detect_search_field(query)

        # 1. Витягуємо всі можливі ідентифікатори з запиту
        criteria = {
            'phones': re.findall(r'(?:(?:\+?38|\+?7|8)?[\s\-\(\)]*)?(?:\d[\s\-\(\)]*){10,11}', query),
            'emails': re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', query),
            'dates': re.findall(r'\b(?:\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})\b', query),
            'years': re.findall(r'\b(?:19\d{2}|20[012]\d)\b', query),
            # INN: строго 10 или 12 цифр (не внутри большего числа)
            'inns': re.findall(r'(?<!\d)(\d{10}|\d{12})(?!\d)', query),
            # Паспорт: 4 цифры + пробел + 6 цифр (только такой формат, без конфликта с INN)
            'passports': re.findall(r'\b\d{4}\s\d{6}\b', query),
            'vins': re.findall(r'\b[A-HJ-NPR-Z0-9]{17}\b', query.upper()),
            # СНИЛС: формат XXX-XXX-XXX XX или 11 цифр НЕ начинающихся с 7/8
            'snils': re.findall(r'\b\d{3}-\d{3}-\d{3}\s\d{2}\b', query),
            'nicknames': re.findall(r'@[a-zA-Z0-9_]{3,32}', query)
        }

        # Спеціальна обробка телефонів: залишаємо лише ті, що реально схожі на номери (10-12 цифр)
        valid_phones = []
        for p in criteria['phones']:
            digits = re.sub(r'[^\d]', '', p)
            if 10 <= len(digits) <= 12:
                valid_phones.append(digits)
        criteria['phones'] = list(set(valid_phones))

        # Якщо це чисто числовий запит — уточнюємо поля
        q_digits = re.sub(r'[^\d]', '', query)
        if re.sub(r'[\s\-\.\(\)\+\/]', '', query).isdigit() and not criteria['emails'] and not criteria['nicknames']:
            dlen = len(q_digits)
            if dlen == 12 and not q_digits.startswith('380'):
                # 12 цифр не 380... = INN фізосіб РФ (однозначно)
                criteria['inns'] = [q_digits]
                criteria['phones'] = []
                criteria['snils'] = []
                auto_field = "inn"
            elif dlen == 12 and q_digits.startswith('380'):
                # 380... = телефон України
                criteria['phones'] = [q_digits]
                criteria['inns'] = []
                auto_field = "phone"
            elif dlen == 11 and q_digits.startswith(('7', '8')):
                # 7/8... = телефон РФ
                criteria['phones'] = [q_digits]
                criteria['inns'] = []
                criteria['snils'] = []
                auto_field = "phone"
            elif dlen == 11 and not q_digits.startswith(('7', '8')):
                # 11 цифр не 7/8 = СНІЛС
                criteria['snils'] = [q_digits]
                criteria['phones'] = []
                criteria['inns'] = []
                auto_field = "snils"
            elif dlen == 10:
                # НЕОДНОЗНАЧНО: може бути INN (10 цифр) АБО телефон без коду країни
                # Шукаємо в ОБОХ полях одночасно через OR
                criteria['inns'] = [q_digits]
                criteria['phones'] = [q_digits]  # залишаємо для phone-умови
                auto_field = "inn_or_phone"


        # Очищаємо запит від знайдених ідентифікаторів, щоб залишити тільки ПІБ
        clean_fio = query
        for key in criteria:
            for val in criteria[key]:
                clean_fio = clean_fio.replace(val, "")
        
        # Обмежуємо FIO до 3 токенів (інакше на 1.4млрд рядків занадто повільно)
        fio_tokens = [re.sub(r'[^\w]', '', t.lower()) for t in clean_fio.split() if len(re.sub(r'[^\w]', '', t)) > 2]
        fio_tokens = fio_tokens[:3]

        # 2. Будуємо SQL умову
        conditions = []
        params = {}

        if fio_tokens:
            for i, token in enumerate(fio_tokens):
                conditions.append(f"hasTokenCaseInsensitive(fio, %(t{i})s)")
                params[f"t{i}"] = token

        if criteria['phones']:
            # Генеруємо всі варіанти формату і шукаємо через IN() — використовує примарний індекс ORDER BY
            all_variants = []
            for p in criteria['phones']:
                all_variants.extend(self.phone_variants(p))
            all_variants = list(set(all_variants))
            # Параметризуємо кожен варіант
            for i, v in enumerate(all_variants):
                params[f"ph{i}"] = v
            in_list = ", ".join(f"%(ph{i})s" for i in range(len(all_variants)))
            conditions.append(f"phone IN ({in_list})")

        if criteria['emails']:
            # Email: пошук по вхожденню (ngrambf_v1 індекс підтримує LIKE)
            email_conds = [f"lower(email) LIKE %(e{i})s" for i in range(len(criteria['emails']))]
            conditions.append(f"({' OR '.join(email_conds)})")
            for i, e in enumerate(criteria['emails']):
                params[f"e{i}"] = f"%{e.lower()}%"

        if criteria['dates']:
            date_conds = [f"birth_date = %(d{i})s" for i in range(len(criteria['dates']))]
            conditions.append(f"({' OR '.join(date_conds)})")
            for i, d in enumerate(criteria['dates']): params[f"d{i}"] = d
        elif criteria['years']:
            # Пошук по року — тільки якщо немає точної дати
            year_conds = [f"startsWith(birth_date, %(y{i})s)" for i in range(len(criteria['years']))]
            conditions.append(f"({' OR '.join(year_conds)})")
            for i, y in enumerate(criteria['years']): params[f"y{i}"] = y

        if criteria['inns']:
            inn_q = criteria['inns'][0]
            if auto_field == "inn_or_phone":
                # 10 цифр — шукаємо і в INN і всі варіанти phone через IN()
                phone_vars = self.phone_variants(inn_q)
                for i, v in enumerate(phone_vars):
                    params[f"pa{i}"] = v
                phone_in = ", ".join(f"%(pa{i})s" for i in range(len(phone_vars)))
                params["inn_ambig"] = inn_q
                conditions.append(
                    f"(inn = %(inn_ambig)s OR phone IN ({phone_in}))"
                )
            else:
                inn_conds = [f"inn = %(inn{i})s" for i in range(len(criteria['inns']))]
                conditions.append(f"({' OR '.join(inn_conds)})")
                for i, inn in enumerate(criteria['inns']): params[f"inn{i}"] = inn

        if criteria['vins']:
            # VIN: шукаємо у transport І raw_data (VIN може бути у доп. полях)
            vin_conds = [
                f"(transport LIKE %(v{i})s OR raw_data LIKE %(v{i})s)"
                for i in range(len(criteria['vins']))
            ]
            conditions.append(f"({' OR '.join(vin_conds)})")
            for i, v in enumerate(criteria['vins']):
                params[f"v{i}"] = f"%{v}%"

        if criteria['passports']:
            # Паспорт: пошук по вхождению (ngrambf_v1 індекс підтримує LIKE)
            pass_conds = [f"passport LIKE %(pass{i})s" for i in range(len(criteria['passports']))]
            conditions.append(f"({' OR '.join(pass_conds)})")
            for i, p in enumerate(criteria['passports']):
                clean_p = re.sub(r'[^\d]', '', p)
                params[f"pass{i}"] = f"%{clean_p}%"

        if criteria['snils']:
            snils_conds = [f"snils = %(sn{i})s" for i in range(len(criteria['snils']))]
            conditions.append(f"({' OR '.join(snils_conds)})")
            for i, s in enumerate(criteria['snils']): 
                params[f"sn{i}"] = re.sub(r'[^\d]', '', s)

        if criteria['nicknames']:
            # Никнейм: шукаємо з @ і без @, великі/малі — LIKE покриває всі варіанти
            nick_conds = []
            for i, n in enumerate(criteria['nicknames']):
                clean_n = n.lstrip('@').lower()
                params[f"nick{i}"] = f"%{clean_n}%"
                nick_conds.append(f"lower(nickname) LIKE %(nick{i})s")
            conditions.append(f"({' OR '.join(nick_conds)})")

        # Пошук по нікнейму якщо запит — одне слово без пробілів (без @)
        # Наприклад: 'ivan_petrov' або 'usernameXYZ'
        if not conditions and len(query.split()) == 1 and re.match(r'^[a-zA-Z0-9_]{3,32}$', query):
            params["nick_plain"] = f"%{query.lower()}%"
            conditions.append("lower(nickname) LIKE %(nick_plain)s")
            auto_field = "nickname"

        # Telegram ID (якщо це число від 5 до 15 знаків і не потрапило в інші категорії)
        tg_ids = [t for t in re.findall(r'\b\d{5,15}\b', query) if t not in criteria['inns'] and t not in [re.sub(r'[^\d]', '', p) for p in criteria['passports']]]
        if tg_ids:
            tg_conds = [f"tg_id = %(tg{i})s" for i in range(len(tg_ids))]
            conditions.append(f"({' OR '.join(tg_conds)})")
            for i, tid in enumerate(tg_ids): params[f"tg{i}"] = int(tid)


        if not conditions:
            tokens = [re.sub(r'[^\w]', '', t.lower()) for t in query.split() if len(re.sub(r'[^\w]', '', t)) >= 2]
            if not tokens: return []
            # Пошук за токенами у ключових текстових полях
            conditions = [f"(hasTokenCaseInsensitive(fio, %(t{i})s) OR hasTokenCaseInsensitive(address, %(t{i})s) OR hasTokenCaseInsensitive(raw_data, %(t{i})s))" for i in range(len(tokens))]
            for i, t in enumerate(tokens): params[f"t{i}"] = t

        # Захист від SQL Injection: перевіряємо назву таблиці
        if not re.match(r'^[a-zA-Z0-9_]+$', target_table):
            if return_field: return [], auto_field
            return []

        # Перевіряємо кеш
        cache_key = hashlib.md5(
            f"{target_table}:{json.dumps(conditions, sort_keys=True)}:{json.dumps(params, sort_keys=True)}".encode()
        ).hexdigest()
        cached = self._cache_get(cache_key)
        if cached is not None:
            logger.info(f"Cache HIT for query: {query[:50]}")
            if return_field: return cached, auto_field
            return cached

        # SELECT без lowerUTF8 у запиті — виконуємо lower на Python-стороні
        fields = "fio, phone, email, inn, snils, driver_license, address, nickname, transport, birth_date, source_table, passport, password, raw_data, tg_id"
        # Налаштування CH: таймаут 15с, обмеження пам'яті
        sql = (
            f"SELECT {fields} FROM {target_table} "
            f"WHERE {' AND '.join(conditions)} "
            f"LIMIT 500 "
            f"SETTINGS max_memory_usage=536870912"
        )
        logger.info(f"Executing Multi-Search [level {pivot_level}]: {query[:60]}")
        
        client = await self.get_client()
        try:
            async with self.semaphore:
                res = await client.query(sql, parameters=params)
                rows = list(res.named_results())
                # Нормалізуємо fio/address до нижнього регістру на Python-стороні
                for row in rows:
                    if row.get('fio'): row['fio'] = row['fio'].lower()
                    if row.get('address'): row['address'] = row['address'].lower()
            
            if not rows:
                result = []
                self._cache_set(cache_key, result)
                if return_field: return result, auto_field
                return result

            # --- Smart Pivot ВИМКНЕНО за замовчуванням для швидкодії ---
            # Pivot робить до 5 додаткових SQL-запитів, що сповільнює відповідь
            # Вмикається тільки якщо pivot_level передано явно як -1
            if pivot_level == -1:
                pivots_candidate = set()
                pivot_keys = {'phone', 'email', 'inn', 'snils', 'passport', 'transport'}
                for row in rows[:50]:  # Скорочено до 50 рядків
                    for k, v in row.items():
                        if not v: continue
                        k_lower = k.lower()
                        if any(pk in k_lower for pk in pivot_keys):
                            val_str = str(v).strip()
                            if 'phone' in k_lower:
                                phones = re.findall(r'\d{10,13}', re.sub(r'[\s\-\.\(\)\+]', '', val_str))
                                for p in phones: pivots_candidate.add((p, "phone"))
                            elif 'email' in k_lower:
                                emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', val_str)
                                for e in emails: pivots_candidate.add((e, "email"))
                            else:
                                clean_id = re.sub(r'[^\d]', '', val_str)
                                if clean_id:
                                    detected = self.detect_search_field(clean_id)
                                    if detected not in ["fio", "nickname", "tg_id", "defect"]:
                                        pivots_candidate.add((clean_id, detected))

                final_pivots = [(q, f) for q, f in pivots_candidate if q != clean_query][:3]
                if final_pivots:
                    tasks = [self.search(q, manual_field=f, pivot_level=1, table=target_table) for q, f in final_pivots]
                    pivot_results_all = await asyncio.gather(*tasks)
                    for pivot_results in pivot_results_all:
                        for pivot_table_data in pivot_results:
                            if len(pivot_table_data) > 1:
                                rows.extend(pivot_table_data[1:])
            
            # --- Групування результатів ---
            grouped = {}
            for row in rows:
                source = row.get('source_table', 'unknown_source')
                if source not in grouped: grouped[source] = []
                grouped[source].append(row)
            
            results_matrix = []
            for source, source_rows in grouped.items():
                results_matrix.append([source] + source_rows)

            self._cache_set(cache_key, results_matrix)
            if return_field:
                return results_matrix, auto_field
            return results_matrix

        except Exception as e:
            logger.error(f"Async search error: {e}")
            if return_field:
                return [], auto_field
            return []

    async def execute_raw_sql(self, sql_content: str):
        """Виконує довільний SQL та повертає дані для SELECT."""
        client = await self.get_client()
        try:
            sql_stripped = sql_content.strip()
            sql_lower = sql_stripped.lower()
            
            # Заборона небезпечних операцій
            forbidden = ["drop", "truncate", "delete", "alter", "update", "insert", "create"]
            if any(cmd in sql_lower for cmd in forbidden):
                return False, "Використання небезпечних SQL команд (DROP, TRUNCATE, тощо) заборонено"

            if sql_lower.startswith("select"):
                res = await client.query(sql_stripped)
                return True, list(res.result_rows)
            else:
                await client.command(sql_stripped)
                return True, "SQL виконано успішно"
        except Exception as e:
            logger.error(f"SQL Execution error: {e}")
            return False, str(e)

    async def get_all_source_counts(self):
        """Отримує кількість записів для кожної source_table у UA та RU базах."""
        client = await self.get_client()
        counts = {}
        try:
            for table in ["global_search_ua", "global_search_ru"]:
                # ClickHouse query to group by source_table
                sql = f"SELECT source_table, count(*) as cnt FROM {table} GROUP BY source_table"
                res = await client.query(sql)
                for row in res.named_results():
                    source = row['source_table'] or "unknown"
                    # Key format: "table:source_name"
                    key = f"{table}:{source}"
                    counts[key] = int(row['cnt'])
            return counts
        except Exception as e:
            logger.error(f"Error getting counts from ClickHouse: {e}")
            return {}

# Singleton сервис
service = ClickHouseSearchService()

async def search_across_tables(query: str, manual_field: str = None, table: str = None, return_field: bool = False):
    return await service.search(query, manual_field, table=table, return_field=return_field)
