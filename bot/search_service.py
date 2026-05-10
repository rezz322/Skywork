import os
import re
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
    Класс для высоконагруженного поиска в ClickHouse (30+ RPS).
    Использует нативный AsyncClient и семафоры для управления конкурентностью.
    """
    
    def __init__(self):
        self._client = None
        # Позволяем до 50 одновременных запросов к БД
        self.semaphore = asyncio.Semaphore(50)
        self.table = "global_search_ua"
        
        # Настройки подключения
        self.config = {
            'host': os.getenv("CH_HOST"),
            'port': int(os.getenv("CH_PORT", 8123)),
            'username': os.getenv("CH_USER"),
            'password': os.getenv("CH_PASSWORD"),
            'database': os.getenv("CH_DATABASE")
        }

    async def get_client(self):
        """Ленивая инициализация асинхронного клиента."""
        if self._client is None:
            logger.info(f"Initializing Async ClickHouse client at {self.config['host']}")
            self._client = await clickhouse_connect.get_async_client(**self.config)
        return self._client

    def detect_search_field(self, query: str) -> str:
        """Определяет тип данных (Smart Dispatcher) для РФ и Украины."""
        query = query.strip()
        
        # Предварительная очистка для проверки числовых полей
        q_clean = re.sub(r'[\s\-\.\(\)\+\/]', '', query)
        is_numeric = q_clean.isdigit()
        
        patterns = {
            "email": r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',
            "birth_date": r'^\d{2}[\.\-]\d{2}[\.\-]\d{4}$|^\d{4}-\d{2}-\d{2}$',
            "nickname": r'^@[a-zA-Z0-9_]{3,32}$',
            "passport_block": r'\d{4}\s\d{6}' # Серия и номер паспорта РФ
        }

        if re.match(patterns["email"], query):
            return "email"
        if re.match(patterns["birth_date"], query):
            return "birth_date"
        if re.match(patterns["nickname"], query):
            return "nickname"
            
        # Проверка на наличие блока паспорта в строке (даже если есть другой текст)
        if re.search(patterns["passport_block"], query):
            return "passport"
            
        # Если начинается с +, это точно телефон
        if query.startswith('+') and re.sub(r'[^\d]', '', query).isdigit():
            return "phone"

        # Предварительная очистка для проверки числовых полей
        q_clean = re.sub(r'[^\d]', '', query)
        length = len(q_clean)
        
        if 10 <= length <= 13:
            # Если в строке в основном цифры (или есть буквы, но длина подходит под телефон)
            # и это не похоже на другие типы, считаем телефоном
            if length == 11 and q_clean.startswith(('7', '8')):
                return "phone"
            if length == 12 and q_clean.startswith('380'):
                return "phone"
            if q_clean.startswith(('7', '8', '380', '9')):
                return "phone"

        if is_numeric:
            # СНИЛС (РФ)
            if length == 11:
                return "snils"
            
            # ИНН / Паспорт
            if length == 10:
                return "inn"  # Юрлица РФ или РНОКПП Украины
            
            if length == 12:
                return "inn"  # Физлица РФ
                
            if 5 <= length <= 15:
                return "tg_id"

        if q_clean.isdigit() and length > 15:
            return "defect"

        if len(query) == 17 and re.match(r'^[A-HJ-NPR-Z0-9]{17}$', query.upper()):
            return "vin"
        if query.startswith('@'):
            return "nickname"
        if len(query.split()) >= 2:
            return "fio"
        return "fio"


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
            # Для паспорта извлекаем только первую комбинацию из 10 цифр (серия + номер)
            # Это важно, если в строке есть дата выдачи или код подразделения
            match = re.search(r'\d{10}', re.sub(r'\s', '', query))
            if match:
                return match.group(0)
            return re.sub(r'[^\d]', '', query)
        if field == "fio":
            return query.lower()
        if field == "birth_date":
            # Нормализуем разделители даты к точкам (если нужно для базы)
            return query.replace('-', '.')
        return query

    async def search(self, raw_query: str, manual_field: str = None, pivot_level: int = 0, table: str = None):
        """Асинхронний пошук з автоматичним розпізнаванням кількох критеріїв одночасно."""
        target_table = table or self.table
        query = raw_query.strip()
        clean_query = query
        if not query: return []
        
        # 1. Витягуємо всі можливі ідентифікатори з запиту
        criteria = {
            'phones': re.findall(r'\+?[\d\s\-\(\)]{10,15}', query),
            'emails': re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', query),
            'dates': re.findall(r'\b(?:\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})\b', query),
            'years': re.findall(r'\b(?:19\d{2}|20[012]\d)\b', query),
            'inns': re.findall(r'\b\d{10}\b|\b\d{12}\b', query),
            'passports': re.findall(r'\b\d{4}\s?\d{6}\b|\b\d{10}\b', query),
            'vins': re.findall(r'\b[A-HJ-NPR-Z0-9]{17}\b', query.upper()),
            'snils': re.findall(r'\b\d{3}-\d{3}-\d{3}\s\d{2}\b|\b\d{11}\b', query),
            'nicknames': re.findall(r'@[a-zA-Z0-9_]{3,32}', query)
        }

        # Спеціальна обробка телефонів: залишаємо лише ті, що реально схожі на номери (10-12 цифр)
        valid_phones = []
        for p in criteria['phones']:
            digits = re.sub(r'[^\d]', '', p)
            if 10 <= len(digits) <= 12:
                valid_phones.append(digits)
        criteria['phones'] = list(set(valid_phones))


        # Очищаємо запит від знайдених ідентифікаторів, щоб залишити тільки ПІБ
        clean_fio = query
        for key in criteria:
            for val in criteria[key]:
                clean_fio = clean_fio.replace(val, "")
        
        fio_tokens = [re.sub(r'[^\w]', '', t.lower()) for t in clean_fio.split() if len(re.sub(r'[^\w]', '', t)) > 2]

        # 2. Будуємо SQL умову
        conditions = []
        params = {}

        if fio_tokens:
            for i, token in enumerate(fio_tokens):
                conditions.append(f"hasTokenCaseInsensitive(fio, %(t{i})s)")
                params[f"t{i}"] = token

        if criteria['phones']:
            phone_conds = []
            for i, p in enumerate(criteria['phones']):
                core = p[-10:]
                params[f"p{i}"] = f"%{core}%"
                phone_conds.append(f"phone LIKE %(p{i})s")
            conditions.append(f"({' OR '.join(phone_conds)})")

        if criteria['emails']:
            email_conds = [f"email ILIKE %(e{i})s" for i in range(len(criteria['emails']))]
            conditions.append(f"({' OR '.join(email_conds)})")
            for i, e in enumerate(criteria['emails']): params[f"e{i}"] = e

        if criteria['dates']:
            date_conds = [f"birth_date = %(d{i})s" for i in range(len(criteria['dates']))]
            conditions.append(f"({' OR '.join(date_conds)})")
            for i, d in enumerate(criteria['dates']): params[f"d{i}"] = d
        elif criteria['years']:
            year_conds = [f"birth_date ILIKE %(y{i})s" for i in range(len(criteria['years']))]
            conditions.append(f"({' OR '.join(year_conds)})")
            for i, y in enumerate(criteria['years']): params[f"y{i}"] = f"%{y}%"

        if criteria['inns']:
            inn_conds = [f"inn = %(inn{i})s" for i in range(len(criteria['inns']))]
            conditions.append(f"({' OR '.join(inn_conds)})")
            for i, inn in enumerate(criteria['inns']): params[f"inn{i}"] = inn

        if criteria['vins']:
            vin_conds = [f"(transport ILIKE %(v{i})s OR raw_data ILIKE %(v{i})s)" for i in range(len(criteria['vins']))]
            conditions.append(f"({' OR '.join(vin_conds)})")
            for i, v in enumerate(criteria['vins']): params[f"v{i}"] = f"%{v}%"

        if criteria['passports']:
            pass_conds = [f"passport LIKE %(pass{i})s" for i in range(len(criteria['passports']))]
            conditions.append(f"({' OR '.join(pass_conds)})")
            for i, p in enumerate(criteria['passports']): 
                params[f"pass{i}"] = f"%{re.sub(r'[^\d]', '', p)}%"

        if criteria['snils']:
            snils_conds = [f"snils = %(sn{i})s" for i in range(len(criteria['snils']))]
            conditions.append(f"({' OR '.join(snils_conds)})")
            for i, s in enumerate(criteria['snils']): 
                params[f"sn{i}"] = re.sub(r'[^\d]', '', s)

        if criteria['nicknames']:
            nick_conds = [f"nickname ILIKE %(nick{i})s" for i in range(len(criteria['nicknames']))]
            conditions.append(f"({' OR '.join(nick_conds)})")
            for i, n in enumerate(criteria['nicknames']): 
                params[f"nick{i}"] = f"%{n.replace('@', '')}%"

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
            logger.error(f"Invalid table name: {target_table}")
            return []

        fields = "lowerUTF8(fio) AS fio, phone, email, inn, snils, driver_license, lowerUTF8(address) AS address, nickname, transport, birth_date, source_table, passport, password, raw_data, tg_id"
        sql = f"SELECT {fields} FROM {target_table} WHERE {' AND '.join(conditions)} LIMIT 1000"
        client = await self.get_client()
        logger.info(f"Executing Multi-Search [level {pivot_level}]: {query}")

        try:
            async with self.semaphore:
                res = await client.query(sql, parameters=params)
                rows = list(res.named_results())
            
            if not rows:
                return []

            # --- Оптимизированный Механизм Smart Pivot ---
            if pivot_level == 0:
                pivots_candidate = set()
                pivot_keys = {'phone', 'mobile', 'telephone', 'телефон', 'номер', 'email', 'inn', 'іпн', 'snils', 'снілс', 'tg_id', 'id', 'passport', 'vin', 'transport'}
                
                # Сканируем только первые 200 строк для ускорения
                for row in rows[:200]:
                    for k, v in row.items():
                        if not v: continue
                        k_lower = k.lower()
                        if any(pk in k_lower for pk in pivot_keys):
                            val_str = str(v).strip()
                            if any(pk in k_lower for pk in ['phone', 'mobile', 'telephone', 'телефон', 'номер']):
                                phones = re.findall(r'\d{10,13}', re.sub(r'[\s\-\.\(\)\+]', '', val_str))
                                for p in phones: pivots_candidate.add((p, "phone"))
                            elif 'email' in k_lower:
                                emails = re.findall(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+', val_str)
                                for e in emails: pivots_candidate.add((e, "email"))
                            elif 'vin' in k_lower or 'transport' in k_lower:
                                vins = re.findall(r'[A-HJ-NPR-Z0-9]{17}', val_str.upper())
                                for v_val in vins: pivots_candidate.add((v_val, "vin"))
                            else:
                                # Для ИНН/Паспорта/СНИЛС оставляем только цифры
                                clean_id = re.sub(r'[^\d]', '', val_str)
                                if clean_id:
                                    detected = self.detect_search_field(clean_id)
                                    if detected not in ["fio", "nickname", "address", "tg_id"]:
                                        pivots_candidate.add((clean_id, detected))


                # Фильтруем текущий запрос и ограничиваем количество рекурсий
                final_pivots = [(q, f) for q, f in pivots_candidate if q != clean_query]
                
                # Приоритизация: Email и ИНН/Паспорт идем в начале
                priority = {"email": 0, "passport": 1, "inn": 2, "snils": 3, "phone": 4}
                final_pivots.sort(key=lambda x: priority.get(x[1], 10))
                
                # Запускаем до 5 параллельных поисков
                pivots_limit = final_pivots[:5]
                if pivots_limit:
                    tasks = [self.search(q, manual_field=f, pivot_level=1, table=target_table) for q, f in pivots_limit]
                    pivot_results_all = await asyncio.gather(*tasks)
                    
                    for pivot_results in pivot_results_all:
                        for pivot_table_data in pivot_results:
                            if len(pivot_table_data) > 1:
                                rows.extend(pivot_table_data[1:])
            
            # --- Группировка результатов ---
            grouped = {}
            for row in rows:
                source = row.get('source_table', 'unknown_source')
                if source not in grouped: grouped[source] = []
                grouped[source].append(row)
            
            results_matrix = []
            for source, source_rows in grouped.items():
                results_matrix.append([source] + source_rows)
            return results_matrix
            
        except Exception as e:
            logger.error(f"Async search error: {e}")
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

async def search_across_tables(query: str, manual_field: str = None, table: str = None):
    return await service.search(query, manual_field, table=table)
