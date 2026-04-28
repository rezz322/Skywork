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
        self.table = "global_search_n"
        
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
        """Асинхронный поиск с умным определением полей, поддержкой фильтров и Pivot."""
        target_table = table or self.table
        query = raw_query.strip()
        if not query:
            return []
            
        field = manual_field or self.detect_search_field(query)
        clean_query = self.format_query(query, field)
        
        # --- Защита от "пустых" или слишком коротких поисков (предотвращаем LIKE '%') ---
        if not clean_query:
            logger.warning(f"Aborting search: clean_query is empty for {raw_query}")
            return []
            
        # Минимальные куски для поиска (чтобы не искал по 1 цифре)
        if field == "phone" and len(clean_query) < 7:
            logger.warning(f"Phone query too short: {clean_query}")
            return []
        if field in ["inn", "snils", "passport", "tg_id", "email"] and len(clean_query) < 5:
            logger.warning(f"ID query too short: {clean_query}")
            return []
        if field == "fio" and len(clean_query) < 3:
            logger.warning(f"FIO query too short: {clean_query}")
            return []

        # Получаем клиент
        client = await self.get_client()

        params = {}
        date_filter = ""
        
        # 1. Если поиск по ФИО, попробуем найти дату или год в строке
        if field == "fio":
            # Ищем полную дату (ДД.ММ.ГГГГ или ГГГГ-ММ-ДД)
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2})', query)
            # Ищем год (4 цифры в диапазоне 1900-2026)
            year_match = re.search(r'\b(19\d{2}|20[012]\d)\b', query)
            
            date_filter = ""
            order_by_parts = []
            clean_query = query
            
            if date_match:
                extracted_date = date_match.group(0)
                params["extracted_date"] = extracted_date
                date_filter = "AND birth_date = %(extracted_date)s"
                order_by_parts.append("(birth_date = %(extracted_date)s) DESC")
                clean_query = clean_query.replace(extracted_date, "")
            elif year_match:
                extracted_year = year_match.group(0)
                params["extracted_year"] = f"%{extracted_year}%"
                date_filter = "AND birth_date ILIKE %(extracted_year)s"
                order_by_parts.append("(birth_date ILIKE %(extracted_year)s) DESC")
                clean_query = clean_query.replace(extracted_year, "")
            
            clean_query = clean_query.strip().lower()
            raw_tokens = [t.strip() for t in re.split(r'[,; ]', clean_query) if t.strip()]
            # Очистка токенов от лишних символов (например, Иванов, -> иванов)
            tokens = [re.sub(r'[^\w]', '', t) for t in raw_tokens if re.sub(r'[^\w]', '', t)]
            
            if not tokens: return []
            
            token_conditions = [f"hasToken(fio, %(t{i})s)" for i in range(len(tokens))]
            order_by_clause = f"ORDER BY {', '.join(order_by_parts)}, fio ASC" if order_by_parts else "ORDER BY fio ASC"
            
            sql = f"SELECT * FROM {target_table} WHERE {' AND '.join(token_conditions)} {date_filter} {order_by_clause} LIMIT 1000"
            for i, token in enumerate(tokens):
                params[f"t{i}"] = token
        
        elif field in ["address", "nickname", "transport"]:
            sql = f"SELECT * FROM {target_table} WHERE {field} ILIKE %(q)s LIMIT 1000"
            params["q"] = f"%{clean_query}%"
        elif field == "phone":
            # Используем "Умную группировку" для поиска форматированных номеров
            # Например: 9031234567 -> %903%123%4567% (найдет и со скобками, и без)
            if len(clean_query) >= 10:
                # Берем последние 10 цифр и делим на логические блоки: [3][3][4]
                core = clean_query[-10:]
                smart_pattern = f"%{core[:3]}%{core[3:6]}%{core[6:]}%"
            else:
                smart_pattern = f"%{clean_query}%"
            
            # Сортировка: Сначала точные совпадения, потом по длине
            sql = f"""
                SELECT * FROM {target_table} 
                WHERE ({field} LIKE %(q_smart)s OR {field} LIKE %(q_strict)s)
                ORDER BY ({field} = %(exact)s OR {field} = %(exact_plus)s) DESC, length({field}) ASC 
                LIMIT 1000
            """
            params["q_smart"] = smart_pattern
            params["q_strict"] = f"%{clean_query}%"
            params["exact"] = clean_query
            params["exact_plus"] = "+" + clean_query
        elif field == "defect":
            return []
        else:
            sql = f"SELECT * FROM {target_table} WHERE {field} = %(q)s LIMIT 1000"
            params["q"] = clean_query

        logger.info(f"Executing search [level {pivot_level}]: {field}='{clean_query}'")

        try:
            async with self.semaphore:
                res = await client.query(sql, parameters=params)
                rows = list(res.named_results())
            
            if not rows:
                return []

            # --- Оптимизированный Механизм Smart Pivot ---
            if pivot_level == 0:
                pivots_candidate = set()
                pivot_keys = {'phone', 'mobile', 'telephone', 'телефон', 'номер', 'email', 'inn', 'іпн', 'snils', 'снілс', 'tg_id', 'id', 'passport'}
                
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
                            else:
                                clean_id = re.sub(r'[^\d]', '', val_str) if k_lower != 'email' else val_str
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
            seen_hashes = set()
            unique_rows = []

            for row in rows:
                # Дедупликация строк
                row_hash = hash(frozenset(row.items()))
                if row_hash in seen_hashes:
                    continue
                seen_hashes.add(row_hash)
                
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
        """Выполняет произвольный SQL и возвращает данные для SELECT."""
        client = await self.get_client()
        try:
            sql_stripped = sql_content.strip()
            if sql_stripped.lower().startswith("select"):
                res = await client.query(sql_stripped)
                return True, list(res.result_rows)
            else:
                await client.command(sql_stripped)
                return True, "SQL виконано успішно"
        except Exception as e:
            logger.error(f"SQL Execution error: {e}")
            return False, str(e)

# Singleton сервис
service = ClickHouseSearchService()

async def search_across_tables(query: str, manual_field: str = None, table: str = None):
    return await service.search(query, manual_field, table=table)
