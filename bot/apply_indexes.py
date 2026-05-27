#!/usr/bin/env python3
"""
Скрипт применения skip-индексов к ClickHouse.
Запуск: python apply_indexes.py
(должен быть запущен с доступом к CH, или через: docker exec osint_clickhouse python /bot/apply_indexes.py)
"""
import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

client = clickhouse_connect.get_client(
    host=os.getenv("CH_HOST", "localhost"),
    port=int(os.getenv("CH_PORT", 8123)),
    username=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", ""),
    database=os.getenv("CH_DATABASE", "default"),
)

TABLES = ["global_search_ua", "global_search_ru"]

# Типы индексов:
# bloom_filter  - для точного поиска (=), поля: phone, inn, snils, tg_id, nickname
# tokenbf_v1   - для hasToken(), поля: fio (поиск по словам ФИО)
# ngrambf_v1   - для LIKE '%...%', поля: email, passport, transport, raw_data
INDICES = [
    # (name, column, type, granularity)
    # --- Точный поиск (bloom_filter) ---
    ("idx_phone",    "phone",    "bloom_filter(0.01)",          4),
    ("idx_inn",      "inn",      "bloom_filter(0.01)",          4),
    ("idx_snils",    "snils",    "bloom_filter(0.01)",          4),
    ("idx_nickname", "nickname", "bloom_filter(0.01)",          4),
    ("idx_tg_id",    "tg_id",   "minmax",                      4),

    # --- Поиск по токенам/словам (tokenbf_v1) ---
    ("idx_fio",      "fio",      "tokenbf_v1(32768, 3, 0)",    4),

    # --- Поиск по вхождению LIKE '%...%' (ngrambf_v1) ---
    # ngrambf_v1(n, size, hashes, seed): n=3 (trigrams), size=65536
    ("idx_email",    "email",    "ngrambf_v1(3, 65536, 2, 0)", 4),
    ("idx_passport", "passport", "ngrambf_v1(3, 65536, 2, 0)", 4),
    ("idx_transport","transport","ngrambf_v1(3, 65536, 2, 0)", 4),
]

for table in TABLES:
    print(f"\n=== {table} ===")
    for idx_name, col, idx_type, gran in INDICES:
        try:
            sql = f"ALTER TABLE {table} ADD INDEX IF NOT EXISTS {idx_name} {col} TYPE {idx_type} GRANULARITY {gran}"
            client.command(sql)
            print(f"  ✅ ADD INDEX {idx_name} ({idx_type})")
        except Exception as e:
            print(f"  ⚠️  ADD INDEX {idx_name}: {e}")

        try:
            sql = f"ALTER TABLE {table} MATERIALIZE INDEX {idx_name}"
            client.command(sql)
            print(f"  ✅ MATERIALIZE {idx_name}")
        except Exception as e:
            print(f"  ⚠️  MATERIALIZE {idx_name}: {e}")

print("\nDone!")
