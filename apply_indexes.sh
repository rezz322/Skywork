#!/bin/bash
# Запуск: bash /root/Skywork/apply_indexes.sh
# Применяет skip-индексы к ClickHouse для ускорения поиска

CH_CONTAINER="osint_clickhouse"
CH_USER="default"
# Укажи пароль если есть, или оставь пустым
CH_PASS=""

run_sql() {
    if [ -z "$CH_PASS" ]; then
        docker exec $CH_CONTAINER clickhouse-client --query "$1"
    else
        docker exec $CH_CONTAINER clickhouse-client --password "$CH_PASS" --query "$1"
    fi
}

echo "=== Применяем индексы к ClickHouse ==="

for TABLE in global_search_ua global_search_ru; do
    echo ""
    echo "--- Таблица: $TABLE ---"

    # tokenbf_v1 для ФИО (hasToken)
    echo "  [1/7] idx_fio (tokenbf_v1)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_fio fio TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_fio"

    # bloom_filter для точных числовых полей
    echo "  [2/7] idx_phone (bloom_filter)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_phone phone TYPE bloom_filter(0.01) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_phone"

    echo "  [3/7] idx_inn (bloom_filter)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_inn inn TYPE bloom_filter(0.01) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_inn"

    echo "  [4/7] idx_snils (bloom_filter)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_snils snils TYPE bloom_filter(0.01) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_snils"

    echo "  [5/7] idx_tg_id (minmax)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_tg_id tg_id TYPE minmax GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_tg_id"

    # ngrambf_v1 для LIKE-поиска
    echo "  [6/7] idx_email (ngrambf_v1)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_email email TYPE ngrambf_v1(3, 65536, 2, 0) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_email"

    echo "  [7/7] idx_passport (ngrambf_v1)..."
    run_sql "ALTER TABLE $TABLE ADD INDEX IF NOT EXISTS idx_passport passport TYPE ngrambf_v1(3, 65536, 2, 0) GRANULARITY 4"
    run_sql "ALTER TABLE $TABLE MATERIALIZE INDEX idx_passport"

    echo "  Проверка индексов:"
    run_sql "SELECT name, type FROM system.data_skipping_indices WHERE table = '$TABLE'"

    echo "  ✅ $TABLE готово"
done

echo ""
echo "=== Все индексы применены! ==="
