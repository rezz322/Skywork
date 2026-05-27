-- ============================================================
-- Добавляем skip-индексы для быстрого поиска в ClickHouse
-- Применить: выполнить в ClickHouse для обеих таблиц
-- ============================================================

-- === global_search_ua ===

-- Токен-индекс для ФИО (hasToken работает по нему мгновенно)
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_fio fio TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4;

-- Bloom-filter для точного поиска телефона, INN, SNILS, паспорта
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_phone phone TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_inn inn TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_snils snils TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_passport passport TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_email email TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_tg_id tg_id TYPE minmax GRANULARITY 4;
ALTER TABLE global_search_ua ADD INDEX IF NOT EXISTS idx_nickname nickname TYPE bloom_filter(0.01) GRANULARITY 4;

-- Материализуем индексы (применяем к существующим данным)
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_fio;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_phone;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_inn;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_snils;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_passport;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_email;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_tg_id;
ALTER TABLE global_search_ua MATERIALIZE INDEX idx_nickname;

-- === global_search_ru ===

ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_fio fio TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_phone phone TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_inn inn TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_snils snils TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_passport passport TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_email email TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_tg_id tg_id TYPE minmax GRANULARITY 4;
ALTER TABLE global_search_ru ADD INDEX IF NOT EXISTS idx_nickname nickname TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE global_search_ru MATERIALIZE INDEX idx_fio;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_phone;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_inn;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_snils;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_passport;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_email;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_tg_id;
ALTER TABLE global_search_ru MATERIALIZE INDEX idx_nickname;
