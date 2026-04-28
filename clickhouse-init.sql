-- Створення таблиці для глобального пошуку RU
CREATE TABLE IF NOT EXISTS global_search_ru (
    fio String, 
    phone String, 
    email String, 
    inn String, 
    snils String, 
    driver_license String, 
    address String, 
    nickname String, 
    transport String, 
    birth_date String, 
    source_table String, 
    passport String, 
    password String,
    raw_data String,
    tg_id Int64
) ENGINE = MergeTree()
ORDER BY (phone, inn, fio, email);

-- Можна також додати стандартну таблицю, якщо її ще немає
CREATE TABLE IF NOT EXISTS global_search_n (
    fio String,
    phone String,
    email String,
    inn String,
    snils String,
    address String,
    nickname String,
    birth_date String,
    source_table String,
    raw_data String
) ENGINE = MergeTree()
ORDER BY (phone, fio);
