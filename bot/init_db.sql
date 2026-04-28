-- Таблиця для керування підписками та авторизацією користувачів
CREATE TABLE IF NOT EXISTS subscriptions (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    status VARCHAR(20) DEFAULT 'inactive',
    current_period_start TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    current_period_end TIMESTAMPTZ,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Тут можна додавати інші таблиці або індекси в майбутньому
