-- Таблиця для керування користувачами, ролями та авторизацією
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    phone TEXT,
    role VARCHAR(20) DEFAULT 'user', -- super_admin, admin, user
    password_hash TEXT,
    raw_password TEXT, -- Тільки для супер-адміна
    is_authorized BOOLEAN DEFAULT FALSE,
    is_banned BOOLEAN DEFAULT FALSE,
    created_by BIGINT, -- ID адміна, який видав пароль
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Копіюємо дані зі старої таблиці, якщо вона існує
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'subscriptions') THEN
        INSERT INTO users (user_id, username, first_name, registration_date)
        SELECT user_id, username, first_name, registration_date FROM subscriptions
        ON CONFLICT (user_id) DO NOTHING;
    END IF;
END $$;
