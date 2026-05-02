import os
import logging
import psycopg2
import hashlib
from datetime import datetime
from psycopg2 import pool
from config import PG_CONFIG, SUPER_ADMIN_IDS

logger = logging.getLogger("bot.database")

try:
    pg_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,
        user=PG_CONFIG["user"],
        password=PG_CONFIG["password"],
        host=PG_CONFIG["host"],
        port=PG_CONFIG["port"],
        database=PG_CONFIG["database"]
    )
    logger.info("PostgreSQL connection pool created")
except Exception as e:
    logger.error(f"Error creating PostgreSQL connection pool: {e}")
    pg_pool = None

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(user_id, username, first_name):
    """Сбор ID и ника при первом контакте"""
    if not pg_pool: return
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            # Инициализация схемы
            sql_file_path = os.path.join(os.path.dirname(__file__), 'init_db.sql')
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                cur.execute(f.read())
            
            cur.execute("""
                INSERT INTO users (user_id, username, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    updated_at = CURRENT_TIMESTAMP
            """, (user_id, username, first_name))
            conn.commit()
    except Exception as e:
        logger.error(f"Error registering user {user_id}: {e}")
    finally:
        if conn: pg_pool.putconn(conn)

def is_super_admin(user_id):
    return user_id in SUPER_ADMIN_IDS

def get_user(user_id):
    if not pg_pool: return None
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            return dict(zip(columns, row)) if row else None
    finally:
        if conn: pg_pool.putconn(conn)

def get_user_by_username(username):
    if not username: return None
    username = username.replace("@", "").strip()
    if not pg_pool: return None
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username,))
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            return dict(zip(columns, row)) if row else None
    finally:
        if conn: pg_pool.putconn(conn)

def set_user_password(username, password, admin_id):
    """Админ выдает пароль пользователю по нику"""
    user = get_user_by_username(username)
    if not user: return False, "Користувач не знайдений в базі. Нехай спочатку напише боту /start"
    
    h = hash_password(password)
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET password_hash = %s, raw_password = %s, created_by = %s, is_authorized = FALSE 
                WHERE user_id = %s
            """, (h, password, admin_id, user['user_id']))
            conn.commit()
            return True, f"Пароль для {username} встановлено: {password}"
    except Exception as e:
        logger.error(f"Error setting password: {e}")
        return False, str(e)
    finally:
        if conn: pg_pool.putconn(conn)

def check_auth(user_id):
    """Проверка, авторизован ли пользователь"""
    if is_super_admin(user_id): return True
    user = get_user(user_id)
    if not user: return False
    if user['is_banned']: return False
    if user['role'] == 'admin': return True
    return user['is_authorized']

def validate_password(user_id, password):
    """Проверка пароля при входе"""
    user = get_user(user_id)
    pwd_hash = hash_password(password)
    
    # 1. Сначала проверим, не является ли это паролем другого пользователя (кража)
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT username, created_by, user_id FROM users WHERE password_hash = %s", (pwd_hash,))
            other = cur.fetchone()
            if other and other[2] != user_id:
                logger.warning(f"Theft detected: {user_id} used password of {other[0]}")
                return False, f"theft:{other[0]}:{other[1] or 'system'}"
    except Exception as e:
        logger.error(f"Error in theft check: {e}")
    finally:
        if conn: pg_pool.putconn(conn)

    # 2. Теперь проверим, совпадает ли это с паролем самого пользователя
    if user and user['password_hash'] == pwd_hash:
        return True, "success"
            
    return False, "wrong"

def authorize_user_step_1(user_id):
    """Помечает что пароль введен верно (для ожидания телефона)"""
    return True # Логика в handlers.py через временный флаг или Redis

def update_user_phone(user_id, phone):
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET phone = %s, is_authorized = TRUE WHERE user_id = %s", (phone, user_id))
            conn.commit()
    finally:
        if conn: pg_pool.putconn(conn)

def set_role(user_id, role):
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET role = %s WHERE user_id = %s", (role, user_id))
            conn.commit()
            return True
    except: return False
    finally:
        if conn: pg_pool.putconn(conn)

def ban_user(user_id, status=True):
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET is_banned = %s, is_authorized = FALSE WHERE user_id = %s", (status, user_id))
            conn.commit()
            return True
    finally:
        if conn: pg_pool.putconn(conn)

def get_all_by_role(role):
    if not pg_pool: return []
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, username, raw_password, is_banned FROM users WHERE role = %s", (role,))
            return cur.fetchall()
    finally:
        if conn: pg_pool.putconn(conn)

def get_all_users():
    if not pg_pool: return []
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, username, phone, is_authorized, role, first_name FROM users")
            return cur.fetchall()
    finally:
        if conn: pg_pool.putconn(conn)

def get_users_by_admin(admin_id):
    if not pg_pool: return []
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, username, phone, is_authorized, role, first_name FROM users WHERE created_by = %s", (admin_id,))
            return cur.fetchall()
    finally:
        if conn: pg_pool.putconn(conn)

def delete_user(user_id):
    if not pg_pool: return False
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            conn.commit()
            return True
    except: return False
    finally:
        if conn: pg_pool.putconn(conn)

def get_db_stats():
    """Получение статистики по базе данных"""
    if not pg_pool: return {}
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            # Общая статистика пользователей
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM users WHERE is_authorized = TRUE")
            authorized_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
            admins = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM users WHERE is_banned = TRUE")
            banned_users = cur.fetchone()[0]
            
            return {
                "total_users": total_users,
                "authorized_users": authorized_users,
                "admins": admins,
                "banned_users": banned_users
            }
    except Exception as e:
        logger.error(f"Error getting db stats: {e}")
        return {}
    finally:
        if conn: pg_pool.putconn(conn)
