import os
import logging
import psycopg2
import time
from datetime import datetime
from psycopg2 import pool
from config import PG_CONFIG

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

def register_user(user_id, username, first_name):
    if not pg_pool:
        return
    
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            # Читаємо SQL з файлу
            try:
                sql_file_path = os.path.join(os.path.dirname(__file__), 'init_db.sql')
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                cur.execute(schema_sql)
            except Exception as e:
                logger.error(f"Error executing init_db.sql: {e}")
            
            cur.execute("""
                INSERT INTO subscriptions (user_id, username, first_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name
            """, (user_id, username, first_name))
            conn.commit()
    except Exception as e:
        logger.error(f"Error registering user {user_id}: {e}")
    finally:
        if conn:
            pg_pool.putconn(conn)

def check_subscription(user_id):
    """
    Checks if the user has an active subscription in subscriptions table.
    Returns (is_active, expiry_date)
    """
    if not pg_pool:
        return False, None
    
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            # Используем таблицу subscriptions
            cur.execute("""
                SELECT status, current_period_end 
                FROM subscriptions 
                WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                status, expiry_date = row
                
                if status == 'authorized':
                    return True, expiry_date
                
                if not expiry_date:
                    return False, None
                
                # Приводим к naive datetime для сравнения, если оно aware
                if expiry_date.tzinfo is not None:
                    expiry_date = expiry_date.replace(tzinfo=None)
                
                is_active = (status == 'active' and expiry_date > datetime.now())
                return is_active, expiry_date
            return False, None
    except Exception as e:
        logger.error(f"Error checking subscription for {user_id}: {e}")
        return False, None

    finally:
        if conn:
            pg_pool.putconn(conn)

def authorize_user(user_id):
    if not pg_pool:
        return False
    
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE subscriptions 
                SET status = 'authorized', updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
            """, (user_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error authorizing user {user_id}: {e}")
        return False
    finally:
        if conn:
            pg_pool.putconn(conn)

def get_user_info(user_id):
    """
    Returns registration info for the user.
    """
    if not pg_pool:
        return None
    
    conn = None
    try:
        conn = pg_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT registration_date FROM subscriptions WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            if row:
                return {"registration_date": row[0]}
            return None
    except Exception as e:
        logger.error(f"Error getting user info for {user_id}: {e}")
        return None
    finally:
        if conn:
            pg_pool.putconn(conn)
