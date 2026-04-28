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
            # Create table if not exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id BIGINT PRIMARY KEY
                )
            """)
            
            # Ensure user_id is BIGINT
            cur.execute("""
                DO $$ 
                DECLARE 
                    pk_name text;
                BEGIN 
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND column_name = 'user_id' 
                        AND data_type = 'uuid'
                    ) THEN
                        SELECT conname INTO pk_name 
                        FROM pg_constraint 
                        WHERE conrelid = 'subscriptions'::regclass AND contype = 'p';

                        IF pk_name IS NOT NULL THEN
                            EXECUTE 'ALTER TABLE subscriptions DROP CONSTRAINT ' || pk_name;
                        END IF;

                        ALTER TABLE subscriptions RENAME COLUMN user_id TO user_id_uuid_old;
                        ALTER TABLE subscriptions ALTER COLUMN user_id_uuid_old DROP NOT NULL;
                        ALTER TABLE subscriptions ADD COLUMN user_id BIGINT PRIMARY KEY;
                    ELSIF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND column_name = 'user_id'
                    ) THEN
                        ALTER TABLE subscriptions ADD COLUMN user_id BIGINT PRIMARY KEY;
                    END IF;

                    FOR pk_name IN (
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = 'subscriptions' 
                        AND is_nullable = 'NO' 
                        AND column_default IS NULL 
                        AND column_name != 'user_id'
                    ) LOOP
                        EXECUTE 'ALTER TABLE subscriptions ALTER COLUMN ' || quote_ident(pk_name) || ' DROP NOT NULL';
                    END LOOP;
                END $$;
            """)
            
            for col, col_type in [
                ("username", "TEXT"), 
                ("first_name", "TEXT"), 
                ("status", "VARCHAR(20) DEFAULT 'inactive'"),
                ("current_period_start", "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"),
                ("current_period_end", "TIMESTAMPTZ"),
                ("registration_date", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ]:
                cur.execute(f"""
                    DO $$ 
                    BEGIN 
                        BEGIN
                            ALTER TABLE subscriptions ADD COLUMN {col} {col_type};
                        EXCEPTION
                            WHEN duplicate_column THEN NULL;
                        END;
                    END $$;
                """)
            
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
