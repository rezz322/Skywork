import logging
import psycopg2
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
            
            for col, col_type in [("username", "TEXT"), ("first_name", "TEXT"), ("registration_date", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")]:
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
