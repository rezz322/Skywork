import os
import psycopg2
from dotenv import load_dotenv

def recreate_table():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB")
        )
        cur = conn.cursor()
        
        # 1. Drop and Create
        sql = """
        DROP TABLE IF EXISTS bd_tg_osint;
        CREATE TABLE bd_tg_osint (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id BIGINT NOT NULL UNIQUE,
            status VARCHAR(20) NOT NULL DEFAULT 'inactive', 
            current_period_start TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            current_period_end TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(sql)
        print("Table recreated successfully.")
        
        # 2. Activate for user
        user_id = 5355119908
        sql_activate = """
        INSERT INTO bd_tg_osint (user_id, status, current_period_start, current_period_end)
        VALUES (%s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '1 month');
        """
        cur.execute(sql_activate, (user_id,))
        print(f"Subscription activated for {user_id}.")
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    recreate_table()
