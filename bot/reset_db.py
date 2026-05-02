import os
import psycopg2
import logging
from dotenv import load_dotenv

# Завантажуємо .env з батьківської директорії
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path=env_path, override=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reset_db")

def reset_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_DB")
        )
        cur = conn.cursor()
        
        logger.info("Dropping existing tables...")
        cur.execute("DROP TABLE IF EXISTS users CASCADE;")
        
        # Читаємо схему з init_db.sql
        sql_file_path = os.path.join(os.path.dirname(__file__), 'init_db.sql')
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            logger.info("Recreating tables from init_db.sql...")
            cur.execute(f.read())
            
        conn.commit()
        logger.info("Database reset successfully!")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error resetting database: {e}")

if __name__ == "__main__":
    reset_db()
