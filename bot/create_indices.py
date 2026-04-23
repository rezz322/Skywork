import os
import psycopg2
import time
import logging
from search_service import SEARCH_CONFIG, DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("indexer")

def create_indices():
    logger.info("Starting database indexing process...")
    logger.info(f"Target Database: {DB_CONFIG['dbname']} at {DB_CONFIG['host']}")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Ensure pg_trgm is enabled
        logger.info("Ensuring pg_trgm extension is enabled...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        
        total_configs = len(SEARCH_CONFIG)
        for i, config in enumerate(SEARCH_CONFIG, 1):
            table = config["table"]
            cols = config["cols"]
            
            logger.info(f"[{i}/{total_configs}] Processing table: '{table}'")
            
            for col in cols:
                # Truncate index name if it's too long (Postgres limit 63 bytes)
                index_name = f"trgm_idx_{table}_{col}"
                if len(index_name) > 60:
                    index_name = index_name[:50] + "_trgm"
                
                logger.info(f"  - Creating GIN index for column: '{col}'...")
                start_time = time.time()
                
                try:
                    # Using double quotes for table and column names to handle reserved words and Cyrillic
                    sql = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table}" USING gin ("{col}" gin_trgm_ops);'
                    cur.execute(sql)
                    
                    duration = time.time() - start_time
                    logger.info(f"    ✅ Success! Duration: {duration:.2f} seconds")
                except Exception as e:
                    logger.error(f"    ❌ Error creating index on {table}({col}): {e}")
                    
        cur.close()
        conn.close()
        logger.info("Indexing process completed successfully!")
        
    except Exception as e:
        logger.critical(f"Critical error during indexing: {e}")

if __name__ == "__main__":
    create_indices()
