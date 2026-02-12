import pymysql
from config import Config

def fix_mapping_status():
    conn = pymysql.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        database=Config.DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    with conn.cursor() as cur:
        sql = f"""
        UPDATE {Config.DB_TABLE}
        SET mapping_status = 'Mapped'
        WHERE mapping_status IS NULL OR TRIM(mapping_status) = '';
        """
        cur.execute(sql)
        print(f"Updated rows: {cur.rowcount}")

    conn.close()

if __name__ == "__main__":
    fix_mapping_status()
