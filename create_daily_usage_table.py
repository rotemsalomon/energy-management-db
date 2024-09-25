import sqlite3
import logging

def create_daily_usage_table(db_file):
    # Create a new SQLite database (or connect to it if it already exists)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time TEXT NOT NULL,
            asset_id TEXT,
            asset_name TEXT,
            date TEXT,
            total_kwh REAL,
            cnt_comp_on INTEGER,
            cnt_comp_off INTEGER,
            ave_comp_runtime REAL,
            max_comp_runtime REAL,
            min_comp_runtime REAL,
            update_time TEXT,
            total_kwh_charge TEXT
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # Default values
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the database and table if not already created
    create_table_daily_usage(db_file)

    # Log that the script has executed
    logging.info('Table daily_usage table successfully created')
