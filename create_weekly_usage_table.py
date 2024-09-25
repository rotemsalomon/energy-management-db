import sqlite3
from datetime import datetime
import logging

def create_weekly_usage_table(db_file):
    """Create the weekly_usage table if it doesn't exist."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weekly_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time TEXT NOT NULL,
            week_start_date TEXT NOT NULL,
            week_end_date TEXT NOT NULL,
            asset_id TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            total_kwh REAL,
            total_kwh_charge TEXT
        )
    ''')
    conn.commit()
    
    # Close the connection
    conn.close()

if __name__ == '__main__':
    # Database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the weekly_usage_table table
    create_weekly_usage_table(db_file)

    # Log that the script has executed
    logging.info('Table weekly_usage successfully created')

