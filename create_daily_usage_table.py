import sqlite3
import logging

def create_daily_usage_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the daily_usage table with the specified schema
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time TEXT DEFAULT '',
            date TEXT,
            day_of_week TEXT,
            hour INTEGER,
            asset_id TEXT,
            asset_name TEXT,
            current_hour_kwh REAL DEFAULT 0,
            current_hour_kwh_co2e REAL DEFAULT 0.0,
            total_kwh REAL,
            total_kwh_charge TEXT,
            total_kwh_co2e REAL DEFAULT 0.0,
            percentage_change_kwh REAL,
            daily_total_kwh REAL,
            daily_total_kwh_charge REAL,
            daily_total_kwh_co2e REAL DEFAULT 0.0,
            cnt_comp_on INTEGER,
            cnt_comp_off INTEGER,
            ave_comp_runtime REAL DEFAULT 0,
            min_comp_runtime REAL DEFAULT 0,
            max_comp_runtime REAL DEFAULT 0
        )
    ''')

    # Create indexes for the daily_usage table
    cursor.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_asset_date_hour 
        ON daily_usage (asset_id, date, hour);
    ''')

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_daily_usage_date_hour 
        ON daily_usage (date, hour);
    ''')

    conn.commit()

if __name__ == '__main__':
    # Default values
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the database and table if not already created
    create_daily_usage_table(db_file)

    # Log that the script has executed
    logging.info('Table daily_usage table successfully created')
