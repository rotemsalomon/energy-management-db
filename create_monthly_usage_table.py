import sqlite3
import logging

def create_monthly_usage_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the monthly_usage table with columns 
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time TEXT NOT NULL,
            date TEXT NOT NULL,
            asset_id TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            total_kwh REAL NOT NULL,
            total_kwh_charge TEXT,
            kwh_percentage_change REAL,
            charge_percentage_change REAL,
            kwh_change TEXT,
            charge_change TEXT
        )
    ''')
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == '__main__':
    # Database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the monthly_usage_table table
    create_monthly_usage_table(db_file)

    # Log that the script has executed
    logging.info('Table monthly_usage successfully created')

