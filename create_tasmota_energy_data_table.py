import sqlite3
from datetime import datetime

def create_db_and_table(db_file):
    # Create a new SQLite database (or connect to it if it already exists)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the table if it doesn't exist, with cur_comp_state column included
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasmota_energy_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            response_time TEXT NOT NULL,
            energy_time TEXT NOT NULL,
            total_start_time TEXT,
            total REAL,
            yesterday REAL,
            today REAL,
            period REAL,
            power INTEGER,
            apparent_power INTEGER,
            reactive_power INTEGER,
            factor REAL,
            voltage REAL,
            current REAL,
            asset_id TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            cur_comp_state TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # Default values
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the database and table if not already created
    create_db_and_table(db_file)

    # Log that the script has executed
    logging.info('Table tasmota_energy_data successfully created')
