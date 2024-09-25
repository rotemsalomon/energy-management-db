import sqlite3
from datetime import datetime

def create_energy_rates_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create a new table called energy_rates
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                update_time TEXT NOT NULL,
                supplier_name TEXT NOT NULL,
                supplier_type TEXT,
                supplier_url TEXT,
                supplier_plan_name TEXT,
                rate REAL NOT NULL,
                currency TEXT NOT NULL,
                rate_type TEXT,
                rate_start TEXT NOT NULL,
                rate_end TEXT NOT NULL
            )
        ''')
        conn.commit()
        print("Table `energy_rates` created successfully.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")

    # Close the connection to the database
    conn.close()

def populate_energy_rates(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    update_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')
    supplier_name = 'AGL'
    supplier_type = 'Retailer'
    supplier_url = 'https://agl.com.au'
    supplier_plan_name = 'Business Plan'
    rate = 0.27
    currency = 'AUD'
    rate_start = '21:00:00'
    rate_end = '06:59:59'

    # Insert the values into the energy_rates table
    try:
        cursor.execute('''
            INSERT INTO energy_rates (
                update_time, supplier_name, supplier_type, supplier_plan_name, rate, currency, rate_start, rate_end, supplier_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            update_time,
            supplier_name,
            supplier_type,
            supplier_plan_name,
            rate,
            currency,
            rate_start,
            rate_end,
            supplier_url
        ))
        conn.commit()
        print("Record inserted successfully.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")

    # Close the connection to the database
    conn.close()

if __name__ == '__main__':
    # Database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the energy_rates table
    create_energy_rates_table(db_file)

    # Populate the energy_rates table
    populate_energy_rates(db_file)
