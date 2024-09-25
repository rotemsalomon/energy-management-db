import sqlite3
from datetime import datetime

def create_org_info_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the org_info table with the specified columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS org_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            update_time TEXT NOT NULL,
            org_id TEXT NOT NULL,
            org_name TEXT NOT NULL,
            service_status TEXT NOT NULL,
            service_mode TEXT NOT NULL,
            service_plan TEXT NOT NULL
        )
    ''')
    conn.commit()

    # Close the connection
    conn.close()

def populate_org_info(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    update_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')
    org_id = '11 111 111 111'
    org_name = 'Sanders Place'
    service_status = 'Active'
    service_mode = 'Demo'
    service_plan = 'Trial'

    # Insert the values into the org_info table
    cursor.execute('''
        INSERT INTO org_info (
            update_time, org_id, org_name,
            service_status, service_mode, service_plan
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        update_time, org_id, org_name,
        service_status, service_mode, service_plan
    ))
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == '__main__':
    # Database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the org_info table
    create_org_info_table(db_file)

    # Populate the org_info table
    populate_org_info(db_file)

