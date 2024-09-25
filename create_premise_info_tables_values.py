import sqlite3
from datetime import datetime

def create_prem_info_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the prem_info table without making premise_id auto-increment
    cursor.execute('''

    CREATE TABLE IF NOT EXISTS prem_info (
        premise_id INTEGER PRIMARY KEY AUTOINCREMENT,
        premise_name TEXT,
        org_id INTEGER,
        street_number TEXT,
        street TEXT,
        city TEXT,
        zip TEXT,
        state TEXT,
        country TEXT,
        supplier_id INTEGER,
        supplier_type TEXT,
        supplier_name TEXT,
        supplier_plan_name TEXT,
        service_state TEXT,
        service_mode TEXT,
        service_plan TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    conn.commit()

    # Close the connection
    conn.close()

def populate_prem_info(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    premise_name = 'Main Office'
    street_number = '1'
    street = 'Brooks street'
    city = 'Melbourne'
    zip = '94301'
    state = 'Victoria'
    country = 'Australia'
    supplier_id = '74 115 061 375'
    supplier_type = 'Electricity'
    supplier_name = 'AGL'
    supplier_plan_name = 'Business Plan'
    service_state = 'Active'
    service_mode = 'Demo'
    service_plan = 'Trial'
    org_id = '239057203'

    # Insert the values into the prem_info table
    cursor.execute('''
        INSERT INTO prem_info (
            premise_name, street_number,
            street, city, zip, state, country, supplier_id, supplier_type, supplier_name,
            supplier_plan_name, service_state, service_mode, service_plan, org_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        premise_name, street_number,
        street, city, zip, state, country, supplier_id, supplier_type, supplier_name,
        supplier_plan_name, service_state, service_mode, service_plan, org_id
    ))
    conn.commit()

    # Close the connection
    conn.close()

def populate_prem_info1(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    premise_name = 'Office'
    street_number = '11-15'
    street = 'Albert street'
    city = 'Richmond'
    zip = '3121'
    state = 'Victoria'
    country = 'Australia'
    supplier_id = '74 115 061 375'
    supplier_type = 'Electricity'
    supplier_name = 'AGL'
    supplier_plan_name = 'Business Plan'
    service_state = 'Active'
    service_mode = 'Demo'
    service_plan = 'Trial'
    org_id = '11 111 111 111'

    # Insert the values into the prem_info table
    cursor.execute('''
        INSERT INTO prem_info (
            premise_name, street_number,
            street, city, zip, state, country, supplier_id, supplier_type, supplier_name,
            supplier_plan_name, service_state, service_mode, service_plan, org_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        premise_name, street_number,
        street, city, zip, state, country, supplier_id, supplier_type, supplier_name,
        supplier_plan_name, service_state, service_mode, service_plan, org_id
    ))
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == '__main__':
    # Database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the org_info table
    create_prem_info_table(db_file)

    # Populate the org_info table
    populate_prem_info(db_file)
    populate_prem_info1(db_file)
