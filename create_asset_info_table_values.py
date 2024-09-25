import sqlite3
from datetime import datetime

def create_asset_info_table(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the asset_info table with the additional plug_proto and plug_uri columns
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS asset_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id TEXT,
            premise_id INTEGER,
            name TEXT,
            make TEXT,
            model TEXT,
            type TEXT,
            manufacturer TEXT,
            year INTEGER,
            notes TEXT,
            plug_id TEXT,
            plug_network TEXT,
            plug_ip TEXT,
            plug_subnet TEXT,
            plug_gateway TEXT,
            plug_ip_allocation TEXT,
            plug_sw_version TEXT,
            plug_hostname TEXT,
            plug_make TEXT,
            plug_type TEXT,
            plug_region TEXT,
            plug_amps TEXT,
            plug_proto TEXT,
            plug_uri TEXT,
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

def populate_asset_info_entry1(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to inert
    asset_id = 'DC234SN'
    premise_id = 1
    name = 'Borelli 2-door IDC'
    make = 'IDC'
    model = '2-door'
    type_ = 'Fridge'
    manufacturer = 'Borelli'
    year = 2023
    notes = ''
    plug_id = '5120ec82-aa16-4eb9-b912-1bf7b17'
    plug_network = 'WIFI'
    plug_ip = '192.168.2.159'
    plug_subnet = '255.255.255.0'
    plug_gateway = '192.168.2.1'
    plug_ip_allocation = 'dhcp'
    plug_sw_version = '14-08.1'
    plug_hostname = '5120ec82-aa16-4eb9-b912-1bf7b17'
    plug_make = 'Tasmota'
    plug_type = 'Power'
    plug_region = 'AU'
    plug_amps = 10
    plug_proto = 'http'
    plug_uri = 'cm?cmnd=Status+10'
    service_state = 'Active'
    service_mode = 'Demo'
    service_plan = 'Saver'

    # Insert the values into the asset_info table
    try:
        cursor.execute('''
            INSERT INTO asset_info (
                asset_id, premise_id, name, make, model, type, manufacturer, year, notes,
                plug_id, plug_network, plug_ip, plug_subnet, plug_gateway, plug_ip_allocation, plug_sw_version, plug_hostname, 
                plug_make, plug_type, plug_region, plug_amps, plug_proto, plug_uri,
                service_state, service_mode, service_plan
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            asset_id,
            premise_id,
            name,
            make,
            model,
            type_,
            manufacturer,
            year,
            notes,
            plug_id,
            plug_network,
            plug_ip,
            plug_subnet,
            plug_gateway,
            plug_ip_allocation,
            plug_sw_version,
            plug_hostname,
            plug_make,
            plug_type,
            plug_region,
            plug_amps,
            plug_proto,
            plug_uri,
            service_state,
            service_mode,
            service_plan
        ))
        conn.commit()
        print("Record inserted successfully.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")

    # Close the connection to the database
    conn.close()

def populate_asset_info_entry2(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    asset_id = 'D2209CK09FF60S151'
    premise_id = 1
    name = 'Borrelli upright freezer'
    make = 'R290'
    model = 'R290'
    type_ = 'Freezer'
    manufacturer = 'Borelli'
    year = 2023
    notes = ''
    plug_id = 'tasmota-F97904-6404'
    plug_network = 'WIFI'
    plug_ip = '192.168.2.133'
    plug_subnet = '255.255.255.0'
    plug_gateway = '192.168.2.1'
    plug_ip_allocation = 'dhcp'
    plug_sw_version = '14-08.1'
    plug_hostname = 'tasmota-F97904-6404'
    plug_make = 'Tasmota'
    plug_type = 'Power'
    plug_region = 'AU'
    plug_amps = 10
    plug_proto = 'http'
    plug_uri = 'cm?cmnd=Status+10'
    service_state = 'Active'
    service_mode = 'Demo'
    service_plan = 'Saver'

    # Insert the values into the asset_info table
    try:
        cursor.execute('''
            INSERT INTO asset_info (
                asset_id, premise_id, name, make, model, type, manufacturer, year, notes,
                plug_id, plug_network, plug_ip, plug_subnet, plug_gateway, plug_ip_allocation, plug_sw_version, plug_hostname,
                plug_make, plug_type, plug_region, plug_amps, plug_proto, plug_uri,
                service_state, service_mode, service_plan
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            asset_id,
            premise_id,
            name,
            make,
            model,
            type_,
            manufacturer,
            year,
            notes,
            plug_id,
            plug_network,
            plug_ip,
            plug_subnet,
            plug_gateway,
            plug_ip_allocation,
            plug_sw_version,
            plug_hostname,
            plug_make,
            plug_type,
            plug_region,
            plug_amps,
            plug_proto,
            plug_uri,
            service_state,
            service_mode,
            service_plan
        ))
        conn.commit()
        print("Record inserted successfully.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")

    # Close the connection to the database
    conn.close()

def populate_asset_info_entry3(db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define the values to insert
    asset_id = '11111111111111'
    premise_id = 1
    name = 'Beverage Cooler'
    make = 'SK156-HD'
    model = 'SK156-HD'
    type_ = 'Fridge'
    manufacturer = 'Borelli'
    year = 2021
    notes = ''
    plug_id = 'tasmota-F977AC-6060'
    plug_network = 'WIFI'
    plug_ip = '192.168.31.47'
    plug_subnet = '255.255.255.0'
    plug_gateway = '192.168.31.1'
    plug_ip_allocation = 'dhcp'
    plug_sw_version = '14-08.1'
    plug_hostname = 'tasmota-F977AC-6060'
    plug_make = 'Tasmota'
    plug_type = 'Power'
    plug_region = 'AU'
    plug_amps = 10
    plug_proto = 'http'
    plug_uri = 'cm?cmnd=Status+10'
    service_state = 'Active'
    service_mode = 'Demo'
    service_plan = 'Saver'

    # Insert the values into the asset_info table
    try:
        cursor.execute('''
            INSERT INTO asset_info (
                asset_id, premise_id, name, make, model, type, manufacturer, year, notes,
                plug_id, plug_network, plug_ip, plug_subnet, plug_gateway, plug_ip_allocation, plug_sw_version, plug_hostname,
                plug_make, plug_type, plug_region, plug_amps, plug_proto, plug_uri,
                service_state, service_mode, service_plan
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            asset_id,
            premise_id,
            name,
            make,
            model,
            type_,
            manufacturer,
            year,
            notes,
            plug_id,
            plug_network,
            plug_ip,
            plug_subnet,
            plug_gateway,
            plug_ip_allocation,
            plug_sw_version,
            plug_hostname,
            plug_make,
            plug_type,
            plug_region,
            plug_amps,
            plug_proto,
            plug_uri,
            service_state,
            service_mode,
            service_plan
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

    # Create the asset_info table
    create_asset_info_table(db_file)

    # Populate the asset_info table
    populate_asset_info_entry1(db_file)

    # Populate the asset_info table
    populate_asset_info_entry2(db_file)

# Populate the asset_info table
    populate_asset_info_entry3(db_file)
