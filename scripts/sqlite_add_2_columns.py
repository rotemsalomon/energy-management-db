import sqlite3

def add_columns_to_table(db_file, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Step 1: Add new columns `asset_id` and `asset_name` to the existing table
    try:
        cursor.execute(f'''
            ALTER TABLE {table_name} ADD COLUMN asset_id TEXT;
        ''')
        cursor.execute(f'''
            ALTER TABLE {table_name} ADD COLUMN asset_name TEXT;
        ''')
        conn.commit()
        print("Columns `asset_id` and `asset_name` added successfully.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")
        return

    # Step 2: Update the table to populate the new columns with default values
    try:
        cursor.execute(f'''
            UPDATE {table_name}
            SET asset_id = 'DC234SN', asset_name = 'Borelli 2-door IDC';
        ''')
        conn.commit()
        print("New columns populated with default values.")
    except sqlite3.OperationalError as e:
        print(f"An error occurred during the update: {e}")

    # Close the connection to the database
    conn.close()

if __name__ == '__main__':
    # Database file path and table name
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'
    table_name = 'tasmota_energy_data'

    # Add new columns and update them with default values
    add_columns_to_table(db_file, table_name)

