import sqlite3
import csv

def table2csv(db_file, table_name, csv_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        # Query to select all data from the table
        cursor.execute(f'''
            SELECT * FROM {table_name} LIMIT 1;
        ''')
        
        print("Fetching all rows from the table {table_name}")
        # Fetch all rows
        rows = cursor.fetchall()

        # Opening CSV and appending to file
        print("Opening CSV and appending to file {csv_name}")

        with open({csv_name}, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)

        # Close the connection
        print("CSV file writing to {csv_name} completed successfully")
        conn.close()
        print("Connection to {db_file} successfully closed")

    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")
        return

if __name__ == '__main__':
    # Database file path and table name
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'
    table_name = 'tasmota_energy_data'
    csv_name = '/root/projects/tasmota/sqlite3_db/raw_tasmota_data_dump'

    # Export table to CSV
    table2csv(db_file, table_name, csv_name)