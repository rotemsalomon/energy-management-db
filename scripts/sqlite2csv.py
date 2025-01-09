import sqlite3
import csv

def table2csv(db_file, table_name, csv_name):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Query to select all data from the table
        cursor.execute(f'''
            SELECT * FROM {table_name} LIMIT 1;
        ''')

        # Fetch all rows
        rows = cursor.fetchall()

        # Writing to CSV
        with open(csv_name + '.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)

        # Close the connection
        conn.close()
        print(f"CSV file '{csv_name}.csv' has been successfully created.")

    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    # Database file path and table name
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'
    table_name = 'tasmota_energy_data'
    csv_name = '/root/projects/tasmota/sqlite3_db/raw_tasmota_data_dump'

    # Export table to CSV
    table2csv(db_file, table_name, csv_name)
