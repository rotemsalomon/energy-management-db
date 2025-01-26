import sqlite3
import csv
from tqdm import tqdm  # Import the progress bar

def table2csv(db_file, table_name, csv_name):
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Query to select column names
        cursor.execute(f'''
            PRAGMA table_info({table_name});
        ''')

       # Fetch column names in the table that match the filter
        columns = cursor.fetchall()
        filtered_columns = [col[1] for col in columns if col[1] in ['id', 'asset_id', 'asset_name', 'response_time', 'power_status', 'power', 'cur_comp_state']]

        # Ensure columns are ordered as per the predefined list
        desired_order = ['id', 'asset_id', 'asset_name', 'response_time', 'power_status', 'power', 'cur_comp_state']
        ordered_columns = [col for col in desired_order if col in filtered_columns]

        # Query to select all data from the table
        cursor.execute(f'''
            SELECT 
                id, 
                asset_id, 
                asset_name, 
                response_time, 
                power_status, 
                power,
                cur_comp_state 
            FROM {table_name}
            WHERE asset_id = 'DC234SN'
            AND id >= 1665168;
        ''')

        # Fetch all rows
        rows = cursor.fetchall()

        # Print the number of rows
        print(f"Number of rows to be exported: {len(rows)}")

        # Writing to CSV with progress bar
        with open(csv_name + '.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(ordered_columns)  # Write column names as the first row

            # Use tqdm to display progress bar
            with tqdm(total=len(rows), unit="rows") as pbar:
                for row in rows:
                    writer.writerow(row)
                    pbar.update(1)  # Update progress bar

        # Close the connection
        conn.close()
        print(f"\nCSV file '{csv_name}.csv' has been successfully created with {len(rows)} rows.")

    except sqlite3.OperationalError as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    # Database file path and table name
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'
    table_name = 'tasmota_energy_data'
    csv_name = '/root/projects/tasmota/scripts/raw_tasmota_data_dump'

    # Export table to CSV
    table2csv(db_file, table_name, csv_name)
