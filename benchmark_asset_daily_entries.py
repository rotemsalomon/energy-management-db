# python3 benchmark_asset_daily_entries.py --start_date 2024-09-26 --end_date 2024-09-26 --asset_ids DC234SN,D2209CK09FF60S151
# python3 benchmark_asset_daily_entries.py --start_date 2024-01-01 --end_date 2024-01-31 --asset_ids DC234SN,AB123XY

import sqlite3
import argparse
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='/var/log/tasmota-scripts.log',
    filemode='a'
)

# Function to update the benchmark flag in the daily_usage table
def update_benchmark_entries(db_file, start_date, end_date, asset_ids):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Convert asset_ids list to a tuple format for SQL query
    asset_ids_tuple = tuple(asset_ids)

    try:
        # Update the is_benchmark flag for the specified date range and asset_ids
        cursor.execute("""
            UPDATE daily_usage 
            SET is_benchmark = 1
            WHERE date BETWEEN ? AND ?
            AND asset_id IN ({})
        """.format(','.join('?' for _ in asset_ids)), (start_date, end_date, *asset_ids_tuple))

        conn.commit()
        logging.info(f"Benchmark entries updated for asset_ids: {asset_ids} between {start_date} and {end_date}")
    except sqlite3.Error as e:
        logging.error(f"Error updating benchmark entries: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    # Default database file path
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Command-line argument parser
    parser = argparse.ArgumentParser(description="Update benchmark entries in the daily_usage table.")
    parser.add_argument('--start_date', type=str, required=True, help='Start date for benchmark range (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date for benchmark range (YYYY-MM-DD)')
    parser.add_argument('--asset_ids', type=str, required=True, help='Comma-separated list of asset IDs')
    parser.add_argument('--db_file', type=str, default=db_file, help='Path to the SQLite database file')

    # Parse the arguments
    args = parser.parse_args()

    # Split the asset_ids argument into a list
    asset_ids = args.asset_ids.split(',')

    # Call the function to update benchmark entries
    update_benchmark_entries(args.db_file, args.start_date, args.end_date, asset_ids)
