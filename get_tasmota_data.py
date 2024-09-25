import requests
import json
import sqlite3
import time
import sys
import logging
from datetime import datetime
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler("/var/log/tasmota_sqlite3.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_asset_info(conn, asset_id):
    """Retrieve asset_name, plug_ip, plug_proto, and plug_uri based on asset_id."""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT name, plug_ip, plug_proto, plug_uri
        FROM asset_info
        WHERE asset_id = ?
    ''', (asset_id,))

    result = cursor.fetchone()
    if result:
        asset_name, plug_ip, plug_proto, plug_uri = result
        # Dynamically construct URL
        url = f"{plug_proto}://{plug_ip}/{plug_uri}"
        return asset_name, url
    else:
        logging.error(f"No asset information found for asset_id: {asset_id}")
        return None, None

def fetch_and_save_data(url, conn, asset_id):
    try:
        # Make the request without authentication
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses

        # Parse the JSON response
        data = response.json()

        # Extract relevant fields
        response_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')
        status_sns = data.get('StatusSNS', {})
        energy_time = status_sns.get('Time', '')
        energy_data = status_sns.get('ENERGY', {})

        # Get asset_name from asset_info
        asset_name, _ = get_asset_info(conn, asset_id)

        # Determine compressor state based on power value
        power = energy_data.get('Power', 0)
        cur_comp_state = 'OFF' if power < 100 else 'ON'

        # Insert data into the database
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tasmota_energy_data (
                url, response_time, energy_time, total_start_time, total,
                yesterday, today, period, power, apparent_power, reactive_power, factor, voltage, current,
                asset_id, asset_name, cur_comp_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            url,
            response_time,
            energy_time,
            energy_data.get('TotalStartTime', ''),
            energy_data.get('Total', 0.0),
            energy_data.get('Yesterday', 0.0),
            energy_data.get('Today', 0.0),
            energy_data.get('Period', 0.0),
            power,
            energy_data.get('ApparentPower', 0),
            energy_data.get('ReactivePower', 0),
            energy_data.get('Factor', 0.0),
            energy_data.get('Voltage', 0.0),
            energy_data.get('Current', 0.0),
            asset_id,
            asset_name,
            cur_comp_state
        ))
        conn.commit()

        # Log successful data fetching and saving
        # logging.info(f'Request successful. Data written to database for {url} with compressor state {cur_comp_state}.')

    except (requests.ConnectionError, requests.Timeout, requests.RequestException, json.JSONDecodeError) as e:
        logging.error(f'Failed to retrieve data from {url}: {e}')

        # Insert a new row with the current time, empty data, but include url, asset_id, and asset_name
        response_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')
        asset_name, _ = get_asset_info(conn, asset_id)

        # Record the failure into the database
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tasmota_energy_data (
                url, response_time, energy_time, total_start_time, total,
                yesterday, today, period, power, apparent_power, reactive_power, factor, voltage, current,
                asset_id, asset_name, cur_comp_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            url,
            response_time,
            '', '', 0.0, 0.0, 0.0, 0.0, 0, 0, 0, 0.0, 0.0, 0.0,
            asset_id,
            asset_name,
            'UNKNOWN'
        ))
        conn.commit()

        logging.info(f"Recorded failure to database with no data for {url} at {response_time}, asset_id: {asset_id}, asset_name: {asset_name}")

if __name__ == '__main__':
    # Argument parsing to get asset_id from the command-line argument
    parser = argparse.ArgumentParser(description="Fetch and store energy data for a specific asset.")
    parser.add_argument('--asset-id', required=True, help="The asset ID for which to fetch and store energy data.")
    args = parser.parse_args()
    asset_id = args.asset_id

    # Default values
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Open a persistent database connection
    conn = sqlite3.connect(db_file)

    # Log that the script has started
    logging.info('Starting tasmota_sqlite3 service')

    # Fetch asset information and construct the URL
    asset_name, url = get_asset_info(conn, asset_id)

    if url:
        # Continuous loop to run every 15 seconds
        while True:
            # Fetch and save data 4 times per minute (every 15 seconds)
            fetch_and_save_data(url, conn, asset_id)

            # Wait for 15 seconds before the next request
            time.sleep(15)
    else:
        logging.error("No valid URL found. Exiting script.")

    # Close the database connection when done
    conn.close()

