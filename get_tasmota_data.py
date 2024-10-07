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
        logging.FileHandler("/var/log/tasmota_get_data.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def create_db_and_table(db_file):
    """Create the database and tasmota_energy_data table with necessary columns."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the tasmota_energy_data table if it doesn't exist, including the power_status column
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasmota_energy_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            response_time TEXT NOT NULL,
            energy_time TEXT,
            total_start_time TEXT,
            total REAL,
            yesterday REAL,
            today REAL,
            period REAL,
            power INTEGER,
            apparent_power INTEGER,
            reactive_power INTEGER,
            factor REAL,
            voltage REAL,
            current REAL,
            asset_id TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            cur_comp_state TEXT NOT NULL,
            power_status TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

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
        return asset_name, plug_ip, plug_proto, plug_uri
    else:
        logging.error(f"No asset information found for asset_id: {asset_id}")
        return None, None, None, None, None

def get_power_status(plug_proto, plug_ip):
    """Retrieve the power status of the device using the constructed URL."""
    try:
        power_status_url = f"{plug_proto}://{plug_ip}/cm?cmnd=Power"
        response = requests.get(power_status_url)    # Make the request without authentication
        response.raise_for_status() # Raises HTTPError for bad responses

        # Parse the JSON response
        power_status_data = response.json()
        power_status = power_status_data.get('POWER', 'UNKNOWN')  # Default to 'UNKNOWN' if not found
        #logging.info(f'Power status retrieved: {power_status}')
        return power_status
    
    except requests.RequestException as e:
        logging.error(f'Failed to retrieve power status from {power_status_url}: {e}')
        return 'UNKNOWN'
    except json.JSONDecodeError:
        logging.error('Failed to decode JSON response for power status.')
        return 'UNKNOWN'

def get_power_metrics(plug_proto, plug_ip, plug_uri):
    """Retrieve and parse power metrics from the Tasmota device."""
    try:
        # Dynamically construct URL
        power_metrics_url = f"{plug_proto}://{plug_ip}/{plug_uri}"
        response = requests.get(power_metrics_url)   # Make the request without authentication
        response.raise_for_status()  # Raises HTTPError for bad responses

        # Parse the JSON response
        data = response.json()

        # Extract relevant fields
        status_sns = data.get('StatusSNS', {})
        energy_time = status_sns.get('Time', '')
        energy_data = status_sns.get('ENERGY', {})
        
        # Add response time to record when request was made
        response_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')

        return {
            "response_time": response_time,
            "energy_time": energy_time,
            "total_start_time": energy_data.get('TotalStartTime', ''),
            "total": energy_data.get('Total', 0.0),
            "yesterday": energy_data.get('Yesterday', 0.0),
            "today": energy_data.get('Today', 0.0),
            "period": energy_data.get('Period', 0.0),
            "power": energy_data.get('Power', 0),
            "apparent_power": energy_data.get('ApparentPower', 0),
            "reactive_power": energy_data.get('ReactivePower', 0),
            "factor": energy_data.get('Factor', 0.0),
            "voltage": energy_data.get('Voltage', 0.0),
            "current": energy_data.get('Current', 0.0)
        }

    except (requests.ConnectionError, requests.Timeout, requests.RequestException) as e:
        logging.error(f'Failed to retrieve power metrics from {url}: {e}')
        return None
    except json.JSONDecodeError:
        logging.error('Failed to decode JSON response for power metrics.')
        return None

def fetch_and_save_data(url, conn, asset_id, asset_name, plug_proto, plug_ip, plug_uri):
    try:
        # Retrieve power metrics
        power_metrics = get_power_metrics(plug_proto, plug_ip, plug_uri)
        if not power_metrics:
            logging.error(f"Failed to retrieve power metrics from {url}.")
            return

        # Get power status
        power_status = get_power_status(plug_proto, plug_ip)

        # Determine compressor state based on power value
        power = power_metrics.get('power', 0)
        cur_comp_state = 'OFF' if power < 100 else 'ON'

        # Insert data into the database
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tasmota_energy_data (
                response_time, energy_time, total_start_time, total,
                yesterday, today, period, power, apparent_power, reactive_power, factor, voltage, current,
                asset_id, asset_name, cur_comp_state, power_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            power_metrics['response_time'],
            power_metrics['energy_time'],
            power_metrics['total_start_time'],
            power_metrics['total'],
            power_metrics['yesterday'],
            power_metrics['today'],
            power_metrics['period'],
            power,
            power_metrics['apparent_power'],
            power_metrics['reactive_power'],
            power_metrics['factor'],
            power_metrics['voltage'],
            power_metrics['current'],
            asset_id,
            asset_name,
            cur_comp_state,
            power_status
        ))
        conn.commit()

        #logging.info(f'Request successful. Data written to database for {url} with compressor state {cur_comp_state} and power status {power_status}.')

    except sqlite3.Error as e:
        logging.error(f"SQLite error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == '__main__':
    # Argument parsing to get asset_id from the command-line argument
    parser = argparse.ArgumentParser(description="Fetch and store energy data for a specific asset.")
    parser.add_argument('--asset-id', required=True, help="The asset ID for which to fetch and store energy data.")
    args = parser.parse_args()
    asset_id = args.asset_id

    # Default values
    db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

    # Create the database and table if not already created
    create_db_and_table(db_file)

    # Open a persistent database connection
    conn = sqlite3.connect(db_file)

    # Log that the script has started
    logging.info('Starting tasmota_sqlite3 service')

    # Fetch asset information and construct the URL components
    asset_name, plug_ip, plug_proto, plug_uri = get_asset_info(conn, asset_id)

    if plug_ip and plug_proto and plug_uri:
        # Continuous loop to run every 15 seconds
        while True:
            # Fetch and save data 4 times per minute (every 15 seconds)
            fetch_and_save_data(None, conn, asset_id, asset_name, plug_proto, plug_ip, plug_uri)

            # Wait for 15 seconds before the next request
            time.sleep(15)
    else:
        logging.error("No valid asset information found. Exiting script.")

    # Close the database connection when done
    conn.close()
