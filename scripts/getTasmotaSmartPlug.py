import requests
import json
import csv
import time
import os
import sys
from datetime import datetime

# URLs
url1 = 'http://192.168.2.159/cm?cmnd=Status+10'
url2 = 'http://192.168.2.134/cm?cmnd=Status+10'

# Function to log messages to both console and log file
class Logger:
    def __init__(self, log_file):
        self.terminal = sys.stdout
        self.log = open(log_file, "a")

    def write(self, message):
        # Get the local time with timezone awareness for logging
        timestamp = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
        log_message = f'[{timestamp}] {message}'
        self.terminal.write(log_message)
        self.log.write(log_message)

    def flush(self):
        pass  # This flush method is needed for Python 3 compatibility.

def fetch_and_save_data(url, output_file_path):
    # Check if the file exists
    file_exists = os.path.exists(output_file_path)

    try:
        # Make the request without authentication
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses

        # Parse the JSON response
        data = response.json()
        data['url'] = url  # Add the URL to the data

        # Add local timestamp to the data
        data['local_timestamp'] = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')

        # Write the data to the CSV file immediately
        with open(output_file_path, 'a', newline='') as csv_file:
            dict_writer = csv.DictWriter(csv_file, fieldnames=data.keys())

            # Write the header only if the file doesn't exist
            if not file_exists:
                dict_writer.writeheader()
                file_exists = True  # Set this to True after the first write

            dict_writer.writerow(data)
            print(f'Request successful. Data written to {csv_file.name}')
    
    except requests.ConnectionError:
        print(f'Failed to connect to {url}. Please check your network connection.')
    except requests.Timeout:
        print(f'Request to {url} timed out.')
    except requests.RequestException as e:
        print(f'Failed to retrieve data: {e}')
    except json.JSONDecodeError:
        print('Failed to decode JSON response.')

if __name__ == '__main__':
    # Default values
    default_file_path = './data'
    default_file_name = 'borelli_idc.csv'

    # Log directory and log file setup
    log_directory = '/var/log'
    os.makedirs(log_directory, exist_ok=True)  # Create the directory if it doesn't exist
    log_file_path = os.path.join(log_directory, 'athom-script.log')

    # Setup logging
    sys.stdout = Logger(log_file_path)

    # Select URL (using default URL1)
    url = url1
    print(f'Using URL: {url}')

    # Combine file path and file name
    output_file_path = os.path.join(default_file_path, default_file_name)

    # Continuous loop to run every 15 seconds
    while True:
        # Fetch and save data 4 times per minute (every 15 seconds)
        fetch_and_save_data(url, output_file_path)

        # Wait for 15 seconds before the next request
        time.sleep(15)

