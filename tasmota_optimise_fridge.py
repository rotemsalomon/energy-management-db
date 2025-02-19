import requests
import pandas as pd
import time
from datetime import datetime

# File paths
input_file_path = 'data/fridge_simulator_test_data.csv'
start_time = datetime.now().strftime('%Y-%m-%d-%H-%M')
output_file_path = f'../data/fridge_simulator_iteration_result_{start_time}.csv'

def get_power_state():
    """Check the current power state of the smart plug."""
    url = "http://192.168.2.77/cm?cmnd=Power"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return 1 if data.get("POWER") == "ON" else 0
    except requests.RequestException as e:
        print(f"Error checking power state: {e}")
        return None

def set_power_state(state):
    """Set the power state of the smart plug."""
    command = "Power%20On" if state == 1 else "Power%20Off"
    url = f"http://192.168.2.77/cm?cmnd={command}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return data.get("POWER") == ("ON" if state == 1 else "OFF"), url
    except requests.RequestException as e:
        print(f"Error setting power state: {e}")
        return False, url

def log_action(state_start_time, url, resulting_state):
    """Log the state transition to a CSV file."""
    df = pd.DataFrame([[state_start_time, url, resulting_state]], 
                      columns=['state_start_time', 'url', 'resulting_state'])
    df.to_csv(output_file_path, mode='a', header=not pd.io.common.file_exists(output_file_path), index=False)

def monitor_power_state(schedule):
    """Continuously checks local time and updates power state when required."""
    print("Starting power state monitoring...")
    
    while True:
        current_time = datetime.now().strftime('%H:%M:%S')
        if current_time in schedule:
            expected_state = schedule[current_time]
            current_state = get_power_state()
            
            if current_state is not None and current_state != expected_state:
                print(f"[{current_time}] Changing power state to {'ON' if expected_state else 'OFF'}")
                success, url = set_power_state(expected_state)
                resulting_state = get_power_state()
                
                if success:
                    print(f"[{current_time}] Successfully changed power state to {'ON' if resulting_state else 'OFF'}")
                else:
                    print(f"[{current_time}] Failed to change power state")
                
                log_action(current_time, url, resulting_state)
        
        time.sleep(1)  # Check every second

# Load schedule
df = pd.read_csv(input_file_path)
schedule = dict(zip(df['state_start_time'], df['state']))

# Start monitoring
monitor_power_state(schedule)
