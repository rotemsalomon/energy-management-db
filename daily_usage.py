import sqlite3
from datetime import datetime, timedelta
import schedule # type: ignore
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='/var/log/tasmota-daily-kwh.log',
    filemode='a'
)

# Emission factors (kg CO2-e per kWh)
EF2 = 0.68  # Scope 2 emission factor
EF3 = 0.09  # Scope 3 emission factor

# Database file path
db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

def ensure_real(value):
    """
    Converts a value to a float, returning None if the conversion fails.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        logging.error(f"Failed to convert value to REAL: {value}")
        return None


def calculate_benchmark_percentage(delta, benchmark):
    """
    Calculates the percentage change between delta and benchmark.
    Returns 0 if the benchmark is 0 to avoid division errors.
    """
    return round((delta / benchmark) * 100, 2) if benchmark != 0 else 0

def calculate_percentage_change_kwh(today_kwh, yesterday_kwh):
    """ Calculate the percentage change between today and yesterday's kWh usage. """
    if yesterday_kwh == 0:
        return 100.0 if today_kwh > 0 else 0.0
    return ((today_kwh - yesterday_kwh) / yesterday_kwh) * 100

def format_runtime(minutes):
    # Convert total minutes to seconds
    total_seconds = int(minutes * 60)  # Convert to total seconds
    # Calculate minutes and seconds
    mins, secs = divmod(total_seconds, 60)
    return f"{mins:02}:{secs:02}"  # Format as mm:ss

def calculate_co2e_emission(kwh):
    """Calculate CO2e emissions in tonnes."""
    EF2 = 0.68  # Scope 2 emission factor in kg CO2e/kWh
    EF3 = 0.09  # Scope 3 emission factor in kg CO2e/kWh

    # Calculate total CO2e in tonnes
    tCO2e = (kwh * (EF2 + EF3)) / 1000  # Convert kg to tonnes

    # Return CO2e value in grams if it's less than 0.5 tonnes
    if tCO2e < 0.5:
        return round(tCO2e * 1000, 2)  # Convert to grams
    return round(tCO2e, 2)  # Return in tonnes

def get_org_id_and_premise_id_for_asset(cursor, asset_id):
    """
    Lookup the org_id and premise_id for a given asset_id from the prem_info table.
    """
    try:
        cursor.execute('''
            SELECT org_id, premise_id FROM asset_info WHERE asset_id = ?
        ''', (asset_id,))
        result = cursor.fetchone()
        
        if result:
            org_id, premise_id = result
            #logging.info(f"Found org_id: {org_id} and premise_id: {premise_id} for asset_id: {asset_id}")
            return org_id, premise_id
        else:
            logging.warning(f"No org_id or premise_id found for asset_id: {asset_id}")
            return None, None
    except Exception as e:
        logging.error(f"Error while fetching org_id and premise_id for asset_id {asset_id}: {e}")
        return None, None
    
def get_rate_for_response_time(cursor, response_time_str, asset_id):
    """
    Retrieve the applicable rate based on response_time for a given asset_id.
    response_time is the local time a response is recieved from the plug.
    """
    # Convert response_time_str to a datetime object and then to a time object
    response_time = datetime.strptime(response_time_str, '%Y-%m-%d %H:%M:%S').time()

    # Get the premise_id from asset_info
    cursor.execute('''
        SELECT premise_id
        FROM asset_info
        WHERE asset_id = ?
    ''', (asset_id,))
    premise_id = cursor.fetchone()[0]
    #logging.info(f"Premise id for {asset_id} is {premise_id}")

    # Get the supplier name and plan name from prem_info
    cursor.execute('''
        SELECT supplier_name, supplier_plan_name
        FROM prem_info
        WHERE premise_id = ?
    ''', (premise_id,))
    prem_info = cursor.fetchone()
    supplier_name, supplier_plan_name = prem_info
    #logging.info(f"The supplier for {asset_id} is {supplier_name} and the plan is: {supplier_plan_name}")

    # Query to get the rate applicable for the given response_time
    query = """
    SELECT rate_start, rate_end, rate
    FROM energy_rates
    WHERE supplier_name = ? AND supplier_plan_name = ?
    """
    cursor.execute(query, (supplier_name, supplier_plan_name))
    rates = cursor.fetchall()
    #logging.info(f"rates = {rates}")

    applicable_rate = None

    for rate_start_str, rate_end_str, rate in rates:
        rate_start = datetime.strptime(rate_start_str, '%H:%M:%S').time()
        rate_end = datetime.strptime(rate_end_str, '%H:%M:%S').time()

        if rate_end < rate_start:
            # Handle case where rate period spans midnight
            if response_time >= rate_start or response_time < rate_end:
                applicable_rate = rate
                #logging.info(f"Debugging: For asset ID: {asset_id} - Midnight case: The applicable rate is: {applicable_rate}")
                break
        else:
            # Normal case where rate period does not span midnight
            if rate_start <= response_time <= rate_end:
                applicable_rate = rate
                #logging.info(f"Debugging: For asset ID: {asset_id} - Normal case: The applicable rate is: {applicable_rate}")
                break

    if applicable_rate is None:
        logging.warning(f"No matching rate found for asset_id {asset_id} at {response_time_str}")

    return applicable_rate if applicable_rate else 0

def compare_asset_with_benchmark(cursor, asset_id, current_data):
    # If running between 00:00 and 00:59, adjust the date and hour to handle the previous day
    if current_data['hour'] == 0:
        current_data['date'] = (current_data['date'] - timedelta(days=1)).strftime('%Y-%m-%d')
        current_data['hour'] = f"{str(23).zfill(2)}:00"  # Only the 23rd hour of the previous day is valid

    # Query to get benchmark entries for the given asset_id, day_of_week, and hour
    query = '''
    SELECT total_kwh, total_kwh_co2e, total_kwh_charge
    FROM daily_usage
    WHERE asset_id = ? AND is_benchmark = 1 AND day_of_week = ? AND hour = ?
    '''
    cursor.execute(query, (asset_id, current_data['day_of_week'], current_data['hour']))
    benchmark_entry = cursor.fetchone()

    # If there are no benchmark entries, return None
    if not benchmark_entry:
        logging.info(f"No benchmark entries for asset_id {asset_id}, skipping comparison.")
        return None

    # Unpack and convert benchmark values
    benchmark_values = list(map(ensure_real, benchmark_entry))
    if None in benchmark_values:
        logging.error(f"Skipping entry due to conversion error: {benchmark_entry}")
        return None

    # Unpack benchmark values
    (benchmark_total_kwh, benchmark_total_kwh_co2e, benchmark_total_kwh_charge) = benchmark_values

    # Compute deltas
    ## Usage: positive result means more usage, negative result means less usage
    ## CO2e (Offset): Negative result means less emissions, positve result means more emissions
    ## Charge (Spend): Positive result means more savings, negative result means less savings
    total_kwh_delta = current_data['total_kwh'] - benchmark_total_kwh
    total_kwh_charge_delta = benchmark_total_kwh_charge - current_data['total_kwh_charge']
    total_kwh_co2e_delta = current_data['total_kwh_co2e'] - benchmark_total_kwh_co2e
    logging.info(f"total_kwh_delta: {current_data['total_kwh']} - {benchmark_total_kwh} = {total_kwh_delta}")
    logging.info(f"total_kwh_co2e_delta: {current_data['total_kwh_co2e']} - {benchmark_total_kwh_co2e}  = {total_kwh_co2e_delta}")
    logging.info(f"total_kwh_charge_delta: {benchmark_total_kwh_charge} - {current_data['total_kwh_charge']} = {total_kwh_charge_delta}")

    total_kwh_delta_percent = calculate_benchmark_percentage(total_kwh_delta, benchmark_total_kwh)
    total_kwh_charge_delta_percent = calculate_benchmark_percentage(total_kwh_charge_delta, benchmark_total_kwh_charge)
    total_kwh_co2e_delta_percent = calculate_benchmark_percentage(total_kwh_co2e_delta, benchmark_total_kwh_co2e)
    logging.info(f"total_kwh_delta_percent: {total_kwh_delta} / {benchmark_total_kwh} = {total_kwh_delta_percent}")
    logging.info(f"total_kwh_charge_delta_percent: {total_kwh_charge_delta} / {benchmark_total_kwh_charge} = {total_kwh_charge_delta_percent}")
    logging.info(f"total_kwh_co2e_delta_percent: {total_kwh_co2e_delta} / {benchmark_total_kwh_co2e}  = {total_kwh_co2e_delta_percent}")

    # Log comparison results
    logging.info(f"Comparison results for asset_id {asset_id}: "
                 f"{total_kwh_delta} kWh, {total_kwh_co2e_delta} CO2e, {total_kwh_charge_delta} charge, "
                 f"{total_kwh_delta_percent}% delta in kWh, "
                 f"{total_kwh_co2e_delta_percent}% delta in CO2e, "
                 f"{total_kwh_charge_delta_percent}% delta in charge.")

    # Return all the delta results
    return {
        'total_kwh_delta': total_kwh_delta,
        'total_kwh_charge_delta': total_kwh_charge_delta,
        'total_kwh_co2e_delta': total_kwh_co2e_delta,
        'total_kwh_delta_percent': total_kwh_delta_percent,
        'total_kwh_charge_delta_percent': total_kwh_charge_delta_percent,
        'total_kwh_co2e_delta_percent': total_kwh_co2e_delta_percent,
    }

def compare_daily_with_benchmark(cursor, current_data):
    """
    Computes daily metrics and their deltas against benchmark values for a specific date and hour.
    Note: This function does not rely on `asset_id` as all daily values are the same for a date/hour.

    Args:
        cursor: Database cursor for executing queries.
        current_data: A dictionary containing 'date', 'hour', 'day_of_week',
                      'daily_total_kwh', 'daily_total_kwh_co2e', and 'daily_total_kwh_charge'.

    Returns:
        A dictionary containing the daily metrics and deltas, or None if benchmark data is not found.
    """
    # Adjust date and hour for the previous day if running during the first hour of the day
    if current_data['hour'] == 0:
        current_data['date'] = (current_data['date'] - timedelta(days=1)).strftime('%Y-%m-%d')
        current_data['hour'] = f"{str(23).zfill(2)}:00"

    # Query to get benchmark values (no longer relying on asset_id)
    query = '''
    SELECT daily_total_kwh, daily_total_kwh_co2e, daily_total_kwh_charge
    FROM daily_usage
    WHERE is_benchmark = 1 AND day_of_week = ? AND hour = ?
    '''
    cursor.execute(query, (current_data['day_of_week'], current_data['hour']))
    benchmark_entry = cursor.fetchone()

    if not benchmark_entry:
        logging.info("No benchmark entries found for the specified day_of_week and hour, skipping delta computation.")
        return None

    # Process benchmark values
    benchmark_values = list(map(ensure_real, benchmark_entry))
    if None in benchmark_values:
        logging.error(f"Skipping due to invalid benchmark values: {benchmark_entry}")
        return None

    benchmark_daily_total_kwh, benchmark_daily_total_kwh_co2e, benchmark_daily_total_kwh_charge = benchmark_values

    # Compute deltas
    ## Usage: positive result means more usage, negative result means less usage
    ## CO2e (Offset): Negative result means less emissions, positve result means more emissions
    ## Charge (Spend): Positive result means more savings, negative result means less savings
    daily_total_kwh_delta = current_data['daily_total_kwh'] - benchmark_daily_total_kwh
    daily_total_kwh_charge_delta = benchmark_daily_total_kwh_charge - current_data['daily_total_kwh_charge']
    daily_total_kwh_co2e_delta = benchmark_daily_total_kwh_co2e - current_data['daily_total_kwh_co2e']

    daily_total_kwh_delta_percent = calculate_benchmark_percentage(daily_total_kwh_delta, benchmark_daily_total_kwh)
    daily_total_kwh_charge_delta_percent = calculate_benchmark_percentage(daily_total_kwh_charge_delta, benchmark_daily_total_kwh_charge)
    daily_total_kwh_co2e_delta_percent = calculate_benchmark_percentage(daily_total_kwh_co2e_delta, benchmark_daily_total_kwh_co2e)


    # Log computation results
    logging.info(f"Computed daily metrics are: "
                 f"KWH Delta: {daily_total_kwh_delta}, CO2e Delta: {daily_total_kwh_co2e_delta}, "
                 f"Charge Delta: {daily_total_kwh_charge_delta}, "
                 f"KWH Delta %: {daily_total_kwh_delta_percent}, "
                 f"CO2e Delta %: {daily_total_kwh_co2e_delta_percent}, "
                 f"Charge Delta %: {daily_total_kwh_charge_delta_percent}.")

    # Return results as a dictionary
    return {
        'daily_total_kwh_delta': daily_total_kwh_delta,
        'daily_total_kwh_co2e_delta': daily_total_kwh_co2e_delta,
        'daily_total_kwh_charge_delta': daily_total_kwh_charge_delta,
        'daily_total_kwh_delta_percent': daily_total_kwh_delta_percent,
        'daily_total_kwh_co2e_delta_percent': daily_total_kwh_co2e_delta_percent,
        'daily_total_kwh_charge_delta_percent': daily_total_kwh_charge_delta_percent,
    }
#def get_missing_hours(cursor, static_date=None, static_hour=None):
## To specify date and hour to run script, comment above and update static_date, static_hour and uncomment below.
def get_missing_hours(cursor, static_date="2025-12-31", static_hour="00"):
    try:
        # Use static values if provided, otherwise calculate dynamically
        if static_date is not None and static_hour is not None:
            current_date = static_date
            current_hour = int(static_hour)
        else:
            now = datetime.now()
            current_date = now.strftime('%Y-%m-%d')
            current_hour = now.hour

        # Determine valid hours
        if current_hour == 0:
            # Midnight case: process the previous day's 23:00
            previous_date = (datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            valid_hours = {23}
        else:
            # Normal case: valid hours from 00:00 to the previous hour of the current day
            valid_hours = set(range(current_hour))

        # Query to get hours for the current day from the `daily_usage` table
        query = '''
        SELECT hour
        FROM daily_usage
        WHERE date = ?
        '''
        target_date = previous_date if current_hour == 0 else current_date
        cursor.execute(query, (target_date,))

        recorded_hours = set()
        for row in cursor.fetchall():
            hour_value = row[0]  # Fetch hour as string
            try:
                # Handle string format (e.g., HH:MM) or direct integer values
                if isinstance(hour_value, str) and ':' in hour_value:
                    hour = int(hour_value.split(':')[0])  # Extract hour from HH:MM format
                else:
                    hour = int(hour_value)  # Convert directly to int if numeric
                recorded_hours.add(hour)
            except ValueError as e:
                logging.error(f"Invalid hour format in the database: {hour_value}")

        # Calculate missing hours by subtracting recorded hours from valid hours
        missing_hours = sorted(valid_hours - recorded_hours)
        logging.info(f"Valid hours: {valid_hours}")
        logging.info(f"Recorded hours: {recorded_hours}")
        if not missing_hours:
            logging.info("No missing hours detected.")
        else:
            logging.info(f"Missing hours: {missing_hours}")

        # Return missing hours and the corresponding date
        return missing_hours, target_date

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    return [], None  # Return empty list and None for the date in case of error

def calculate_daily_consumption_by_asset(db_file):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row  # Enable named access
    cursor = conn.cursor()

    try:
        # Get missing hours and associated data
        missing_hours, current_date = get_missing_hours(cursor)

        if missing_hours:
            for current_hour in missing_hours:
                # Process metrics for current_hour
                logging.info(f"Getting records for all assets for the day: {current_date}")
                daily_asset_records = get_asset_records_for_day (cursor, current_date)
                #formatted_hour = f"{str(current_hour).zfill(2)}:00"
                #logging.info(f"Processing metrics for missing hour: {formatted_hour}")
                process_metrics_for_hour(conn, cursor, daily_asset_records, current_hour, current_date)  # Pass current_hour and current_date directly
            
            # When done processing missing hours, set current_hour to current hour.
            current_hour = datetime.now().hour
            logging.info(f"Completed processing all missing hours. Setting current hour to: {current_hour}")
        
        else:
            # If no missing hours where discovered (ie The above if statememnt was not invoked), 
            current_hour = datetime.now().hour
            logging.info(f"No missing hours detected. Setting current hour to: {current_hour}")
            
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        conn.commit()  # Ensure data is saved
        conn.close()  # Always close the connection after the process

def get_asset_records_for_day (cursor, current_date):
    try:
        # Convert current_date_str to a datetime object
        current_date_obj = datetime.strptime(current_date, '%Y-%m-%d')
        end_of_day = current_date_obj + timedelta(days=1) # Calculate end_of_day as datetime object
        end_of_day_str = end_of_day.strftime('%Y-%m-%d %H:%M:%S') # Convert to string
        
        # get all records from tasmota_energy_data from the beginning and end of the day
        # (essentially until now)
        query = """
        SELECT asset_id, asset_name, power, response_time
        FROM tasmota_energy_data
        WHERE response_time >= ? AND response_time < ?
        ORDER BY response_time
        """
        cursor.execute(query, (current_date, end_of_day_str))
        daily_asset_records = cursor.fetchall()

        if not daily_asset_records:
            logging.warning("No data found for the current day")
            return

        #logging.info(f"Fetched {len(daily_asset_records)} records for processing")

        return daily_asset_records
    
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

def process_metrics_for_hour(conn, cursor, daily_asset_records, current_hour, current_date):
    try:
        asset_data = {}
        previous_power = {}
        compressor_start_times = {}
        compressor_runtimes = []
        total_kwh_charges = {}
        daily_total_kwh = 0.0

        first_response_time_current_hour = {}
        last_response_time_current_hour = {}

        logging.info(f"Debugging: Processing records for: {current_date}")
        current_hour_str = f"{current_hour:02d}:00"
        logging.info(f"Debugging: Processing {len(daily_asset_records)} records for: {current_hour_str}")

        for row in daily_asset_records:
            # Extract the four values for every row in the dB derived from the query above
            asset_id, asset_name, power, response_time_str = row
            # Convert timestamp in the format '%Y-%m-%d %H:%M:%S' (e.g., '2024-09-29 10:15:00')
            # to datetime format (%Y-%m-%d %H:%M:%S) eg. 2024-09-29-10-15-00 
            response_time = datetime.strptime(response_time_str, '%Y-%m-%d %H:%M:%S')
            # Make response time as string again and extract day of the week.
            day_of_week = response_time.strftime('%A')  # Returns the full weekday name, e.g., 'Monday'

            # Check is asset_id existing in our result array. If not, this means it is
            # the 1st time we are processing the data for this asset_id in the day.
            if asset_id not in asset_data:
                # Initialise the data for this new asset_id
                asset_data[asset_id] = {
                    'total_kwh': 0.0,
                    'cnt_comp_on': 0,
                    'cnt_comp_off': 0,
                    'total_comp_runtime': 0,
                    'asset_name': asset_name,
                    'current_hour_kwh': 0.00,  # Initialize per-asset current_hour_kwh
                    'last_processed_hour': -1,  # Track the last processed hour
                    'compressor_runtimes': [],
                    'response_time_count': 0,  # Initialize the count of response times
                    'daily_total_kwh': 0,  # daily_total_kwh starts at 0 for new day
                    'last_date': current_date,  # Track the last date this asset was updated
                    'last_hour': None  # Track the last hour this asset was updated
                }
                logging.warning(f"Initializing data for asset_id: {asset_id}")
                
                # Set initial values to calculate compressor state:
                previous_power[asset_id] = power # first power reading is to also equal previous_power
                compressor_start_times[asset_id] = None # Record compressor state as off
                total_kwh_charges[asset_id] = 0.0 # initialise kwh charges to start from 0.

                #last_response_time_current_hour[asset_id] = response_time
                #logging.info(f"Debugging: Initializing: The last_response_time_current_hour for {asset_id} based on response_time is: {response_time}")

            # Assume 4 measurements per minute, and calculate kWh per measurement
            interval_seconds = 60 / 4
            kwh = (power / 1000) * (interval_seconds / 3600)

            # Detect if we are in a new hour
            last_date_hour_key = (asset_id, current_date, asset_data[asset_id]['last_processed_hour'])
            current_hour_key = (asset_id, current_date, current_hour)
            #logging.info(f"Last Date Hour Key: {last_date_hour_key}")
            #logging.info(f"Current Hour Key: {current_hour_key}")

            if last_date_hour_key != current_hour_key:
                # New hour detected for the same date or a new day
                logging.info(f"New hour detected for asset {asset_id}. Current hour is: {current_hour_str} for {current_date}. Resetting current_hour_kwh.")
                # Reset for new hour
                asset_data[asset_id]['current_hour_kwh'] = 0.0
                asset_data[asset_id]['last_processed_hour'] = current_hour
                #logging.info(f"Debugging: Date/ResponseTime != Current_date/time for Asset ID: {asset_id}. Resetting for new hour. The current hour value is: {current_hour_str}")

                # Set the first response time for the new hour
                asset_data[asset_id]['response_time_count'] = 1  # Initialize count for the new hour
                #logging.info(f"Debugging: First response time set for asset {asset_id} at {response_time}. Reset response_time_count to 1.")
            else:
                # Still in the same hour, increment response time count
                asset_data[asset_id]['response_time_count'] += 1           

            # Ensure response_time is a datetime object, and current_date is a date object
            if isinstance(response_time, str):
                response_time = datetime.strptime(response_time, '%Y-%m-%d %H:%M:%S')
            if isinstance(current_date, str):
                current_date = datetime.strptime(current_date, '%Y-%m-%d').date()

            if response_time.date() == current_date and current_hour == response_time.hour: # If the date and hour in the response_time field of the record being processed = the current_date and current_hour value
                #logging.info(f"################# Asset ID {asset_id} - Hour {current_hour}: Current hour kWh = {asset_data[asset_id]['current_hour_kwh']}")
                asset_data[asset_id]['current_hour_kwh'] += kwh # Add kwh to usage
                #logging.info(f"Date/ResponseTime = Current_date/time for Asset ID: {asset_id}")
                
                # Now record first and the last response time for the current hour
                if asset_id not in first_response_time_current_hour:
                    first_response_time_current_hour[asset_id] = response_time  # Reset first response time
                last_response_time_current_hour[asset_id] = response_time
                #logging.info(f"Debugging: Date/ResponseTime = Current_date/time for asset {asset_id}, First Response Time for current hour: {first_response_time_current_hour[asset_id]}, Last Response Time for current hour: {last_response_time_current_hour[asset_id]}, Response Time Count: {asset_data[asset_id]['response_time_count']}")

            # Cummulate total_kwh per asset for current_time per hour.
            # Check if the date has changed (new day)
            if asset_data[asset_id]['last_date'] != current_date:
                #logging.info(f"last date != current_date")
                # Reset total_kwh and daily_total_kwh for a new day
                asset_data[asset_id]['total_kwh'] = 0.0
                asset_data[asset_id]['daily_total_kwh'] = 0.0
                asset_data[asset_id]['last_date'] = current_date  # Update to new date
                asset_data[asset_id]['last_hour'] = None  # Reset hour tracking for new day

            # Get the last hour this asset was updated
            last_hour = asset_data[asset_id]['last_hour']
            #logging.info(f"{last_hour}")

            if last_hour is None:  # First hour of the day
                # First hour of the day: set total_kwh to the current hour's kWh
                #logging.info(f"last hour = None")
                asset_data[asset_id]['total_kwh'] = kwh
            else:
                #logging.info(f"last hour != None")
                # For subsequent hours, add the current hour's kWh to the total_kwh from the previous hour
                asset_data[asset_id]['total_kwh'] += kwh
                #logging.info(f"Debugging: For asset ID: {asset_id} - Total kWh: {asset_data[asset_id]['total_kwh']}")

            # Fetch the last saved total_kwh for the asset for the previous hour on the same day
            cursor.execute('''
                SELECT total_kwh FROM daily_usage 
                WHERE asset_id = ? AND date = ? AND hour < ? 
                ORDER BY hour DESC LIMIT 1
            ''', (asset_id, current_date, current_hour_str))

            previous_kwh_record = cursor.fetchone()
            #logging.debug(f"############## Database query result for previous_kwh_record: {previous_kwh_record}")

            # Initialize total_kwh based on previous records or start fresh if no record exists
            if previous_kwh_record:
                previous_total_kwh = previous_kwh_record['total_kwh']
                #logging.info(f"Previous record exists: Total kWh: {previous_total_kwh}")
            else:
                previous_total_kwh = 0.0
                #logging.info(f"No previous record found for asset {asset_id} on date {current_date} before hour {current_hour}")

            # Accumulate the current hour kWh to total_kwh
            #logging.info(f"################# Calculating total_kwh: Previous total_kwh = {previous_total_kwh}, Current hour kWh = {asset_data[asset_id]['current_hour_kwh']}")
            total_kwh = previous_total_kwh + asset_data[asset_id]['current_hour_kwh'] 
            # Update the total_kwh in asset_data
            asset_data[asset_id]['total_kwh'] = total_kwh
            # Update the last hour this asset was updated to the current hour
            asset_data[asset_id]['last_hour'] = current_hour
            #logging.info(f"The last hour = current hour: {current_hour} for {asset_id}")

            # Log or store the total_kwh  as needed
            #logging.info(f"Asset ID {asset_id} - Hour {current_hour}: Total kWh = {asset_data[asset_id]['total_kwh']}")

            #logging.info(f"Debugging: Asset Id: {asset_id} previous power: {previous_power[asset_id]}")
            #logging.info(f"Debugging: Asset Id: {asset_id} power: {power}")
            # Detect compressor ON transition
            if previous_power[asset_id] < 100 and power >= 100:
                asset_data[asset_id]['cnt_comp_on'] += 1
                compressor_start_times[asset_id] = response_time  # Record compressor start time
                #logging.info(f"Compressor ON for asset {asset_id} at {response_time}")

            # Detect compressor OFF transition
            elif previous_power[asset_id] >= 100 and power < 100:
                asset_data[asset_id]['cnt_comp_off'] += 1
                #logging.info(f"Compressor OFF for asset {asset_id} at {response_time}")
                if compressor_start_times[asset_id]:
                    comp_runtime = (response_time - compressor_start_times[asset_id]).total_seconds() / 60.0
                    asset_data[asset_id]['total_comp_runtime'] += comp_runtime
                    asset_data[asset_id]['compressor_runtimes'].append(comp_runtime)
                    #logging.info(f"Compressor runtime for asset {asset_id}: {comp_runtime} minutes")
                    compressor_start_times[asset_id] = None  # Reset start time after calculating runtime

            # Update previous power state to current for the next iteration
            previous_power[asset_id] = power

            # Look up the rate based on response_time
            rate = get_rate_for_response_time(cursor, response_time_str, asset_id)
            kwh_charge = kwh * rate
            total_kwh_charges[asset_id] += kwh_charge
        
        for asset_id in asset_data.keys():
            if asset_id in first_response_time_current_hour:
                logging.info(f"Final Asset ID: {asset_id}, First Response Time for current hour: {first_response_time_current_hour[asset_id]}, Last Response Time for current hour: {last_response_time_current_hour[asset_id]}, Response Time Count: {asset_data[asset_id]['response_time_count']}")
            
        day_of_week = response_time.strftime('%A')

        for asset_id, data in asset_data.items():
            total_kwh = data['total_kwh']
            asset_name = data['asset_name']
            cnt_comp_on = data['cnt_comp_on']
            cnt_comp_off = data['cnt_comp_off']
            total_comp_runtime = data['total_comp_runtime']

            if cnt_comp_on > 0:
                ave_comp_runtime = total_comp_runtime / cnt_comp_on
                ave_comp_runtime_str = format_runtime(ave_comp_runtime)

                # Use the asset-specific compressor runtimes list for max/min calculations
                compressor_runtimes = data['compressor_runtimes']
                max_comp_runtime = max(compressor_runtimes) if compressor_runtimes else 0
                max_comp_runtime_str = format_runtime(max_comp_runtime)
                min_comp_runtime = min(compressor_runtimes) if compressor_runtimes else 0
                min_comp_runtime_str = format_runtime(min_comp_runtime)
            else:
                ave_comp_runtime_str = max_comp_runtime_str = min_comp_runtime_str = "00:00"

            total_kwh_charge = total_kwh_charges.get(asset_id, 0.0)

            if isinstance(response_time, str):
                response_time = datetime.strptime(response_time, '%Y-%m-%d %H:%M:%S')
            
            # Define current_time_str for logging or other purposes
            current_time_str = response_time.strftime('%Y-%m-%d %H:%M:%S')
            #logging.info(f"Current time in string format: {current_time_str}")
            hour = f"{current_hour:02d}:00"
            yesterday_date = (response_time - timedelta(days=1)).strftime('%Y-%m-%d')
        
            # Fetch yesterday's kWh for the same hour
            cursor.execute('''
                SELECT total_kwh FROM daily_usage WHERE asset_id = ? AND date = ? AND hour = ?
            ''', (asset_id, yesterday_date, hour))
            # set yesterday_record to be the 1st record in the same hour yesterday using fetchone() function.
            yesterday_record = cursor.fetchone()
            # assign if value exists. Other value of 0.0 is assigned.
            yesterday_kwh = yesterday_record[0] if yesterday_record else 0.0
            #logging.info(f"Asset ID: {asset_id} yesterday total_kwh usage: {yesterday_kwh}")
            # Calculate percentage change_kwh. Again. total_kwh reflects that cummulative kwh usage
            # for an asset for the current day.
            # yesterday_kwh value is retrieved from the db, by looking for the 1st record for the same hour
            # yesterday (refer above).
            percentage_change_kwh = calculate_percentage_change_kwh(total_kwh, yesterday_kwh)
            
            # Retrieve current_hour_kwh for this asset
            asset_current_hour_kwh = asset_data[asset_id]['current_hour_kwh']
            logging.info(f"{asset_id}: for hour {current_hour}: The current_hour_kwh is: {asset_current_hour_kwh}")


            #logging.info(f"{asset_id}: Calculating CO2 emissions for total_kwh: {total_kwh}, current_hour_kwh: {asset_current_hour_kwh}")
    
            total_kwh_co2e = calculate_co2e_emission(total_kwh)
            current_hour_kwh_co2e = calculate_co2e_emission(asset_current_hour_kwh)
            
            # Prepare data to pass to asset_benchmark delta function
            current_data = {
                'day_of_week': day_of_week,
                'hour': f"{str(current_hour).zfill(2)}:00",  # Use current_hour directly
                'date': current_date,
                'total_kwh': total_kwh,
                'total_kwh_co2e': total_kwh_co2e,
                'total_kwh_charge': total_kwh_charge
            }
            # Run the function and calculate values
            comparison_results = compare_asset_with_benchmark(cursor, asset_id, current_data)

            # If comparison results exist, extract the delta values
            if comparison_results:
                total_kwh_delta = comparison_results['total_kwh_delta']
                total_kwh_charge_delta = comparison_results['total_kwh_charge_delta']
                total_kwh_co2e_delta = comparison_results['total_kwh_co2e_delta']

                total_kwh_delta_percent = comparison_results['total_kwh_delta_percent']
                total_kwh_charge_delta_percent = comparison_results['total_kwh_charge_delta_percent']
                total_kwh_co2e_delta_percent = comparison_results['total_kwh_co2e_delta_percent']

                logging.info(
                    f"Comparison results: {total_kwh_delta}, {total_kwh_charge_delta}, {total_kwh_co2e_delta}, "
                    f"{total_kwh_delta_percent}, {total_kwh_charge_delta_percent}, {total_kwh_co2e_delta_percent}"                )
            else:
                # If no comparison results, set all deltas and percentages to 0
                total_kwh_delta = total_kwh_charge_delta = total_kwh_co2e_delta = 0
                total_kwh_delta_percent = total_kwh_charge_delta_percent = total_kwh_co2e_delta_percent = 0
                logging.info("No benchmark entries found, using default values for deltas and percentages.")

            #logging.info(f"Current hour kWh for {asset_id}: {asset_current_hour_kwh}")
            #logging.info(f"total_kwh_co2e: {total_kwh_co2e} {'grams' if total_kwh_co2e < 500 else 'tonnes'}")
            #logging.info(f"current_hour_kwh_co2e: {current_hour_kwh_co2e} {'grams' if current_hour_kwh_co2e < 500 else 'tonnes'}")

            # Log the value to be written to the database
            logging.info(f"################# Writing to DB: Asset ID {asset_id} - Hour {current_hour}: Total kWh = {total_kwh}")


            org_id, premise_id = get_org_id_and_premise_id_for_asset(cursor, asset_id)

            # Insert or update the record in daily_usage (consolidated with daily saving entries)
            cursor.execute('''
                INSERT INTO daily_usage (
                    asset_id, org_id, premise_id, asset_name, date, total_kwh, cnt_comp_on, cnt_comp_off, ave_comp_runtime, 
                    max_comp_runtime, min_comp_runtime, update_time, total_kwh_charge, hour, 
                    percentage_change_kwh, current_hour_kwh, total_kwh_co2e, 
                    current_hour_kwh_co2e, day_of_week,
                    total_kwh_delta, total_kwh_charge_delta, total_kwh_co2e_delta,
                    total_kwh_delta_percent, total_kwh_charge_delta_percent, total_kwh_co2e_delta_percent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id, date, hour) DO UPDATE SET
                    org_id = excluded.org_id,
                    premise_id = excluded.premise_id,
                    total_kwh = excluded.total_kwh,
                    cnt_comp_on = excluded.cnt_comp_on,
                    cnt_comp_off = excluded.cnt_comp_off,
                    ave_comp_runtime = excluded.ave_comp_runtime,
                    max_comp_runtime = excluded.max_comp_runtime,
                    min_comp_runtime = excluded.min_comp_runtime,
                    update_time = excluded.update_time,
                    total_kwh_charge = excluded.total_kwh_charge,
                    percentage_change_kwh = excluded.percentage_change_kwh,
                    current_hour_kwh = excluded.current_hour_kwh,
                    total_kwh_co2e = excluded.total_kwh_co2e,
                    current_hour_kwh_co2e = excluded.current_hour_kwh_co2e,
                    day_of_week = excluded.day_of_week,
                    total_kwh_delta = excluded.total_kwh_delta,
                    total_kwh_charge_delta = excluded.total_kwh_charge_delta,
                    total_kwh_co2e_delta = excluded.total_kwh_co2e_delta,
                    total_kwh_delta_percent = excluded.total_kwh_delta_percent,
                    total_kwh_co2e_delta_percent = excluded.total_kwh_co2e_delta_percent,
                    total_kwh_charge_delta_percent = excluded.total_kwh_charge_delta_percent
            ''', (
                asset_id, org_id, premise_id, asset_name, current_date, 
                round(total_kwh, 2), cnt_comp_on, cnt_comp_off, 
                ave_comp_runtime_str, max_comp_runtime_str, min_comp_runtime_str, 
                current_time_str, round(total_kwh_charge, 2), hour, 
                round(percentage_change_kwh, 2),
                round(asset_current_hour_kwh, 3), total_kwh_co2e, 
                current_hour_kwh_co2e, 
                day_of_week,
                round(total_kwh_delta, 3), round(total_kwh_charge_delta, 3),
                round(total_kwh_co2e_delta, 3),
                round(total_kwh_delta_percent, 2), round(total_kwh_charge_delta_percent, 2),
                round(total_kwh_co2e_delta_percent, 2)
            ))
            conn.commit()

        logging.info("Daily consumption and benchmark stats updated successfully.")

        # Calculate daily metric values for the current date and hour
        cursor.execute('''
            SELECT 
                SUM(total_kwh) AS daily_total_kwh, 
                SUM(total_kwh_co2e) AS daily_total_kwh_co2e, 
                SUM(total_kwh_charge) AS daily_total_kwh_charge
            FROM daily_usage
            WHERE date = ? AND hour = ?
        ''', (current_date, f"{str(current_hour).zfill(2)}:00"))

        # Fetch results
        results = cursor.fetchone()
        if results:
            daily_total_kwh = results['daily_total_kwh']
            daily_total_kwh_co2e = results['daily_total_kwh_co2e']
            daily_total_kwh_charge = results['daily_total_kwh_charge']

            # Prepare data to pass to daily_benchmark delta function
            current_daily_data = {
                'day_of_week': day_of_week,
                'hour': f"{str(current_hour).zfill(2)}:00",  # Use current_hour directly
                'date': current_date,
                'daily_total_kwh': daily_total_kwh,
                'daily_total_kwh_co2e': daily_total_kwh_co2e,
                'daily_total_kwh_charge': daily_total_kwh_charge
            }

            comparison_daily_results = compare_daily_with_benchmark(cursor, current_daily_data)

            # If comparison results exist, extract the delta values
            if comparison_daily_results:
                daily_total_kwh_delta = comparison_daily_results['daily_total_kwh_delta']
                daily_total_kwh_charge_delta = comparison_daily_results['daily_total_kwh_charge_delta']
                daily_total_kwh_co2e_delta = comparison_daily_results['daily_total_kwh_co2e_delta']

                daily_total_kwh_delta_percent = comparison_daily_results['daily_total_kwh_delta_percent']
                daily_total_kwh_charge_delta_percent = comparison_daily_results['daily_total_kwh_charge_delta_percent']
                daily_total_kwh_co2e_delta_percent = comparison_daily_results['daily_total_kwh_co2e_delta_percent']

                logging.info(
                    f"Comparison results: {daily_total_kwh_delta}, {daily_total_kwh_charge_delta}, {daily_total_kwh_co2e_delta}, "
                    f"{daily_total_kwh_delta_percent}, {daily_total_kwh_charge_delta_percent}, {daily_total_kwh_co2e_delta_percent}"                )
            else:
                # If no comparison results, set all deltas and percentages to 0
                daily_total_kwh_delta = daily_total_kwh_charge_delta = daily_total_kwh_co2e_delta = 0
                daily_total_kwh_delta_percent = daily_total_kwh_charge_delta_percent = daily_total_kwh_co2e_delta_percent = 0
                logging.info("No benchmark entries found, using default values for deltas and percentages.")

            # Update the `daily_usage` table with the calculated values
            logging.info(f"Current data for update: date={current_data['date']}, hour={current_data['hour']}")
            cursor.execute('''
                UPDATE daily_usage
                SET 
                    daily_total_kwh = ?,
                    daily_total_kwh_co2e = ?,
                    daily_total_kwh_charge = ?,
                    daily_total_kwh_delta = ?,
                    daily_total_kwh_charge_delta = ?,
                    daily_total_kwh_co2e_delta = ?,
                    daily_total_kwh_delta_percent = ?,
                    daily_total_kwh_charge_delta_percent = ?,
                    daily_total_kwh_co2e_delta_percent = ?
                WHERE date = ? AND hour = ?
            ''', (
                round(daily_total_kwh, 3),
                round(daily_total_kwh_co2e, 3),
                round(daily_total_kwh_charge, 3),
                round(daily_total_kwh_delta, 3),
                round(daily_total_kwh_charge_delta, 3),
                round(daily_total_kwh_co2e_delta, 3),
                round(daily_total_kwh_delta_percent, 2),
                round(daily_total_kwh_charge_delta_percent, 2),
                round(daily_total_kwh_co2e_delta_percent, 2),
                current_data['date'],
                current_data['hour']
            ))

            # Log the number of rows updated
            rows_updated = cursor.rowcount
            logging.info(f"Rows updated: {rows_updated}")

            # Commit the changes to the database
            conn.commit()
           
            if rows_updated == 0:
                logging.warning(f"No rows were updated for date={current_data['date']} and hour={current_data['hour']}. Check input values.")

            logging.info(f"Updated daily_usage table for {current_data['date']} at {current_data['hour']} with daily totals and deltas.")

    except Exception as e:
        logging.error(f"Query execution failed for {asset_id}: {str(e)}")
        raise
    #finally:
        #conn.close()

def main():

    # Run the function directly for testing
    calculate_daily_consumption_by_asset(db_file)
    #get_missing_hours(db_file)

    # Schedule the task to run at the beginning of every hour
    schedule.every().hour.at(":00").do(calculate_daily_consumption_by_asset, db_file=db_file)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()