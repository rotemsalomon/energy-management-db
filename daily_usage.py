import sqlite3
from datetime import datetime, timedelta
import schedule
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

    # Get the supplier name and plan name from prem_info
    cursor.execute('''
        SELECT supplier_name, supplier_plan_name
        FROM prem_info
        WHERE premise_id = ?
    ''', (premise_id,))
    prem_info = cursor.fetchone()
    supplier_name, supplier_plan_name = prem_info

    # Query to get the rate applicable for the given response_time
    query = """
    SELECT rate_start, rate_end, rate
    FROM energy_rates
    WHERE supplier_name = ? AND supplier_plan_name = ?
    """
    cursor.execute(query, (supplier_name, supplier_plan_name))
    rates = cursor.fetchall()

    applicable_rate = None

    for rate_start_str, rate_end_str, rate in rates:
        rate_start = datetime.strptime(rate_start_str, '%H:%M:%S').time()
        rate_end = datetime.strptime(rate_end_str, '%H:%M:%S').time()

        if rate_end < rate_start:
            # Handle case where rate period spans midnight
            if response_time >= rate_start or response_time < rate_end:
                applicable_rate = rate
                break
        else:
            # Normal case where rate period does not span midnight
            if rate_start <= response_time < rate_end:
                applicable_rate = rate
                break

    if applicable_rate is None:
        logging.warning(f"No matching rate found for asset_id {asset_id} at {response_time_str}")

    return applicable_rate if applicable_rate else 0

def compare_with_benchmark(cursor, asset_id, current_data):
    # Query to get benchmark entries for the given asset_id, day_of_week, and hour
    query = '''
    SELECT total_kwh, total_kwh_co2e, total_kwh_charge FROM daily_usage
    WHERE asset_id = ? AND is_benchmark = 1 AND day_of_week = ? AND hour = ?
    '''
    cursor.execute(query, (asset_id, current_data['day_of_week'], current_data['hour']))
    benchmark_entries = cursor.fetchall()

    # Prepare default values for reductions
    total_kwh_reduction = 0
    total_kwh_charge_reduction = 0
    total_kwh_co2e_reduction = 0
    #logging.info(total_kwh_reduction)
    #logging.info(total_kwh_charge_reduction)
    #logging.info(total_kwh_co2e_reduction)
    if not benchmark_entries:
        logging.info(f"No benchmark entries for asset_id {asset_id}, skipping comparison.")
        return {
            'total_kwh_reduction': total_kwh_reduction,
            'total_kwh_charge_reduction': total_kwh_charge_reduction,
            'total_kwh_co2e_reduction': total_kwh_co2e_reduction
        }
    # Ensure that current_data contains the necessary fields before performing the calculations.
    if 'total_kwh' not in current_data or 'total_kwh_co2e' not in current_data or 'total_kwh_charge' not in current_data:
        logging.error(f"Current data is incomplete: {current_data}")
        return
    
    # Assume current_data contains keys: 'kwh', 'co2e', 'charge'
    logging.info(f"Benchmark entries: {benchmark_entries}")
    if not benchmark_entries:
        logging.error("No benchmark entries found!")
        return
    
    for benchmark in benchmark_entries:
        logging.info(f"Processing benchmark: {benchmark}")
        if len(benchmark) != 3:
            logging.error(f"Unexpected benchmark entry format: {benchmark}")
            continue  # Skip this entry

        benchmark_total_kwh, benchmark_total_kwh_co2e, benchmark_total_kwh_charge = benchmark
        # Calculate reductions
        # Ensure current_data values are converted to float before performing subtraction
        total_kwh_reduction = float(benchmark_total_kwh) - float(current_data['total_kwh'])
        total_kwh_co2e_reduction = float(benchmark_total_kwh_co2e) - float(current_data['total_kwh_co2e'])
        total_kwh_charge_reduction = float(benchmark_total_kwh_charge) - float(current_data['total_kwh_charge'])

        # Log or store the reductions as needed
        logging.info(f"Comparing {asset_id} - kWh reduction: {total_kwh_reduction}, Charge reduction: {total_kwh_charge_reduction}, CO2e reduction: {total_kwh_co2e_reduction}")
    
        # Return reductions as a dictionary
        return {
            'total_kwh_reduction': total_kwh_reduction,
            'total_kwh_charge_reduction': total_kwh_charge_reduction,
            'total_kwh_co2e_reduction': total_kwh_co2e_reduction
        }
def get_missing_hours(db_file):
    # Get current date in 'YYYY-MM-DD' format
    current_date = datetime.now().strftime('%Y-%m-%d')
    current_hour = datetime.now().hour
    valid_hours = set(range(current_hour + 1)) # List of hours from 00:00 to the current hour
    
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Query to get hours for the current day from the daily_usage table
    query = """
    SELECT strftime('%H', update_time) as hour
    FROM daily_usage
    WHERE date(update_time) = ?
    """
    
    cursor.execute(query, (current_date,))
    
    # Fetch the results and convert them to a set of integers (hours)
    recorded_hours = {int(row[0]) for row in cursor.fetchall()}
    
    # Calculate the missing hours by finding the difference between all_hours and recorded_hours
    missing_hours = sorted(valid_hours - recorded_hours)
    
    # Close the database connection
    conn.close()
    
    return missing_hours

def calculate_daily_consumption_by_asset(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        #logging.info("Starting calculation for daily consumption and compressor stats")

        current_date = datetime.now().date()
        start_of_day = datetime.combine(current_date, datetime.min.time())
        end_of_day = start_of_day + timedelta(days=1)

        start_of_day_str = start_of_day.strftime('%Y-%m-%d %H:%M:%S')
        end_of_day_str = end_of_day.strftime('%Y-%m-%d %H:%M:%S')
        # get all records from tasmota_energy_data from the beginning and end of the day
        # (essentially until now)
        query = """
        SELECT asset_id, asset_name, power, response_time
        FROM tasmota_energy_data
        WHERE response_time >= ? AND response_time < ?
        ORDER BY response_time
        """
        cursor.execute(query, (start_of_day_str, end_of_day_str))
        results = cursor.fetchall()

        if not results:
            logging.warning("No data found for the current day")
            return

        logging.info(f"Fetched {len(results)} records for processing")

        asset_data = {}
        previous_power = {}
        compressor_start_times = {}
        compressor_runtimes = []
        total_kwh_charges = {}
        daily_total_kwh = 0.0
        
        # Set currrent hour for record calculations
        missing_hours = get_missing_hours(db_file)
        logging.info(f"Missing hour: {missing_hours}")

        # If missing hours exist
        if missing_hours:
            # Set current_hour to the first missing hour
            current_hour = missing_hours[0]
            logging.info(f"Missing hour: {current_hour}")

        else:
            # Otherwise, set current_hour to the hour from response_time
            current_hour = response_time.hour
            logging.info(f"Current hour: {current_hour}")

        for row in results:
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
                }
                logging.warning(f"Initializing data for asset_id: {asset_id}")
                
                # Set initial values to calculate compressor state:
                previous_power[asset_id] = power # first power reading is to also equal previous_power
                compressor_start_times[asset_id] = None # Record compressor state as off
                total_kwh_charges[asset_id] = 0.0 # initialise kwh charges to start from 0.

            # Assume 4 measurements per minute, and calculate kWh per measurement
            interval_seconds = 60 / 4
            kwh = (power / 1000) * (interval_seconds / 3600)

            # Cumulative kWh usage for this asset.
            asset_data[asset_id]['total_kwh'] += kwh # Add kwh (above) to the current asset_id total_kwh value.
            # Cumulative kwh for all assets.
            daily_total_kwh += kwh # Add kwh (above) to the current asset_id daily_total_kwh value.

            # Get the current hour from the datetime formatted response time in the record
            # current_hour = response_time.hour

            # Reset current_hour_kwh for the asset if a new hour starts
            if asset_data[asset_id]['last_processed_hour'] != current_hour: # If the last_processed_hour value does not = the hour value records are not being processed for.
                #logging.info(f"Resetting current_hour_kwh for asset {asset_id} for new hour {current_hour}")
                asset_data[asset_id]['current_hour_kwh'] = 0.0 # Reset current hour kwh usage to 0.
                asset_data[asset_id]['last_processed_hour'] = current_hour # update the value of last_processed_hour to = current_hour so when the next record is processed, it will be considered in the current_hour.

            # If the response time matches the current hour, accumulate kWh for the current hour
            if response_time.date() == current_date and current_hour == response_time.hour:
                asset_data[asset_id]['current_hour_kwh'] += kwh # If the date and hour in the response_time field of the record being processed = the current_date and current_hour value, add kwh to usage 
                #logging.info(f"Current hour kWh for {asset_id}: {asset_data[asset_id]['current_hour_kwh']}")

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

            # Compute daily total kWh charge for all assets
            daily_total_kwh_charge = daily_total_kwh * rate

        for asset_id, data in asset_data.items():
            total_kwh = data['total_kwh']
            asset_name = data['asset_name']
            cnt_comp_on = data['cnt_comp_on']
            cnt_comp_off = data['cnt_comp_off']
            total_comp_runtime = data['total_comp_runtime']
            day_of_week = response_time.strftime('%A')

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

            response_time = datetime.strptime(response_time_str, '%Y-%m-%d %H:%M:%S')
            # Get the formatted hour for the current record
            # Set minutes and seconds to zero to get the beginning of the hour
            #response_time_start_of_hour = response_time.replace(minute=0, second=0, microsecond=0)

            # Format the hour as HH:00 for the beginning of the hour
            hour = current_hour.strftime('%H:%M')
            #hour = response_time_start_of_hour.strftime('%H:%M')  # This will now always be 'HH:00'
            #current_hour = response_time_start_of_hour.hour  # Get the current hour as an integer
            #logging.info(f"current_hour = {current_hour}")

            # Define current_time_str for logging or other purposes
            current_time_str = response_time.strftime('%Y-%m-%d %H:%M:%S')
        
            # Fetch yesterday's kWh for the same hour
            cursor.execute('''
                SELECT total_kwh FROM daily_usage WHERE asset_id = ? AND date = ? AND hour = ?
            ''', (asset_id, (current_date - timedelta(days=1)).isoformat(), hour))
            # set yesterday_record to be the 1st record in the same hour yesterday using fetchone() function.
            yesterday_record = cursor.fetchone()
            # assign if value exists. Other value of 0.0 is assigned.
            yesterday_kwh = yesterday_record[0] if yesterday_record else 0.0
            # Calculate percentage change_kwh. Again. total_kwh reflects that cummulative kwh usage
            # for an asset for the current day.
            # yesterday_kwh value is retrieved from the db, by looking for the 1st record for the same hour
            # yesterday (refer above).
            percentage_change_kwh = calculate_percentage_change_kwh(total_kwh, yesterday_kwh)
            
            # Retrieve current_hour_kwh for this asset
            asset_current_hour_kwh = asset_data[asset_id]['current_hour_kwh']

            #logging.info(f"{asset_id}: Calculating CO2 emissions for total_kwh: {total_kwh}, current_hour_kwh: {asset_current_hour_kwh}, daily_total_kwh: {daily_total_kwh}")
    
            total_kwh_co2e = calculate_co2e_emission(total_kwh)
            current_hour_kwh_co2e = calculate_co2e_emission(asset_current_hour_kwh)
            daily_total_kwh_co2e = calculate_co2e_emission(daily_total_kwh)
            
            # Prepare data to pass to benchmark reduction function
            current_data = {
                'day_of_week': day_of_week,
                'hour': f"{str(current_hour).zfill(2)}:00",  # Use current_hour directly
                'total_kwh': total_kwh,
                'total_kwh_co2e': total_kwh_co2e,
                'total_kwh_charge': total_kwh_charge
            }
            # Run the function and calculate values
            comparison_results = compare_with_benchmark(cursor, asset_id, current_data)

            # Extract reduction values if comparison results exist
            if comparison_results:
                total_kwh_reduction = comparison_results['total_kwh_reduction']
                total_kwh_charge_reduction = comparison_results['total_kwh_charge_reduction']
                total_kwh_co2e_reduction = comparison_results['total_kwh_co2e_reduction']
            else:
                total_kwh_reduction = total_kwh_charge_reduction = total_kwh_co2e_reduction = 0  # Default values if no comparison results
            
            logging.info(f"{total_kwh_reduction}, {total_kwh_charge_reduction},{total_kwh_co2e_reduction} ")
            #logging.info(f"Current hour kWh for {asset_id}: {asset_current_hour_kwh}")
            #logging.info(f"total_kwh_co2e: {total_kwh_co2e} {'grams' if total_kwh_co2e < 500 else 'tonnes'}")
            #logging.info(f"current_hour_kwh_co2e: {current_hour_kwh_co2e} {'grams' if current_hour_kwh_co2e < 500 else 'tonnes'}")
            #logging.info(f"daily_total_kwh_co2e: {daily_total_kwh_co2e} {'grams' if daily_total_kwh_co2e < 500 else 'tonnes'}")

            # Insert or update the record in daily_usage
            cursor.execute('''
                INSERT INTO daily_usage (asset_id, asset_name, date, total_kwh, cnt_comp_on, cnt_comp_off, ave_comp_runtime, 
                                        max_comp_runtime, min_comp_runtime, update_time, total_kwh_charge, hour, 
                                        percentage_change_kwh, daily_total_kwh, current_hour_kwh, total_kwh_co2e, 
                                        daily_total_kwh_co2e, current_hour_kwh_co2e, daily_total_kwh_charge, day_of_week)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id, date, hour) DO UPDATE SET
                    total_kwh = excluded.total_kwh,
                    cnt_comp_on = excluded.cnt_comp_on,
                    cnt_comp_off = excluded.cnt_comp_off,
                    ave_comp_runtime = excluded.ave_comp_runtime,
                    max_comp_runtime = excluded.max_comp_runtime,
                    min_comp_runtime = excluded.min_comp_runtime,
                    update_time = excluded.update_time,
                    total_kwh_charge = excluded.total_kwh_charge,
                    percentage_change_kwh = excluded.percentage_change_kwh,
                    daily_total_kwh = excluded.daily_total_kwh,
                    current_hour_kwh = excluded.current_hour_kwh,
                    total_kwh_co2e = excluded.total_kwh_co2e,
                    daily_total_kwh_co2e = excluded.daily_total_kwh_co2e,
                    current_hour_kwh_co2e = excluded.current_hour_kwh_co2e,
                    daily_total_kwh_charge = excluded.daily_total_kwh_charge,
                    day_of_week = excluded.day_of_week
            ''', (
                asset_id, asset_name, current_date.isoformat(), 
                round(total_kwh, 2), cnt_comp_on, cnt_comp_off, 
                ave_comp_runtime_str, max_comp_runtime_str, min_comp_runtime_str, 
                current_time_str, round(total_kwh_charge, 2), hour, 
                round(percentage_change_kwh,2), round(daily_total_kwh, 2), 
                round(asset_current_hour_kwh,3), total_kwh_co2e, 
                daily_total_kwh_co2e, current_hour_kwh_co2e,
                round(daily_total_kwh_charge, 2), day_of_week
            ))

            conn.commit()
            cursor.execute('''
                INSERT INTO daily_saving (
                    update_time, asset_id, asset_name, date, hour, day_of_week, 
                    total_kwh_reduction, total_kwh_charge_reduction, total_kwh_co2e_reduction
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (asset_id, date, hour) DO UPDATE SET
                    update_time = excluded.update_time,
                    total_kwh_reduction = excluded.total_kwh_reduction,
                    total_kwh_charge_reduction = excluded.total_kwh_charge_reduction,
                    total_kwh_co2e_reduction = excluded.total_kwh_co2e_reduction
            ''', (
                current_time_str, asset_id, asset_name, current_date.isoformat(), 
                hour, day_of_week, round(total_kwh_reduction, 3), 
                round(total_kwh_charge_reduction,3), round(total_kwh_co2e_reduction,3)
            ))

            conn.commit()

        logging.info("Daily consumption and benchmark stats updated successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        conn.close()

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