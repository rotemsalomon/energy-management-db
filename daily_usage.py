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

def calculate_daily_consumption_by_asset(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        logging.info("Starting calculation for daily consumption and compressor stats")

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
            logging.info("No data found for the current day")
            return

        logging.info(f"Fetched {len(results)} records for processing")

        asset_data = {}
        previous_power = {}
        compressor_start_times = {}
        compressor_runtimes = []
        total_kwh_charges = {}
        daily_total_kwh = 0.0
        current_hour_kwh = 0.0
        
        for row in results:
            asset_id, asset_name, power, response_time_str = row
            response_time = datetime.strptime(response_time_str, '%Y-%m-%d %H:%M:%S')
            # response_time_time = response_time.time() //not used and can be deleted

            # Assume 4 measurements per minute, and calculate kWh per measurement
            interval_seconds = 60 / 4
            kwh = (power / 1000) * (interval_seconds / 3600)
            # Check is asset_id existing in our result array. If not, this means it is
            # the 1st time we are processing the data for this asset_id in the day.
            if asset_id not in asset_data:
                # Initialise the data for this new asset_id
                asset_data[asset_id] = {
                    'total_kwh': 0.0,
                    'cnt_comp_on': 0,
                    'cnt_comp_off': 0,
                    'total_comp_runtime': 0,
                    'asset_name': asset_name
                }
                # store current power for the asset_id which is compared against future reading
                # to see if the compressor has turned on or off.
                previous_power[asset_id] = power
                # Time set to None indicating that it is currently off or hasn't been started yet.
                compressor_start_times[asset_id] = None
                # Reset the list of runtimes.
                compressor_runtimes = []
                # initialise kwh charges to start from 0.
                total_kwh_charges[asset_id] = 0.0

            # Cumulative kWh for this asset. Add kwh (above) to the current asset_id total_kwh value.
            asset_data[asset_id]['total_kwh'] += kwh
            # Cumulative kwh for all assets. Add kwh (above) to the current asset_id daily_total_kwh value.
            daily_total_kwh += kwh

            current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            current_hour = datetime.now().hour
            # Check if the response time is today and in the current hour.
            # If so, add to current_hour_kwh value.
            if response_time.date() == current_date and response_time.hour == current_hour:
                current_hour_kwh += kwh
            
            # Compressor transition detection logic
            if previous_power[asset_id] < 100 and power >= 100:
                asset_data[asset_id]['cnt_comp_on'] += 1
                compressor_start_times[asset_id] = response_time  # Record compressor start time
                #logging.info(f"Compressor ON detected for asset {asset_id} at {response_time}")

            elif previous_power[asset_id] >= 100 and power < 100:
                asset_data[asset_id]['cnt_comp_off'] += 1
                if compressor_start_times[asset_id]:
                    comp_runtime = (response_time - compressor_start_times[asset_id]).total_seconds() / 60.0  # In minutes
                    #logging.info(f"Compressor OFF detected for asset {asset_id} at {response_time}. Runtime: {comp_runtime} minutes")
                    asset_data[asset_id]['total_comp_runtime'] += comp_runtime
                    compressor_runtimes.append(comp_runtime)
                    compressor_start_times[asset_id] = None
            # update the value of previous_power for the asset_id, to compare against the next record for that asset_id
            # to determine if compressor state changed.
            previous_power[asset_id] = power

            # Look up the rate based on response_time
            rate = get_rate_for_response_time(cursor, response_time_str, asset_id)
            kwh_charge = kwh * rate
            total_kwh_charges[asset_id] += kwh_charge

        for asset_id, data in asset_data.items():
            total_kwh = data['total_kwh']
            asset_name = data['asset_name']
            cnt_comp_on = data['cnt_comp_on']
            cnt_comp_off = data['cnt_comp_off']
            total_comp_runtime = data['total_comp_runtime']

            if cnt_comp_on > 0:
                ave_comp_runtime = total_comp_runtime / cnt_comp_on
                #logging.info(f"Asset ID: {asset_id}")
                #logging.info(f"Average Comp Runtime: {ave_comp_runtime}")
                ave_comp_runtime_str = format_runtime(ave_comp_runtime)
                max_comp_runtime = max(compressor_runtimes) if compressor_runtimes else 0
                max_comp_runtime_str = format_runtime(max_comp_runtime)
                min_comp_runtime = min(compressor_runtimes) if compressor_runtimes else 0
                min_comp_runtime_str = format_runtime(min_comp_runtime)
            else:
                ave_comp_runtime_str = max_comp_runtime_str = min_comp_runtime_str = "00:00"

            total_kwh_charge = total_kwh_charges.get(asset_id, 0.0)

            response_time = datetime.strptime(response_time_str, '%Y-%m-%d %H:%M:%S')
            hour = response_time.hour

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

        logging.info(f"Calculating CO2 emissions for total_kwh: {total_kwh}, current_hour_kwh: {current_hour_kwh}, daily_total_kwh: {daily_total_kwh}")
    
        total_kwh_co2e = calculate_co2e_emission(total_kwh)
        current_hour_kwh_co2e = calculate_co2e_emission(current_hour_kwh)
        daily_total_kwh_co2e = calculate_co2e_emission(daily_total_kwh)

        logging.info(f"total_kwh_co2e: {total_kwh_co2e} {'grams' if total_kwh_co2e < 500 else 'tonnes'}")
        logging.info(f"current_hour_kwh_co2e: {current_hour_kwh_co2e} {'grams' if current_hour_kwh_co2e < 500 else 'tonnes'}")
        logging.info(f"daily_total_kwh_co2e: {daily_total_kwh_co2e} {'grams' if daily_total_kwh_co2e < 500 else 'tonnes'}")

        # Insert or update the record in daily_usage
        cursor.execute('''
    INSERT INTO daily_usage (asset_id, asset_name, date, total_kwh, cnt_comp_on, cnt_comp_off, 
                            ave_comp_runtime, max_comp_runtime, min_comp_runtime, update_time, 
                            total_kwh_charge, hour, percentage_change_kwh, daily_total_kwh, 
                            current_hour_kwh, total_kwh_co2e, daily_total_kwh_co2e, current_hour_kwh_co2e)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        current_hour_kwh_co2e = excluded.current_hour_kwh_co2e
''', (
    asset_id, asset_name, current_date.isoformat(), 
    round(total_kwh, 2), cnt_comp_on, cnt_comp_off, 
    ave_comp_runtime_str, max_comp_runtime_str, min_comp_runtime_str, 
    current_time_str, round(total_kwh_charge, 2), hour, 
    percentage_change_kwh, round(daily_total_kwh, 2), 
    round(current_hour_kwh, 2), total_kwh_co2e, 
    daily_total_kwh_co2e, current_hour_kwh_co2e
))

        conn.commit()
        logging.info("Daily consumption and compressor stats updated successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        conn.close()

def main():
    # Run the function directly for testing
    calculate_daily_consumption_by_asset(db_file)

    # Schedule the task to run at the beginning of every hour
    schedule.every().hour.at(":00").do(calculate_daily_consumption_by_asset, db_file=db_file)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()