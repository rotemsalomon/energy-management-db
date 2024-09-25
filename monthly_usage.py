import sqlite3
from datetime import datetime, timedelta
import logging
import schedule
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database file path
db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

# Helper function to get start and end dates for a given month and year
def get_month_start_end_dates(year, month):
    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month.replace(day=1) - timedelta(days=1)
    return start_date, end_date

# Calculate the percentage change function
def calculate_percentage_change(current_value, previous_value):
    try:
        current_value = float(current_value)
        previous_value = float(previous_value)
        if previous_value == 0:
            return 100 if current_value > 0 else 0
        return ((current_value - previous_value) / previous_value) * 100
    except ValueError:
        logging.error("ValueError in calculate_percentage_change: Ensure current_value and previous_value are numeric.")
        return 0

# Calculate the total kWh and total_kwh_charge for each asset in the month from 'daily_usage' and insert/update 'monthly_usage'
def calculate_and_update_monthly_usage():
    conn = None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_file)
        logging.info("Connected to the database successfully.")

        cur = conn.cursor()

        # Query to calculate total_kwh and total_kwh_charge for each asset for each month in 'daily_usage'
        query_sql = '''
            SELECT strftime('%Y-%m', update_time) AS month, asset_id, asset_name,
                SUM(total_kwh) AS total_kwh,
                SUM(total_kwh_charge) AS total_kwh_charge
            FROM daily_usage
            GROUP BY month, asset_id, asset_name;
        '''
        cur.execute(query_sql)
        results = cur.fetchall()

        if results:
            logging.info("Data fetched successfully from daily_usage.")
        else:
            logging.warning("No data found in daily_usage for the month calculation.")

        # Insert or update records in 'monthly_usage'
        for row in results:
            month, asset_id, asset_name, total_kwh, total_kwh_charge = row
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Check if the record for the month already exists in 'monthly_usage'
            select_sql = '''
            SELECT id, total_kwh, total_kwh_charge FROM monthly_usage WHERE asset_id = ? AND date = ?;
            '''
            cur.execute(select_sql, (asset_id, month))
            existing_record = cur.fetchone()

            if existing_record:
                # Update the existing record
                update_sql = '''
                UPDATE monthly_usage
                SET update_time = ?, total_kwh = ?, total_kwh_charge = ?
                WHERE id = ?;
                '''
                cur.execute(update_sql, (update_time, total_kwh, total_kwh_charge, existing_record[0]))
                logging.info(f"Updated monthly_usage for asset_id: {asset_id}, month: {month}.")
            else:
                # Insert a new record
                insert_sql = '''
                INSERT INTO monthly_usage (update_time, date, asset_id, asset_name, total_kwh, total_kwh_charge)
                VALUES (?, ?, ?, ?, ?, ?);
                '''
                cur.execute(insert_sql, (update_time, month, asset_id, asset_name, total_kwh, total_kwh_charge))
                logging.info(f"Inserted new record into monthly_usage for asset_id: {asset_id}, month: {month}.")

        # Calculate percentage changes for current month compared to previous month
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        previous_month = current_month - 1 if current_month > 1 else 12
        previous_year = current_year if current_month > 1 else current_year - 1

        previous_month_start, previous_month_end = get_month_start_end_dates(previous_year, previous_month)
        current_month_start, _ = get_month_start_end_dates(current_year, current_month)

        cur.execute('''
            SELECT asset_id, SUM(total_kwh) AS total_kwh, SUM(total_kwh_charge) AS total_kwh_charge
            FROM monthly_usage
            WHERE date = ?
            GROUP BY asset_id;
        ''', (current_date.strftime('%Y-%m'),))
        current_month_data = cur.fetchall()

        if current_month_data:
            logging.info("Current month data fetched successfully from monthly_usage.")
        else:
            logging.warning("No current month data found in monthly_usage for percentage change calculation.")

        cur.execute('''
            SELECT asset_id, SUM(total_kwh) AS total_kwh, SUM(total_kwh_charge) AS total_kwh_charge
            FROM daily_usage
            WHERE update_time BETWEEN ? AND ?
            GROUP BY asset_id;
        ''', (previous_month_start.strftime('%Y-%m-%d'), previous_month_end.strftime('%Y-%m-%d')))
        previous_month_data = cur.fetchall()

        # Create a mapping for easy lookup of previous month's data
        previous_month_dict = {row[0]: (row[1], row[2]) for row in previous_month_data}

        for row in current_month_data:
            asset_id, total_kwh, total_kwh_charge = row

            prev_kwh, prev_charge = previous_month_dict.get(asset_id, (0, 0))
            kwh_change = total_kwh - prev_kwh
            charge_change = total_kwh_charge - prev_charge

            kwh_percentage_change = calculate_percentage_change(total_kwh, prev_kwh)
            charge_percentage_change = calculate_percentage_change(total_kwh_charge, prev_charge)

            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Update the existing record with percentage changes and actual changes
            update_sql = '''
            UPDATE monthly_usage
            SET update_time = ?, kwh_change = ?, charge_change = ?, kwh_percentage_change = ?, charge_percentage_change = ?
            WHERE date = ? AND asset_id = ?;
            '''
            cur.execute(update_sql, (update_time, kwh_change, charge_change, kwh_percentage_change, charge_percentage_change, current_date.strftime('%Y-%m'), asset_id))
            logging.info(f"Updated monthly_usage with percentage changes and actual changes for asset_id: {asset_id}, month: {current_date.strftime('%Y-%m')}. kWh Change: {kwh_change:.2f}, Charge Change: {charge_change:.2f}. kWh Percentage Change: {kwh_percentage_change:.2f}%, Charge Percentage Change: {charge_percentage_change:.2f}%.")

        conn.commit()
        logging.info("Monthly usage data committed successfully.")

    except sqlite3.Error as e:
        logging.error(f"Error during monthly usage calculation or insertion: {e}")

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

# Main function to schedule the task
def main():
    # Schedule the task to run at midnight every day
    schedule.every().day.at("00:00").do(calculate_and_update_monthly_usage)
    logging.info("Scheduled task to run at midnight every day.")

    # Run the function directly for testing
    #calculate_and_update_monthly_usage()

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    main()

