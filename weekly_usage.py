import sqlite3
from datetime import datetime, timedelta
import schedule  # Importing schedule, but we won't use it for now
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='/var/log/tasmota-weekly-usage.log',
    filemode='a'
)

# Define the database path
db_file = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

def get_week_start(date):
    """Get the start date (Sunday) of the week for a given date."""
    return date - timedelta(days=(date.weekday() + 1) % 7)

def get_week_end(week_start):
    """Get the end date (Saturday) of the week for a given start date."""
    return week_start + timedelta(days=6)

def update_weekly_usage():
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    logging.info("Connected to the database successfully.")
    cursor = conn.cursor()

    # Get the current date
    today = datetime.now().date()

    # Calculate the start and end of the current week (Sunday to Saturday)
    week_start = get_week_start(today)
    week_end = get_week_end(week_start)

    # Fetch all unique asset_ids from daily_usage
    cursor.execute("SELECT DISTINCT asset_id FROM daily_usage")
    asset_ids = cursor.fetchall()

    for asset_id_tuple in asset_ids:
        asset_id = asset_id_tuple[0]

        # Get asset_name for the asset_id. Required if new record is created for a new week.
        cursor.execute("SELECT asset_name FROM daily_usage WHERE asset_id = ?", (asset_id,))
        asset_name = cursor.fetchone()[0]

        # Query to get the weekly total for the asset
        cursor.execute("""
            SELECT SUM(total_kwh), SUM(total_kwh_charge) 
            FROM daily_usage 
            WHERE asset_id = ? 
            AND hour = '23:00'
            AND DATE(update_time) >= ? 
            AND DATE(update_time) <= ?
        """, (asset_id, week_start, week_end))

        # Fetch the results
        result = cursor.fetchone()

        # result will be a tuple (sum_total_kwh, sum_total_kwh_charge)
        if result:
            weekly_total_kwh, weekly_total_kwh_charge = result
            logging.info(f"Weekly total kWh: {weekly_total_kwh}, Weekly total charge: {weekly_total_kwh_charge}")
        else:
            logging.info("No records found for the specified week and asset.")
  
        # Handle None if no data, and unpack the results
        total_kwh = result[0] or 0.0  # Sum of total_kwh
        total_kwh_charge = result[1] or 0.0  # Sum of total_kwh_charge

        # Get the current timestamp in the required format
        update_time = datetime.now().replace(microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

        # Check if a record already exists for this week and asset_id in weekly_usage
        cursor.execute("""
            SELECT id FROM weekly_usage WHERE asset_id = ? AND week_start_date = ?
        """, (asset_id, week_start))
        record = cursor.fetchone()

        if record:
            # Update the existing record
            cursor.execute("""
                UPDATE weekly_usage 
                SET total_kwh = ?, total_kwh_charge = ?, update_time = ?, week_end_date = ? 
                WHERE id = ?
            """, (total_kwh, total_kwh_charge, update_time, week_end, record[0]))
            logging.info(f"Updated weekly_usage for asset_id: {asset_id}, week_start: {week_start}, week_end: {week_end}.")
        else:
            # Insert a new record
            cursor.execute("""
                INSERT INTO weekly_usage (update_time, week_start_date, week_end_date, asset_id, asset_name, total_kwh, total_kwh_charge) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (update_time, week_start, week_end, asset_id, asset_name, total_kwh, total_kwh_charge))
            logging.info(f"Updated weekly_usage for asset_id: {asset_id}, week_start: {week_start}, week_end: {week_end}.")

    # Commit changes and close the connection
    conn.commit()
    logging.info("Weekly usage data committed successfully.")
    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    # Schedule the task to run at midnight every day
    schedule.every().day.at("00:00").do(update_weekly_usage)

    # Run the function directly for testing
    update_weekly_usage()

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every hour 

