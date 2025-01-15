import sqlite3
import json
from datetime import datetime
import paho.mqtt.client as mqtt
from logging_utils import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger('sensor_subscriber_logger')

# SQLite database path
DB_FILE = '/root/projects/tasmota/sqlite3_db/tasmota_data.db'

# Global SQLite connection
db_connection = None

def init_db_connection():
    """Initialize a persistent database connection."""
    global db_connection
    try:
        db_connection = sqlite3.connect(DB_FILE, check_same_thread=False)
        logger.info("Database connection established.")
    except sqlite3.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        exit(1)

def close_db_connection():
    """Close the database connection."""
    global db_connection
    if db_connection:
        db_connection.close()
        logger.info("Database connection closed.")

def get_sensor_names():
    """Fetch unique sensor names from the sensor_info table."""
    try:
        cursor = db_connection.cursor()
        cursor.execute("SELECT DISTINCT sensor_name FROM sensor_info")
        sensor_names = [row[0] for row in cursor.fetchall()]
        logger.info(f"Fetched sensor names: {sensor_names}")
        return sensor_names
    except sqlite3.Error as e:
        logger.error(f"Error querying sensor_info table: {e}")
        return []

def parse_message(payload):
    """Parse and format the incoming payload."""
    try:
        parsed_data = {
            "Timestamp": None,
            "Complete Local Name": None,
            "MAC Address": None,
            "UUID": None,
            "Major": None,
            "Minor": None,
            "Rx Power (dBm)": None,
            "Battery Voltage (mV)": None,
            "Broadcast Interval (ms)": None,
            "Temperature (C)": None,
            "Humidity (%)": None,
        }

        # Parse and format the timestamp
        raw_timestamp = payload.get("Timestamp")
        if raw_timestamp:
            try:
                dt_object = datetime.fromisoformat(raw_timestamp)
                parsed_data["Timestamp"] = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                logger.warning(f"Invalid timestamp format '{raw_timestamp}': {e}")

        # Process "Scan Data" array
        scan_data = payload.get("Scan Data", [])
        for item in scan_data:
            description = item.get("Description")
            parsed = item.get("Parsed Data", {})

            if description == "Complete Local Name":
                parsed_data["Complete Local Name"] = parsed.get("Complete Local Name")
            elif description == "Manufacturer":
                parsed_data.update({
                    "UUID": parsed.get("UUID"),
                    "Major": parsed.get("Major"),
                    "Minor": parsed.get("Minor"),
                    "Rx Power (dBm)": parsed.get("Rx Power (dBm)"),
                })
            elif description == "16b Service Data":
                parsed_data.update({
                    "MAC Address": parsed.get("MAC Address"),
                    "Battery Voltage (mV)": parsed.get("Battery Voltage (mV)"),
                    "Broadcast Interval (ms)": parsed.get("Broadcast Interval (ms)"),
                    "Temperature (C)": parsed.get("Temperature (C)"),
                    "Humidity (%)": parsed.get("Humidity (%)"),
                })

        # Fallback for MAC Address
        if not parsed_data["MAC Address"]:
            parsed_data["MAC Address"] = payload.get("Device Address")

        logger.debug(f"Parsed data: {parsed_data}")
        return parsed_data
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None

def write_to_sensor_data_table(data):
    """Write parsed message data to the sensor_data table."""
    try:
        cursor = db_connection.cursor()
        cursor.execute("""
            INSERT INTO sensor_data (
                response_time, sensor_name, sensor_mac, sensor_uuid, 
                sensor_major, sensor_minor, sensor_rxpower, sensor_battery, 
                sensor_bcst_interval, sensor_temperature, sensor_humidity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["Timestamp"], data["Complete Local Name"], data["MAC Address"],
            data["UUID"], data["Major"], data["Minor"], data["Rx Power (dBm)"],
            data["Battery Voltage (mV)"], data["Broadcast Interval (ms)"],
            data["Temperature (C)"], data["Humidity (%)"]
        ))
        db_connection.commit()
        logger.debug("Data written to sensor_data table.")
    except sqlite3.Error as e:
        logger.error(f"Error writing to sensor_data table: {e}")

def on_connect(client, userdata, flags, rc):
    """Callback for MQTT connection."""
    if rc == 0:
        logger.info("Connected to the broker successfully.")
        sensor_names = get_sensor_names()
        if sensor_names:
            for sensor_name in sensor_names:
                topic = f"ble/{sensor_name}"
                client.subscribe(topic)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.warning("No sensor names found in the database.")
    else:
        logger.error(f"Failed to connect to the broker. Return code: {rc}")

def on_message(client, userdata, msg):
    """Callback for MQTT messages."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        logger.debug(f"Message received on topic {msg.topic}: {payload}")
        parsed_data = parse_message(payload)
        if parsed_data:
            write_to_sensor_data_table(parsed_data)
        else:
            logger.warning("Parsed data is invalid. Skipping...")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Initialize MQTT client
client = mqtt.Client(client_id="energyApp_sub01")

# Set up SSL/TLS options
CERTFILE = "/etc/ssl/private/energyApp.crt"
KEYFILE = "/etc/ssl/private/energyApp.key"
CA_CERTS = "/etc/ssl/certs/rootCA.pem"

try:
    client.tls_set(certfile=CERTFILE, keyfile=KEYFILE, ca_certs=CA_CERTS)
except FileNotFoundError as e:
    logger.error(f"Error loading TLS settings: {e}")
    exit(1)

# Set MQTT callbacks
client.on_connect = on_connect
client.on_message = on_message

# Start MQTT client loop
try:
    init_db_connection()
    logger.info("Connecting to the broker...")
    client.connect("192.168.2.55", 8883, 60)
    logger.info("Starting MQTT client loop...")
    client.loop_forever()
except KeyboardInterrupt:
    logger.info("Graceful shutdown initiated.")
finally:
    close_db_connection()
