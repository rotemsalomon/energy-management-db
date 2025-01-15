import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully to the broker.")
        # Subscribe to all topics once connected
        client.subscribe("#")
        print("Subscribed to all topics.")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    # Print the topic and the message payload
    print(f"Message received on topic: {msg.topic}")
    print(f"Message payload: {msg.payload.decode('utf-8')}")

# Create an MQTT client instance
client_id = "energyApp_sub01"
client = mqtt.Client(client_id=client_id)

# Set SSL/TLS options
certfile = "/etc/ssl/private/energyApp.crt"
keyfile = "/etc/ssl/private/energyApp.key"
ca_certs = "/etc/ssl/certs/rootCA.pem"

# Handle TLS setup
try:
    client.tls_set(certfile=certfile, keyfile=keyfile, ca_certs=ca_certs)
except FileNotFoundError as e:
    print(f"Error loading TLS settings: {e}")
    exit(1)

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the EMQX broker
try:
    print("Connecting to broker...")
    client.connect("192.168.2.55", 8883, 60)
except Exception as e:
    print(f"Error connecting to broker: {e}")
    exit(1)

# Start the MQTT client loop
try:
    print("Starting MQTT loop to receive messages.")
    client.loop_forever()
except KeyboardInterrupt:
    print("Exiting...")
