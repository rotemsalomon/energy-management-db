import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

def on_message(client, userdata, msg):
    print(f"Message received: {msg.payload}")

# Create an MQTT client instance
client_id = "energyApp_sub01"
client = mqtt.Client(client_id=client_id)

# Set SSL/TLS options
certfile = "/etc/ssl/private/energyApp.crt"
keyfile = "/etc/ssl/private/energyApp.key"
ca_certs = "/etc/ssl/certs/rootCA.pem"

# Handle deprecated callback API
try:
    client.tls_set(certfile=certfile, keyfile=keyfile, ca_certs=ca_certs)
except FileNotFoundError as e:
    print(f"Error loading TLS settings: {e}")

# Set callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the EMQX broker
client.connect("192.168.2.55", 8883, 60)

# Start the MQTT client loop
client.loop_forever()