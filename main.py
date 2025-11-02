import paho.mqtt.client as mqtt
import csv

# MQTT settings
broker = "test.mosquitto.org"
port = 1883
topic = ""

# CSV file
csv_file = "sensor_data.csv"

# Callback when connected
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(topic)

# Callback when message received
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Received: {payload}")
    # Parse payload
    data = payload.split(',')
    if len(data) == 9:
        node, timestamp, temp, gas, mq135, pressure, pm1, pm2_5, pm10 = data
        # Write to CSV
        with open(csv_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([node, timestamp, temp, gas, mq135, pressure, pm1, pm2_5, pm10])
    else:
        print("Invalid payload format")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker, port, 60)

# Create CSV header if not exists
try:
    with open(csv_file, 'r') as f:
        pass
except FileNotFoundError:
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['node', 'timestamp', 'temp', 'gas', 'mq135', 'pressure', 'pm1', 'pm2_5', 'pm10'])

client.loop_forever()