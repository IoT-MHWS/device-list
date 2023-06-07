import paho.mqtt.client as mqtt
import json
import sys
import time

if __name__ != "__main__":
    exit(0)

if len(sys.argv) != 6:
    print("Args: <broker_address> <broker_port> <room_id> <sensor_id> <polling_rate_ms>")
    sys.exit(1)

sensor_type = "temp"
broker_address = sys.argv[1]
broker_port = int(sys.argv[2])
room_id = sys.argv[3]
sensor_id = sys.argv[4]
polling_rate = int(sys.argv[5])

mqtt_topic = f"/cws/{room_id}/{sensor_type}/{sensor_id}"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print(f"Failed to connect, return code: {rc}")

client = mqtt.Client()
client.on_connect = on_connect

client.connect(broker_address, broker_port, keepalive=60)
client.loop_start()

while True:
    payload = {
        "temp": 25.5
    }
    client.publish(mqtt_topic, json.dumps(payload))
    time.sleep(polling_rate / 1_000)

