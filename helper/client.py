import json
import os
import sys
import time
import logging

# Add root and generated directories to python module path
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(os.path.join(ROOT_DIR))

from common.mqtt import mqtt_get_topic, mqtt_connect_loop
from common.args import process_arguments, Socket, MqttId, MapId


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = process_arguments(count=6)

    broker_socket: Socket = args.broker_socket
    map_socket: Socket = args.map_socket
    mqtt_id: MqttId = args.mqtt_id
    map_id: MapId = args.map_id
    polling_rate_ms: int = args.polling_rate_ms

    mqtt_topic = mqtt_get_topic(
        mqtt_id.building_id, mqtt_id.room_id, mqtt_id.type_id, mqtt_id.device_id
    )


    mqtt_client = mqtt_connect_loop(broker_socket.address, broker_socket.port)

    payload = json.dumps({
        "turnable_status": 1
    })

    print(mqtt_topic)
    print(payload)

    mqtt_client.publish(mqtt_topic, payload, qos=1)
    time.sleep(1)
