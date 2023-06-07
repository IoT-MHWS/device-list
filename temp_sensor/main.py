import paho.mqtt.client as mqtt
import json
import sys
import time
import os
from collections import namedtuple

# Add generated directory to python module path
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(os.path.join(ROOT_DIR, "_gen"))

import cwspb.physical_pb2


def mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print(f"Failed to connect, return code: {rc}")


def mqtt_connect_loop(address: str, port: int) -> mqtt.Client:
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_on_connect

    mqtt_client.connect(address, port, keepalive=60)
    mqtt_client.loop_start()
    return mqtt_client


def mqtt_get_topic(building_id: str, room_id: str, device_type: str, device_id: str):
    return "/cws/{}/{}/{}/{}".format(building_id, room_id, device_type, device_id)


Socket = namedtuple("Socket", "address port")
MqttId = namedtuple("MqttId", "building_id room_id type_id device_id")
MapId = namedtuple("MapId", "c_x c_y typ idx")
InputArgs = namedtuple(
    "InputArgs", ["broker_socket", "map_socket", "mqtt_id", "map_id", "polling_rate_ms"]
)


def process_arguments() -> InputArgs:
    try:
        broker_address, broker_port = sys.argv[1].split(":")
        map_address, map_port = sys.argv[2].split(":")
        building_id, room_id, type_id, device_id = sys.argv[3].split(":")
        c_x, c_y = sys.argv[4].split(":")
        polling_rate = sys.argv[5]
    except ValueError:
        raise ValueError(f"Look in process_argument() for valid argument passing")

    return InputArgs(
        broker_socket=Socket(address=broker_address, port=int(broker_port)),
        map_socket=Socket(address=map_address, port=int(map_port)),
        mqtt_id=MqttId(
            building_id=building_id,
            room_id=room_id,
            type_id=type_id,
            device_id=device_id,
        ),
        map_id=MapId(c_x=int(c_x), c_y=int(c_y), typ=int(type_id), idx=int(device_id)),
        polling_rate_ms=int(polling_rate),
    )


if __name__ == "__main__":
    args = process_arguments()

    broker_socket: Socket = args.broker_socket
    map_socket: Socket = args.map_socket
    mqtt_id: MqttId = args.mqtt_id
    map_id: MapId = args.map_id
    polling_rate_ms: int = args.polling_rate_ms

    mqtt_topic = mqtt_get_topic(
        mqtt_id.building_id, mqtt_id.room_id, mqtt_id.type_id, mqtt_id.device_id
    )

    mqtt_client = mqtt_connect_loop(broker_socket.address, broker_socket.port)

    while True:
        mqtt_client.publish(mqtt_topic, '{"msg": "hello!"}')
        time.sleep(1)
