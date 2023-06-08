"""
Receives message by mqtt and sends request to server.
"""

import json
import time
import os
import sys
import grpc
import traceback
import logging
import paho.mqtt.client as mqtt

# Add root and generated directories to python module path
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(os.path.join(ROOT_DIR))
sys.path.append(os.path.join(ROOT_DIR, "_gen"))

from common.mqtt import mqtt_get_topic
from common.args import process_arguments, Socket, MqttId, MapId

import cwspb.service.sv_device_pb2 as sv_device
import cwspb.service.sv_device_pb2_grpc as sv_device_grpc
from cwspb.subject_pb2 import Id
from cwspb.common_pb2 import Coordinates
from cwspb.service.common_pb2 import SubjectId
from cwspb.service.general_pb2 import Response


def mqtt_connect_loop(address: str, port: int) -> mqtt.Client:
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_on_connect

    mqtt_client.connect(address, port, keepalive=60)
    mqtt_client.loop_start()
    return mqtt_client


def grpc_set_turnable_status(
    stub: sv_device_grpc.DeviceServiceStub, map_id: MapId, turnable_status: int
) -> Response:
    response: Response = stub.TurnDevice(
        sv_device.RequestTurnDevice(
            id=SubjectId(
                coordinates=Coordinates(x=map_id.c_x, y=map_id.c_y),
                id=Id(type=map_id.typ, idx=map_id.idx),
            ),
            turnable_status=turnable_status,
        )
    )
    return response


def grpc_handle_response(response: Response) -> bool:
    is_ok: bool = response.status.type == 0
    if not is_ok:
        logging.error(response.status.text)
    return is_ok


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = process_arguments()

    broker_socket: Socket = args.broker_socket
    map_socket: Socket = args.map_socket
    mqtt_id: MqttId = args.mqtt_id
    map_id: MapId = args.map_id
    polling_rate_ms: int = args.polling_rate_ms

    grpc_channel = grpc.insecure_channel(f"{map_socket.address}:{map_socket.port}")
    grpc_stub = sv_device_grpc.DeviceServiceStub(grpc_channel)

    mqtt_topic = mqtt_get_topic(
        mqtt_id.building_id, mqtt_id.room_id, mqtt_id.type_id, mqtt_id.device_id
    )

    def mqtt_on_connect(client: mqtt.Client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(mqtt_topic, qos=1)
        else:
            logging.error(f"Failed to connect, return code: {rc}")

    def mqtt_on_message(client, userdata, msg):
        print("CALLED")
        try:
            payload = json.loads(msg.payload.decode())
            turnable_status = payload["turnable_status"]
            response = grpc_set_turnable_status(grpc_stub, map_id, turnable_status)
            grpc_handle_response(response)
            logging.info(payload)
        except Exception as e:
            logging.error(traceback.format_exc())

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message
    mqtt_client.connect(broker_socket.address, broker_socket.port, keepalive=60)

    mqtt_client.loop_forever()
    grpc_channel.close()
