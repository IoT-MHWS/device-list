import json
import time
import os
import sys
import grpc
import traceback
import logging

# Add root and generated directories to python module path
ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.append(os.path.join(ROOT_DIR))
sys.path.append(os.path.join(ROOT_DIR, "_gen"))

from common.mqtt import mqtt_get_topic, mqtt_connect_loop
from common.args import process_arguments, Socket, MqttId, MapId

import cwspb.service.sv_device_pb2 as sv_device
import cwspb.service.sv_device_pb2_grpc as sv_device_grpc
from cwspb.subject_pb2 import Id
from cwspb.common_pb2 import Coordinates
from cwspb.service.common_pb2 import SubjectId


def grpc_get_camera_info(
    stub: sv_device_grpc.DeviceServiceStub, map_id: MapId
) -> sv_device.ResponseCameraInfo:
    response: sv_device.ResponseCameraInfo = stub.GetCameraInfo(
        sv_device.RequestDevice(
            id=SubjectId(
                coordinates=Coordinates(x=map_id.c_x, y=map_id.c_y),
                id=Id(type=map_id.typ, idx=map_id.idx),
            )
        )
    )
    return response


def grpc_handle_response(response: sv_device.ResponseCameraInfo) -> bool:
    is_ok: bool = response.base.status.type == 0
    if not is_ok:
        logging.error(response.base.status.text)
    return is_ok


def grpc_response_to_dict(response: sv_device.ResponseCameraInfo) -> dict:
    result = {}
    result["visible_subjects"] = list()
    for sub in response.visible_subjects:
        result["visible_subjects"].append(
            {
                "coordinates": {"x": sub.coordinates.x, "y": sub.coordinates.y},
                "id": {"idx": sub.id.idx, "type": sub.id.type},
            }
        )
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

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

    grpc_channel = grpc.insecure_channel(f"{map_socket.address}:{map_socket.port}")
    grpc_stub = sv_device_grpc.DeviceServiceStub(grpc_channel)

    while True:
        try:
            response = grpc_get_camera_info(grpc_stub, map_id)
            if grpc_handle_response(response):
                payload = json.dumps(grpc_response_to_dict(response))
                logging.info(payload)
                mqtt_client.publish(mqtt_topic, payload)
        except Exception as e:
            logging.error(traceback.format_exc())
            break

        time.sleep(polling_rate_ms / 1000)

    grpc_channel.close()
