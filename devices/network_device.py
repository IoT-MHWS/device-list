"""
Device that can receive or transmit single message, isn't connected with server.
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
from cwspb.network_pb2 import Packet


def grpc_transmit_packets(
    stub: sv_device_grpc.DeviceServiceStub, map_id: MapId, packets: list[str]
) -> Response:
    grpc_packets = []
    for packet in packets:
        byte_content = str.encode(packet)
        grpc_packets.append(Packet(content=byte_content))

    print(grpc_packets)

    response: Response = stub.TransmitPacket(
        sv_device.RequestTransmitPackets(
            id=SubjectId(
                coordinates=Coordinates(x=map_id.c_x, y=map_id.c_y),
                id=Id(type=map_id.typ, idx=map_id.idx),
            ),
            packets=grpc_packets,
        )
    )
    return response


def grpc_receive_packets(
    stub: sv_device_grpc.DeviceServiceStub, map_id: MapId
) -> sv_device.ResponseReceivedPackets:
    response: sv_device.ResponseReceivedPackets = stub.ReceivePackets(
        sv_device.RequestDevice(
            id=SubjectId(
                coordinates=Coordinates(x=map_id.c_x, y=map_id.c_y),
                id=Id(type=map_id.typ, idx=map_id.idx),
            ),
        )
    )
    return response


def grpc_handle_transmit_response(response: Response) -> bool:
    is_ok: bool = response.status.type == 0
    if not is_ok:
        logging.error(response.status.text)
    return is_ok


def grpc_handle_received_response(response: sv_device.ResponseReceivedPackets) -> bool:
    is_ok: bool = response.base.status.type == 0
    if not is_ok:
        logging.error(response.base.status.text)
    return is_ok


def grpc_received_response_to_list(
    response: sv_device.ResponseReceivedPackets,
) -> list[str]:
    packets: list[str] = []
    for packet in response.packets:
        str_content = packet.content.decode()
        packets.append(str_content)
    return packets


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = process_arguments(count=7, strict=False)
    # transmit (t) or receive (r)
    is_transmit = sys.argv[6][0] == "t"

    broker_socket: Socket = args.broker_socket
    map_socket: Socket = args.map_socket
    mqtt_id: MqttId = args.mqtt_id
    map_id: MapId = args.map_id
    polling_rate_ms: int = args.polling_rate_ms

    grpc_channel = grpc.insecure_channel(f"{map_socket.address}:{map_socket.port}")
    grpc_stub = sv_device_grpc.DeviceServiceStub(grpc_channel)

    if is_transmit:
        packets = sys.argv[7:]
        response = grpc_transmit_packets(grpc_stub, map_id, packets)
        grpc_handle_transmit_response(response)
    else:
        while True:
            response = grpc_receive_packets(grpc_stub, map_id)
            if grpc_handle_received_response(response):
                packets = grpc_received_response_to_list(response)
                print(packets)
            time.sleep(polling_rate_ms / 1000)

    mqtt_topic = mqtt_get_topic(
        mqtt_id.building_id, mqtt_id.room_id, mqtt_id.type_id, mqtt_id.device_id
    )

    grpc_channel.close()
