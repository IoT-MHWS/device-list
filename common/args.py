from collections import namedtuple
import sys

Socket = namedtuple("Socket", "address port")
MqttId = namedtuple("MqttId", "building_id room_id type_id device_id")
MapId = namedtuple("MapId", "c_x c_y typ idx")
InputArgs = namedtuple(
    "InputArgs", ["broker_socket", "map_socket", "mqtt_id", "map_id", "polling_rate_ms"]
)


def process_arguments(count: int = 6, strict: bool = True) -> InputArgs:
    count_err_msg = f"Invalid count of arguments"

    if strict:
        if len(sys.argv) != count:
            raise ValueError(count_err_msg)
    else:
        if len(sys.argv) < count:
            raise ValueError(count_err_msg)

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
