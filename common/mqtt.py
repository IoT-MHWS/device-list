import paho.mqtt.client as mqtt
import logging


def mqtt_on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
    else:
        logging.error(f"Failed to connect, return code: {rc}")


def mqtt_connect_loop(address: str, port: int) -> mqtt.Client:
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_on_connect

    mqtt_client.connect(address, port, keepalive=60)
    mqtt_client.loop_start()
    return mqtt_client


def mqtt_get_topic(building_id: str, room_id: str, device_type: str, device_id: str):
    return "/cws/{}/{}/{}/{}".format(building_id, room_id, device_type, device_id)
