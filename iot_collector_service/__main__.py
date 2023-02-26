"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .mqtt_client import MqttClient
import json

def run_service():

    # Topic definitions
    data_topic = "porenta/martin_room/air_quality/data/measurements"
    beat_topic = "porenta/martin_room/air_quality/sys/beat"

    print("IOT data collector service v0.1")
    test_client = MqttClient()
    test_client.mqtt_client_connect(usr="admin",
                                    password="admin",
                                    broker="192.168.0.101",
                                    port=1883)

    test_client.mqtt_client_subscribe(topic=data_topic)
    test_client.mqtt_client_subscribe(topic=beat_topic)
    test_client.loop_start()

    while 1:
        data = test_client.data_queue.get()

        # Parse data
        if data["topic"] == data_topic:
            #Parse string to dict
            dict_data = json.loads(data["data"])
            print(f"Timestamp: {dict_data['timestamp']}\nTemp: {dict_data['temperature']}\n"
                  f"Hum: {dict_data['humidity']}\nCO2: {dict_data['co2']}\n")

if __name__ == '__main__':
    run_service()
