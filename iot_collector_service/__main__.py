"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho

from time import sleep
import json

data_topic = "porenta/martin_room/air_quality/data/measurements"
beat_topic = "porenta/martin_room/air_quality/sys/beat"

def run_service():

    topics = {
        "data_topic": data_topic,
        "beat_topic": beat_topic
    }
    conf = CollectorConfiguration(collector_name="MQTT_collector",
                                  usr="admin",
                                  password="admin",
                                  ip_addr="192.168.0.101",
                                  port=1883,
                                  topic=topics)

    collector1 = MqttDataCollector(MqttClientPaho())
    collector1.set_configuration(conf)

    service = DataCollectorService()
    service.add_collector(collector1)
    service.start_collection()

    while 1:
        data = service.get_data()

        if data["topic"] == data_topic:
            # Parse string to dict
            dict_data = json.loads(data["data"])
            print(f"Timestamp: {dict_data['timestamp']}\nTemp: {dict_data['temperature']}\n"
                  f"Hum: {dict_data['humidity']}\nCO2: {dict_data['co2']}\n")

    # service.start_collection()
    # sleep(5)
    # service.stop_collection()

if __name__ == '__main__':
    run_service()
