"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector import MqttDataCollector
from .mqtt_client import MqttClientPaho

def run_service():
    #collector = DataCollector()
    collector = MqttDataCollector(MqttClientPaho())
    collector.subscribe_topic("porenta/martin_room/air_quality/data/measurements")
    collector.subscribe_topic("porenta/martin_room/air_quality/sys/beat")

    collector.run_collector()

if __name__ == '__main__':
    run_service()
