"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector import DataCollector, MqttDataCollector
from .mqtt_client import MqttClientPaho, IMqttClient

import json

def run_service():
    #collector = DataCollector()
    collector = MqttDataCollector(MqttClientPaho())
    collector.run_collector()

if __name__ == '__main__':
    run_service()
