"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .mqtt_client import MqttClient
import random

def run_service():
    print("IOT data collector service v0.1")
    test_client = MqttClient()
    test_client.mqtt_client_connect(usr="admin",
                                    password="admin",
                                    broker="192.168.0.101",
                                    port=1883)

if __name__ == '__main__':
    run_service()
