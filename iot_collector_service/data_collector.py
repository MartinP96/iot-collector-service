from .mqtt_client import MqttClientPaho
import json

data_topic = "porenta/martin_room/air_quality/data/measurements"
beat_topic = "porenta/martin_room/air_quality/sys/beat"

class DataCollector:

    def __init__(self):
        # Define MQTT Client
        self.mqtt_client = MqttClientPaho()
        self.mqtt_client.mqtt_client_connect(
            usr="admin",
            password="admin",
            broker="192.168.0.101",
            port=1883
        )
        self.mqtt_client.mqtt_client_subscribe(topic=data_topic)
        self.mqtt_client.mqtt_client_subscribe(topic=beat_topic)
        self.mqtt_client.loop_start()

    def run_collector(self):

        while 1:
            data = self.mqtt_client.data_queue.get()
            # Parse data
            if data["topic"] == data_topic:
                # Parse string to dict
                dict_data = json.loads(data["data"])
                print(f"Timestamp: {dict_data['timestamp']}\nTemp: {dict_data['temperature']}\n"
                      f"Hum: {dict_data['humidity']}\nCO2: {dict_data['co2']}\n")
