"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho
from .sql_client import  MySqlClient
import json

def run_service():

    # Topic definitions
    data_topic = "porenta/martin_room/air_quality/data/measurements"
    beat_topic = "porenta/martin_room/air_quality/sys/beat"

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

    # Define SQL client
    sql_client = MySqlClient()
    sql_client.connect_sql(
        host="192.168.178.36",
        database="test_db",
        user="remote_admin",
        password="nitram123@!"
    )

    # Test data collection every 10 seconds
    tmp = 0
    while 1:
        data = service.get_data()

        if data["topic"] == data_topic:
            # Parse string to dict
            dict_data = json.loads(data["data"])

            if tmp > 10:
                col_name = ["temperature", "humidity", "co2", "device_id"]
                col_val = [dict_data['temperature'], dict_data['humidity'], dict_data['co2'], 1]
                sql_client.insert_sql("iot_air_quality_measurements", col_name, col_val)

                print(f"Timestamp: {dict_data['timestamp']}\nTemp: {dict_data['temperature']}\n"
                      f"Hum: {dict_data['humidity']}\nCO2: {dict_data['co2']}\n")

                tmp = 0
            tmp += 1

    service.stop_collection()
    sql_client.disconnect_sql()

if __name__ == '__main__':
    run_service()
