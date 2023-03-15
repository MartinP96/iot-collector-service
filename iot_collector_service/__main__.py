"""
    File name: __main__.py
    Date: 10.01.2023
"""

from .data_collector_service import DataCollectorService
from .data_collector import MqttDataCollector, CollectorConfiguration
from .mqtt_client import MqttClientPaho
from .sql_client import MySqlClient
from .sql_service import SQLService
import json

def run_service():

    # Define SQL client
    sql_client = MySqlClient()

    # Define SQL service
    sql_service = SQLService(sql_client=sql_client)
    sql_service.connect_service()

    collector_configuration = sql_service.read_iot_configuration()
    device_configuration = sql_service.read_device_configuration()
    topic_configuration = sql_service.read_topic_configuration()

    service = DataCollectorService()

    for collector_conf in collector_configuration:
        current_collector_topic_configuration = []
        for i in topic_configuration:
            if i["iot_configuration"] == collector_conf.configuration_id:
                current_collector_topic_configuration.append(i)
        collector = MqttDataCollector(MqttClientPaho())
        collector.set_configuration(collector_conf, current_collector_topic_configuration)
        service.add_collector(collector)

    # Start collecting data from MQTT
    service.start_collection()

    try:
        while 1:
            response = service.get_data()
            topic = response["topic"]
            try:  # TMP: Začasna rešitev, v prihodnje bodo vse naprave pošiljale podatke v JSON formatu
                data = json.loads(response["data"])
            except:
                data = response["data"]

            # Assign measurement to device
            for i in topic_configuration:
                if i["topic"] == topic:
                    if i["topic_type"] == 1:  # Measurement
                        measurement = {"device_id": i["device_id"], "topic_id": i["topic_id"]}
                        # Parse measurement packet
                        for m in data:
                            if m != "timestamp":  # TMP: Začasna rešitev, dodelati naprave da pošljejo zraven timestmp
                                measurement["measurement_type_id"] = m
                                measurement["value"] = data[m]
                                print(measurement)

                                sql_service.write_measurement_to_sql(measurement)

    except KeyboardInterrupt:
        service.stop_collection()
        sql_client.disconnect_sql()
        print('Service interrupted')
