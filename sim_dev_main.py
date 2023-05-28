from iot_sim_device.sim_device import SimDevice, SimDeviceConfiguration
from iot_collector_service.mqtt_client import MqttClientPaho

if __name__ == '__main__':

    mqtt_cli = MqttClientPaho()
    sim_dev_conf = SimDeviceConfiguration(broker_usr="admin",
                                          broker_ip="localhost",
                                          broker_password="admin",
                                          broker_port=1883,
                                          subscribe_topic="",
                                          publish_topic="porenta/martin_room/air_quality_1/data/measurements")

    sim_dev = SimDevice(client=mqtt_cli,
                        device_configuration=sim_dev_conf)

    sim_dev.run_device()

