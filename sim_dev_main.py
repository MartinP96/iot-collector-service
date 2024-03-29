from iot_sim_device.sim_device import SimDevice, SimDeviceConfiguration
from iot_collector_service.mqtt_client import MqttClientPaho
import csv

if __name__ == '__main__':

    sim_configuration_path = "sim_configuration/"

    # Read sim devices configuration
    # Read configuration
    with open(f"{sim_configuration_path}sim_device_configuration.csv") as f:
        reader = csv.DictReader(f)
        sim_configuration = list(reader)

    # Read measurements
    with open(f"{sim_configuration_path}sim_device_measurements.csv") as f:
        reader = csv.DictReader(f)
        sim_measurements = list(reader)

    # Define MQtt client
    #mqtt_cli =

    sim_devices = []
    for conf in sim_configuration:

        measurements = list(filter(lambda x: x["device_name"] == conf["device_name"], sim_measurements))
        sim_dev_conf = SimDeviceConfiguration(broker_usr="admin",
                                              broker_ip="localhost",
                                              broker_password="admin",
                                              broker_port=1883,
                                              subscribe_topic=conf["subscribe_topic"],
                                              publish_topic=conf["publish_topic"],
                                              publish_interval=float(conf["publish_interval"]),
                                              device_measurements=measurements)

        sim_dev = SimDevice(client=MqttClientPaho(),
                            device_configuration=sim_dev_conf)
        sim_devices.append(sim_dev)
        sim_dev.run_device()
