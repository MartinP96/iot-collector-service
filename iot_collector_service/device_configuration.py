class DeviceConfiguration:
    def __init__(self, device_name: str,
                 iot_configuration: int,
                 device_id: int,
                 topic: list):
        self.device_name = device_name
        self.device_id = device_id
        self.topic = topic
        self.iot_configuration = iot_configuration
