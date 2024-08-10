class CollectorConfiguration:

    def __init__(self, configuration_id: int,
                 usr: str,
                 password: str,
                 ip_addr: str,
                 port: int,
                 device_name: str):
        self.configuration_id = configuration_id
        self.usr = usr
        self.password = password
        self.ip_addr = ip_addr
        self.port = port
        self.device_name = device_name
