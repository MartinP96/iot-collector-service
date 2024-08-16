from .data_collector import IDataCollector
from abc import ABC, abstractmethod
import os
import logging
from datetime import datetime


DATA_LOG_PATH = "collector_logs/"

class DataCollectorService_Packet():
    """
        Object for storing data from all device collectors
    """
    def __init__(self):
        pass

class IDataCollectorService(ABC):

    @abstractmethod
    def add_collector(self, new_collector: IDataCollector, collector_name=""):
        pass

    @abstractmethod
    def start_collection(self):
        pass

    @abstractmethod
    def stop_collection(self):
        pass

    @abstractmethod
    def create_folder_structure(self):
        pass

class DataCollectorService(IDataCollectorService):
    def __init__(self):
        self.collectors_list = []
        self.create_folder_structure()

        # Create datalog
        logging.basicConfig(filename=f"{DATA_LOG_PATH}log_{datetime.today().strftime('%Y%m%d')}.log",
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            level=logging.INFO,
                            datefmt='%Y-%m-%d %H:%M:%S')

    def add_collector(self, new_collector: IDataCollector, collector_name=""):
        self.collectors_list.append(new_collector)
        self.create_folder_structure()

        # Write to log
        logging.info(f"Adding collector to collector service: Device Id:{new_collector.device_settings.device_id}, "
                     f"Device name: {new_collector.device_settings.device_name}")

    def start_collection(self):
        # Start collection for each collector
        for collector in self.collectors_list:
            status = collector.connect_collector()
            if status == 1:
                print("Collector connected")
                collector.run_collector()
                print("Collector service start")

                # Write to log
                logging.info(f"Starting collection: Device Id:{collector.device_settings.device_id}, "
                             f"Device name: {collector.device_settings.device_name}")
            else:
                print("Collector not connected!")

                # Write to log
                logging.info(f"Collector not connected!: Device Id:{collector.device_settings.device_id}, "
                             f"Device name: {collector.device_settings.device_name}")

    def resume_collection(self):

        for collector in self.collectors_list:
            collector.subscribe_topic()

            # Write to log
            logging.info(f"Resuming collection: Device Id:{collector.device_settings.device_id}, "
                         f"Device name: {collector.device_settings.device_name}")

    def hold_collection(self):

        for collector in self.collectors_list:
            collector.unsubscribe_all_topic()
            print("Collector service held")

            # Write to log
            logging.info(f"Holding collection: Device Id:{collector.device_settings.device_id}, "
                         f"Device name: {collector.device_settings.device_name}")

    def stop_collection(self):

        for collector in self.collectors_list:
            collector.stop_collector()
            collector.disconnect_collector()

            print("Collector service stop")

            # Write to log
            logging.info(f"Stopping collection: Device Id:{collector.device_settings.device_id}, "
                         f"Device name: {collector.device_settings.device_name}")

        del collector

    def get_data(self):
        data_list = []
        for collector in self.collectors_list:
            data = collector.get_data()
            # Check if data not empty
            if data:
                data_list.append(data)
        return data_list

    def publish_data(self, data):
        self.collectors_list[0].publish_data(data)  # TODO: Naredi da deluje za več kolektorjev

    def create_folder_structure(self):
        # Create collector log folder
        if not os.path.exists(DATA_LOG_PATH):
            os.makedirs(DATA_LOG_PATH)

        # Create measurement data folder
        if not os.path.exists("collector_data/"):
            os.makedirs("collector_data/")
        else:
            # Create sub folder of each collector
            for collector in self.collectors_list:
                if not os.path.exists(f"collector_data/{collector.device_settings.device_name}"):
                    os.makedirs(f"collector_data/{collector.device_settings.device_name}")
