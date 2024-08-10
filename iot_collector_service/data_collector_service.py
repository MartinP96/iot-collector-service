from .data_collector import IDataCollector
from abc import ABC, abstractmethod
import os

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

    def add_collector(self, new_collector: IDataCollector, collector_name=""):
        self.collectors_list.append(new_collector)
        self.create_folder_structure()

    def start_collection(self):
        status = self.collectors_list[0].connect_collector()
        if status == 1:
            print("Collector connected")
            self.collectors_list[0].run_collector()
            print("Collector service start")
        else:
            print("Collector not connected!")

    def resume_collection(self):
        self.collectors_list[0].subscribe_topic()

    def hold_collection(self):
        self.collectors_list[0].unsubscribe_all_topic()
        print("Collector service held")

    def stop_collection(self):
        self.collectors_list[0].stop_collector()
        self.collectors_list[0].disconnect_collector()
        del self.collectors_list[0]
        print("Collector service stop")

    def get_data(self):
        return self.collectors_list[0].get_data()  # TODO: Naredi da deluje za več kolektorjev

    def publish_data(self, data):
        self.collectors_list[0].publish_data(data)  # TODO: Naredi da deluje za več kolektorjev

    def create_folder_structure(self):
        # Create collector log folder
        if not os.path.exists("collector_logs/"):
            os.makedirs("collector_logs/")

        # Create measurement data folder
        if not os.path.exists("collector_data/"):
            os.makedirs("collector_data/")
        else:
            #Create sub folder of each collector
            for collector in self.collectors_list:
                pass
