from .data_collector import IDataCollector
from abc import ABC, abstractmethod

class IDataCollectorService(ABC):

    @abstractmethod
    def add_collector(self, new_collector: IDataCollector):
        pass

    @abstractmethod
    def start_collection(self):
        pass

    @abstractmethod
    def stop_collection(self):
        pass

class DataCollectorService(IDataCollectorService):
    def __init__(self):
        self.collectors_list = []

    def add_collector(self, new_collector: IDataCollector):
        self.collectors_list.append(new_collector)

    def start_collection(self):
        self.collectors_list[0].connect_collector()
        self.collectors_list[0].run_collector()
        print("Collector service start")

    def stop_collection(self):
        self.collectors_list[0].stop_collector()
        print("Collector service stop")

    def get_data(self):
        return self.collectors_list[0].get_data()
