from .data_collector import IDataCollector
from abc import ABC, abstractmethod

class IDataCollectorService(ABC):
    pass

class DataCollectorService(IDataCollectorService):
    def __init__(self):
        pass

    def add_collector(self, new_collector: IDataCollector):
        pass