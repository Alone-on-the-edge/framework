from abc import ABC, abstractmethod


class DataFlowPipeline(ABC):

    @abstractmethod
    def read(self):
        """Reading the data from defined source"""
        pass

    @abstractmethod
    def write(self):
        """writing the data to defined destination"""
        pass