from abc import abstractmethod

from utils import DataSource


class Orchestrator:

    def __init__(self, logger_name: str = None, logger_file_name: str = None):
        self.logger = DataSource.create_logger(logger_name, logger_file_name)

    @abstractmethod
    def run(self, instructions=None) -> None:
        """General function for running orchestrators. All of them must accept instructions,
        which is a dict, which the specific options for running either from terminal or external
        json"""
        pass
