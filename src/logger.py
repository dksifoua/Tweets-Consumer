import sys
import logging

from src.singleton import Singleton


class Logger(Singleton):
    __FORMAT = '%(asctime)s - %(levelname)s - (%(threadName)s) - %(message)s'

    def __init__(self, name: str = __name__):
        super().__init__()

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        log_formatter = logging.Formatter(self.__class__.__FORMAT)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler('Tweets-Consumer.log')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.INFO)

        if logger.handlers:
            logger.handlers = []

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        self.__logger = logger

        self.__class__.__instance = self

    @property
    def logger(self):
        return self.__logger

    def debug(self, message: str):
        self.__logger.debug(message)

    def info(self, message: str):
        self.__logger.info(message)

    def warn(self, message: str):
        self.__logger.warning(message)

    def error(self, message: str):
        self.__logger.error(message)

    def critical(self, message: str):
        self.__logger.critical(message)

    def exception(self, message: str):
        self.__logger.exception(message)
