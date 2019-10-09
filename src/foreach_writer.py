import pymongo

from src.logger import Logger


class ForeachWriter:
    logger = Logger.get_instance()

    def __init__(self, collection, database='dynamas'):
        self.__mongo_client = None
        self.__database = database
        self.__collection = collection

    def open(self, partition_id, epoch_id):
        try:
            self.__mongo_client = pymongo.MongoClient()
        except Exception as e:
            self.__class__.logger.error(f'Failed to connect to mongodb with exception => {e}')
            return False
        return True

    def process(self, row):
        self.__mongo_client[self.__database][self.__collection].insert_one(row.asDict(recursive=True))

    def close(self, error):
        if error:
            self.__class__.logger.error(f'Closed {self.__collection} connection with error: {error}')