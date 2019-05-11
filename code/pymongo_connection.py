from pymongo import MongoClient

class MongodbConnector:

    def __init__(self,hostname,port,db_name):
        self._db = MongoClient(hostname, port)[db_name]
        self._collection = None

    def get_collection(self,collection_name):
        self._collection = self._db[collection_name]
        return self._collection

