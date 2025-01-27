from source_code.logger import logging 
from source_code.execption import InsuranceException
from source_code.dbconfig import connect_to_mongodb, connect_to_mysql


def is_mongo_connected(mongo_connection):
    if mongo_connection is not None:
        return True
    else:
        return False