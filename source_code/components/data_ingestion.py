from source_code.logger import logging
from source_code.execption import InsuranceException
import os , sys
from source_code.dbconfig import connect_to_mongodb, connect_to_mysql

mongodb_database_name = os.getenv("mongodb_db")
print(mongodb_database_name)


# class DataIngestion:
#     def __init__(self, ):
#         try:
#             pass
#         except Exception as e:
#             raise InsuranceException(e,sys)
           