from source_code.logger import logging
from source_code.execption import InsuranceException
import os , sys
from source_code.dbconfig import connect_to_mongodb, connect_to_mysql
from source_code.entity import config_entity,artifact_entity




class DataIngestion:
    def __init__(self, dataingestion_config_obj: config_entity.DataIngestionConfig):
        try:
            self.dataingestion_obj = dataingestion_config_obj
        except Exception as e:
            raise InsuranceException(e,sys)
           

##loading the dataset 

    def load_datasets(self):
        """
        This function is used to load the dataset from the multiple databases
          like mysql and mongodb and store it in the artifact directory 
        """

        try:
            logging.info("Loading datasets from databases")
            mongoconnection=connect_to_mongodb(mongodb_connection_string = self.dataingestion_obj.mongodb_connection)
            
            mongodb_database=mongoconnection[self.dataingestion_obj.mongodb_database]
            mongo_collection = mongodb_database[self.dataingestion_obj.mongodb_collection] 
            mongo_documents = mongo_collection.find()
            logging.info("loading data from mongodb")

            mysql_connection=connect_to_mysql(mysql_user=self.dataingestion_obj.mysql_user
                                              ,mysql_password=self.dataingestion_obj.mysql_password
                                              ,mysql_database_name=self.dataingestion_obj.mysql_database_name)
            cursor = mysql_connection.cursor()
            cursor.execute("SELECT * FROM insurance_data")
            rows = cursor.fetchall()
            for row in rows:
                print(row)

        except Exception as e:
            raise InsuranceException(e,sys)