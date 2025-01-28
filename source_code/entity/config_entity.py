#we create variables in it which are used in processing of output
from source_code.logger import logging
from source_code.execption import InsuranceException
from datetime import datetime
import os, sys 


class TrainingPipelineConfig:
    def __init__(self ):
        try: 
            logging.info("Initializing PipelineConfig and artifact configuration")
            self.artifact_dir = os.path.join(os.getcwd(),"Artifcat",datetime.now().strftime("%d-%m-%y %H-%M-%S"))
        except Exception as e:
            obj=InsuranceException(e,sys)
            logging.info(obj.error_message)
            raise InsuranceException(e,sys)



class DataIngestionConfig:
    def __init__(self,training_pipeline_config_obj:TrainingPipelineConfig):

        try:
            logging.info("Initializing DataIngestionConfig variables")
            self.dataingestion_dir = os.path.join(training_pipeline_config_obj.artifact_dir,"data ingestion")
            self.dataset_path = os.path.join(self.dataingestion_dir,"Dataset")
            self.mysql_user = os.getenv("mysql_user")
            self.mysql_password = os.getenv("mysql_password")
            self.mysql_database_name = os.getenv("mysql_database")
            self.mongodb_connection = os.getenv("mongodb_connection_string")
            self.mongodb_database = os.getenv("mongodb_db")
            self.mongodb_collection = os.getenv("mongodb_collection")
            self.test_size = 0.2
            self.dataset_filename = "insurance.csv"
            self.train_set_filename = "train.csv"
            self.test_set_filename = "test.csv"
        except Exception as e:
            raise InsuranceException(e,sys)
