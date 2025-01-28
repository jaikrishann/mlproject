from source_code.logger import logging
from source_code.execption import InsuranceException
import sys ,os
from source_code.entity import config_entity
from source_code.dbconfig import connect_to_mongodb, connect_to_mysql
from source_code.utlis import is_mongo_connected
from source_code.components.data_ingestion import DataIngestion



# logging.info("hello world")
# logging.debug("hello world")
# logging.warning("hello world")
# logging.critical("hello world")
# logging.error("hello world")


# try:
#     10/0
# except Exception as e:

#     obj =  InsuranceException(error_message=e,error_details= sys )
#     logging.warning(obj.error_message)
#     print(obj.error_message)


training_pipeline_obj=config_entity.TrainingPipelineConfig()
dataingestion_config_obj=config_entity.DataIngestionConfig(training_pipeline_config_obj=training_pipeline_obj)

dataingestion_obj = DataIngestion(dataingestion_config_obj=dataingestion_config_obj)

data_ingestion_artifact=dataingestion_obj.load_datasets()

print("data file path",data_ingestion_artifact.Dataset_file_path)
print("train file path",data_ingestion_artifact.Train_df_path)
print("test file path",data_ingestion_artifact.Test_df_path)



# mongo_conn=connect_to_mongodb(
#     mongodb_connection_string=dataingestion_config_obj.mongodb_connection)

# sql_conn = connect_to_mysql(mysql_user=dataingestion_config_obj.mysql_user,
#                             mysql_password=dataingestion_config_obj.mysql_password,
#                             mysql_database_name=dataingestion_config_obj.mysql_database_name)


# if is_mongo_connected(mongo_conn):
#     dataingestion_obj=DataIngestion(dataingestion_config_obj=dataingestion_config_obj)
#     dataingestion_obj.load_datasets()
# else:
#     print("MongoDB is not connected")





# print(training_pipeline_obj.artifact_dir)
# print(dataingestion_config_obj.dataingestion_dir)
# print(dataingestion_config_obj.dataset_path)
# print(dataingestion_config_obj.mysql_user)
# print(dataingestion_config_obj.mysql_password)
# print(dataingestion_config_obj.mysql_database_name)
# print(dataingestion_config_obj.momgodb_connection)
# print(dataingestion_config_obj.mongodb_database)
# print(dataingestion_config_obj.mongodb_collection)


# os.makedirs(training_pipeline_obj.artifact_dir,exist_ok=True)
