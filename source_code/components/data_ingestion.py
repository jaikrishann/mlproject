from source_code.logger import logging
from source_code.execption import InsuranceException
import os , sys
from source_code.dbconfig import connect_to_mongodb, connect_to_mysql
from source_code.entity import config_entity,artifact_entity
from sklearn.model_selection import train_test_split
import pandas as pd




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
            df=pd.DataFrame(list(mongo_documents))
            print(df)
            df.drop("_id",axis=1,inplace=True)
            # to split the data
            logging.info('spliting the data into train and test data')
            train_df,test_df = train_test_split(df,test_size=self.dataingestion_obj.test_size,random_state=42)


            os.makedirs(self.dataingestion_obj.dataingestion_dir,exist_ok=True)
            os.makedirs(self.dataingestion_obj.dataset_path,exist_ok=True)
            ##making path 
            dataset_file_path = os.path.join(self.dataingestion_obj.dataset_path,self.dataingestion_obj.dataset_filename)
            train_df_path = os.path.join(self.dataingestion_obj.dataset_path,self.dataingestion_obj.train_set_filename)
            test_df_path = os.path.join(self.dataingestion_obj.dataset_path,self.dataingestion_obj.test_set_filename)

            df.to_csv(os.path.join(dataset_file_path),index=False)
            logging.info(f"successfully saved the dataset in the artifact directory {dataset_file_path}")
            train_df.to_csv(train_df_path,index=False)
            logging.info(f"successfully saved the train dataset in the artifact directory {train_df_path}")
            test_df.to_csv(test_df_path,index=False)
            logging.info(f"successfully saved the test dataset in the artifact directory {test_df_path}")

            dataingestion_artifact=artifact_entity.DataIngestArtifact(Dataset_file_path=dataset_file_path,
                                               Train_df_path=train_df_path,
                                               Test_df_path=test_df_path)
            return dataingestion_artifact


            # mysql_connection=connect_to_mysql(mysql_user=self.dataingestion_obj.mysql_user
            #                                   ,mysql_password=self.dataingestion_obj.mysql_password
            #                                   ,mysql_database_name=self.dataingestion_obj.mysql_database_name)
            # cursor = mysql_connection.cursor()
            # cursor.execute("SELECT * FROM insurance_data")
            # rows = cursor.fetchall()
            # for row in rows:
            #     print(row)

        except Exception as e:
            raise InsuranceException(e,sys)