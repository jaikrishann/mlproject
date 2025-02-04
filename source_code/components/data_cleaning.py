from source_code.logger import logging
from source_code.execption import InsuranceException
from source_code.entity import config_entity,artifact_entity
import os , sys 
import pandas as pd


class DataCleaning:
    def __init__(self,data_cleaning_config_obj:config_entity.DataCleaningConfig,
                 datavalidation_artifact:artifact_entity.DataValidationArtifact):
        try:
            self.data_cleaning_config_obj=data_cleaning_config_obj
            self.datavalidation_artifact_obj=datavalidation_artifact
        except Exception as e:
            raise InsuranceException(e,sys)
        

    ##remove irrelevant features   --> already done 
    ##load validate data
    def load_validate_data(self)->pd.DataFrame:
        try:
            df=pd.read_csv(self.datavalidation_artifact_obj.Valid_data_path)
            return df
        except Exception as e:
            raise InsuranceException(e,sys)

    ##remove duplicate data 
    def remove_duplicate_data(self,validate_data)->pd.DataFrame:
        try:
           
            total_duplicated_rows=validate_data.duplicated().sum()
            if total_duplicated_rows>0:
                logging.info(f"Removing {total_duplicated_rows} duplicated rows")
                validate_data.drop_duplicates(inplace=True)
                logging.info(f"Removed {total_duplicated_rows} duplicated rows")
                return validate_data
            else:
                logging.info("No duplicate rows found")
                return validate_data
        except Exception as e:
            raise InsuranceException(e,sys)
        

    ##handle missing data 
    def handle_missing_data(self,df:pd.DataFrame)-> pd.DataFrame:
        try:
            missing_value_report=df.isnull().sum().to_dict()
            logging.info(f"Missing value report: {missing_value_report}")
            total_no_of_missing_values=df.isnull().sum().sum()
            ##to count no of rows that contain missing values
            total_no_of_missing_rows = df.isnull().any(axis=1).sum()
            logging.info(f"Total no of missing values: {total_no_of_missing_values}")
            logging.info(f"Total no of missing rows: {total_no_of_missing_rows}")

            ##remove missing values
            df.dropna(inplace=True)
            return df 
            #fill missing values 
            
        except Exception as e:
            raise InsuranceException(e,sys)
        
    # ##handle missing values
    # def handle_missing_values(self,df:pd.DataFrame)-> pd.DataFrame:
    #     try:
    #         numerical_col
    #     except Exception as e:
    #         raise InsuranceException(e,sys)
        
    # ##handle outliers
    # def handle_outliers(self):
    #     try:
    #         pass
    #     except Exception as e:
    #         raise InsuranceException(e,sys)
        
    
    ##handling inconsistent data --> done 

    def DataCleaningInitiate(self)->artifact_entity.DataCleaningArtifact:
        try:
            df = self.load_validate_data()
            df = self.remove_duplicate_data(df)
            df = self.handle_missing_data(df)
            logging.info("Data cleaning completed")
            ##save the clean data 
            os.makedirs(self.data_cleaning_config_obj.data_cleaning_dir,exist_ok=True)
            df.to_csv(self.data_cleaning_config_obj.clean_data_file_path,index=False) 

            datacleaning_artifact = artifact_entity.DataCleaningArtifact(
                Clean_data_path=self.data_cleaning_config_obj.clean_data_file_path
            )  
            return datacleaning_artifact
        except Exception as e:
            raise InsuranceException(e,sys)