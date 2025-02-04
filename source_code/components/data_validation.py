from source_code.logger import logging
from source_code.execption import InsuranceException
import os, sys
from source_code.entity import config_entity, artifact_entity
import pandas as pd


class DataValidation:
    def __init__(self,data_validation_config_obj:config_entity.DataValidationConfig,
                 dataingestion_artifact_obj:artifact_entity.DataIngestArtifact):

        try:
            self.DataValidationConfig = data_validation_config_obj
            self.DataIngestionArtifact = dataingestion_artifact_obj
        except Exception as e:
            raise InsuranceException(e, sys)


    #load dataset 
    def load_data(self):
        try:
            dataset_file_path = self.DataIngestionArtifact.Dataset_file_path
            df = pd.read_csv(dataset_file_path)
            return df
        except Exception as e:
            raise InsuranceException(e, sys)

    # Define validation functions
    @staticmethod
    def validate_age(value):
        return isinstance(value, int) and value >= 0 and value <=100
    @staticmethod
    def validate_sex(value):
        return value in ["male", "female"]

    @staticmethod
    def validate_bmi(value):
        return isinstance(value, (int, float)) and value > 0 and value <=41
    @staticmethod
    def validate_children(value):
        return isinstance(value, int) and value >= 0 and value <=5
    @staticmethod
    def validate_smoker(value):
        return value in ["yes", "no"]
    @staticmethod
    def validate_region(value):
        return value in ["southeast", "southwest", "northeast", "northwest"]
    @staticmethod
    def validate_charges(value):
        return isinstance(value, (int, float)) and value >= 0

    # Apply validation checks
    



    def initiate_datavalidation(self):

        try:
            df = self.load_data()

            ##column wise validation
            expected_columns = { "age", "sex", "bmi", "children", "smoker", "region", "charges"}
            if expected_columns == set(df.columns):
                logging.info(f"All columns are present: {expected_columns}")

                invalid_rows = df[
                (~df["age"].apply(self.validate_age)) |
                (~df["sex"].apply(self.validate_sex)) |
                (~df["bmi"].apply(self.validate_bmi)) |
                (~df["children"].apply(self.validate_children)) |
                (~df["smoker"].apply(self.validate_smoker)) |
                (~df["region"].apply(self.validate_region)) |
                (~df["charges"].apply(self.validate_charges))
                ]

                # Display validation results
                # if we get any invalid rows then we will delete them
                # df.drop(invalid_rows.index, inplace=True)
                if invalid_rows.empty:
                    logging.info("No invalid rows found.")
                    print("No invalid rows found.")


                    os.makedirs(self.DataValidationConfig.data_validation_dir,exist_ok=True)
                    df.to_csv(self.DataValidationConfig.valid_data_file_path,index=False)
                    
                else:
                    logging.info(f"Invalid rows found: {invalid_rows.shape[0]}")
                    #save the invalid rows
                    print("invalid rows found")
                    os.makedirs(self.DataValidationConfig.data_validation_dir,exist_ok=True)
                   
                    invalid_rows.to_csv(self.DataValidationConfig.invalid_data_file_path,index=False)


                    # print(invalid_rows)
                    df.drop(invalid_rows.index, inplace=True)
                    df.to_csv(self.DataValidationConfig.valid_data_file_path,index=False)

            else:
                #if any of the column is not available , extra or maybe we get a renamed column 
                logging.info(f"Columns are not matching: {expected_columns} vs {set(df.columns)}")
                df=df[list(expected_columns)]



                invalid_rows = df[
                (~df["age"].apply(self.validate_age)) |
                (~df["sex"].apply(self.validate_sex)) |
                (~df["bmi"].apply(self.validate_bmi)) |
                (~df["children"].apply(self.validate_children)) |
                (~df["smoker"].apply(self.validate_smoker)) |
                (~df["region"].apply(self.validate_region)) |
                (~df["charges"].apply(self.validate_charges))]


                if invalid_rows.empty:
                    logging.info("No invalid rows found.")


                    os.makedirs(self.DataValidationConfig.data_validation_dir,exist_ok=True)
                    df.to_csv(self.DataValidationConfig.valid_data_file_path,index=False)
                    
                else:
                    logging.info(f"Invalid rows found: {invalid_rows.shape[0]}")
                    #save the invalid rows
                    os.makedirs(self.DataValidationConfig.data_validation_dir,exist_ok=True)
                   
                    invalid_rows.to_csv(self.DataValidationConfig.invalid_data_file_path,index=False)


                    # print(invalid_rows)
                    df.drop(invalid_rows.index, inplace=True)
                    df.to_csv(self.DataValidationConfig.valid_data_file_path,index=False)
            

            ##returning data validation artifact 
            data_validation_artifact = artifact_entity.DataValidationArtifact(
                Valid_data_path=self.DataValidationConfig.valid_data_file_path,
                Invalid_data_path=self.DataValidationConfig.invalid_data_file_path

            )
            logging.info(f"successfully completed data validation{self.DataValidationConfig.data_validation_dir}")

            return data_validation_artifact

        except Exception as e:
            obj = InsuranceException(e, sys)
            logging.info(f"Data validation error{obj.error_message}")
            raise InsuranceException(e, sys)
