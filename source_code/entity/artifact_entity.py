#in this file we create variables but here variables are used to store output of the components
from dataclasses import dataclass


@dataclass
class DataIngestArtifact:
    Dataset_file_path : str
    Train_df_path : str
    Test_df_path : str


@dataclass
class DataValidationArtifact:
    Valid_data_path : str
    Invalid_data_path : str