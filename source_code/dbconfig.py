import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os , sys
from source_code.logger import logging 
from source_code.execption import InsuranceException
import pandas as pd
from pymongo import MongoClient
load_dotenv()

mysql_user = os.getenv("mysql_user")
mysql_password = os.getenv("mysql_password")
mysql_database_name = os.getenv("mysql_database")


mongodb_connection_string = os.getenv("mongodb_connection_string")



def connect_to_mongodb():
    try:
        # Connect to the MongoDB server
        client = MongoClient(mongodb_connection_string)  # Default port
        print("Connected to MongoDB Server")
        
    except Error as e:
        custom_exception=InsuranceException(e,sys)
        logging.error(f"unable to connect to mongodb:{custom_exception.error_message}")
        raise InsuranceException(e,sys)
        return None

def connect_to_mysql():
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(
            host='localhost',       # Replace with your host, e.g., '127.0.0.1'
            user=mysql_user,            # Replace with your MySQL username
            password=mysql_password,        # Replace with your MySQL password
            database=mysql_database_name, # Replace with your database name
            allow_local_infile=True  # Enable loading local files
        )
        
        logging.info("connecting to database")

        if connection.is_connected():
            print("Connected to MySQL Server")
            logging.info("Connected to MySQL Server")
            return connection
    except Error as e:
        custom_exception=InsuranceException(e,sys)
        logging.error(custom_exception.error_message)
        raise InsuranceException(e,sys)
    
        return None



def create_table(connection):
    try:
        cursor = connection.cursor()
        # Define the table structure
        create_table_query = """
        CREATE TABLE IF NOT EXISTS insurance_data (
            bmi FLOAT NOT NULL,
            children INT NOT NULL,
            smoker VARCHAR(10) NOT NULL,
            region VARCHAR(50) NOT NULL,
            charges FLOAT NOT NULL
        );
        """
        cursor.execute(create_table_query)
        print("Table 'insurance_data' created successfully (or already exists).")
    except Error as e:
        raise InsuranceException(e,sys)

def Dump_data_sql(connection, csv_file_path):
    try:
        cursor = connection.cursor()
        # Use the LOCAL option to load from a local file
        load_data_query = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path}'
        INTO TABLE insurance_data
        FIELDS TERMINATED BY ','   -- Column separator
        ENCLOSED BY '"'            -- Text delimiter (optional)
        LINES TERMINATED BY '\\n'  -- Row delimiter
        IGNORE 1 LINES;           -- Ignore header row
        """
        cursor.execute(load_data_query)
        connection.commit()  # Commit the changes to the database
        print("Data loaded successfully from CSV.")
    except Error as e:
        print(f"Error loading data from CSV: {e}")

# Connect to MySQL
connection = connect_to_mysql()
if connection:
    # Create the table if it doesn't exist
    create_table(connection)
    
    # Specify the path to your CSV file
    csv_file_path = './firsthalf_data.csv'  # Update this path
    
    # Load data from CSV into the table
    Dump_data_sql(connection, csv_file_path)

    # Close connection
    connection.close()
    print("Connection closed.")

