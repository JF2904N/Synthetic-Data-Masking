import configparser #config parser for importing configuration file 

import csv #csv file for reading and writing csv files 

from faker import Faker #faker class from the faker module for generating fake data

import uuid #uuid module for generating universally unique identifiers

import random #random module for generating random numbers 

import secrets #secrets module for generating cyptographically strong random numbers 

import hashlib #hashlib module for performing cryptographic hash functions

import os #os module for interacting with the operating system

import pyodbc
import time

def read_config(filename="C:\\DataGenerator\\config\\customerData.cfg"): #FUNCTION to read configuration file 
    config = configparser.ConfigParser() #create instance of ConfigParser 
    config.read(filename) # read the specified configuration file 
    return config["customer_data"]  # Return the section named "customer_data" from the parsed configuration

def generate_synthetic_data_batch(num_records, config, batch_size=1000):
    for _ in range(0, num_records, batch_size):
        yield generate_synthetic_data(min(batch_size, num_records), config)

def generate_synthetic_data(num_records, config): #FUNCTION to generate synthetic data 
    fake = Faker() #create an instance of faker for generating fake data
    data = []
    used_uids = set() #empty set to keep track of used identifiers
    used_guids = set() #empty set to keep track of used GUIDs
    used_emails = set() #set used emails
    used_sin_numbers = set() #used sin num
    
    for _ in range(num_records):
        guid = str(uuid.uuid4()) #creates unique (GUID)
        salt = secrets.token_hex(8)  #8-byte salt 
        first_name = fake.first_name() 
        last_name = fake.last_name()
        birth_date = fake.date_of_birth(minimum_age=5, maximum_age=100)
        name_and_birthdate = (first_name, last_name, birth_date)   #create a tuple of name and birthdate 
        email_domain = random.choice(eval(config["domains"])) #select random email domain from configuration
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,50)}@{email_domain}"
        hash_key = hashlib.sha256(uuid.uuid4().hex.encode()).hexdigest() # Generate hash key using UUID and SHA-256 hash
        sin_like_number = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(100, 999)}"
        unique_identifier = hashlib.sha256((first_name + last_name + sin_like_number + salt).encode()).hexdigest()
        
        # Ensure uniqueness of UID
        while unique_identifier in used_uids:
            unique_identifier = hashlib.sha256((first_name + last_name + sin_like_number + salt).encode()).hexdigest()
        used_uids.add(unique_identifier)
        
        # Ensure uniqueness of GUID
        while guid in used_guids:
            guid = str(uuid.uuid4()) #regenerate guid if it is already used 
        used_guids.add(guid)

        #sin_like_number = None # Initialize SIN-like number variable
        # Ensure uniqueness of SIN-like numbers
        while sin_like_number in used_sin_numbers:
            sin_like_number = f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(100, 999)}"
        used_sin_numbers.add(sin_like_number)

        # Ensure uniqueness of email
        while email in used_emails:
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,50)}@{email_domain}"
        used_emails.add(email)


        data.append({
            "UID": unique_identifier,
            "GUID": guid,
            "FIRST_NAME": first_name,
            "LAST_NAME": last_name,
            "EMAIL": email,
            "BIRTH_DATE": birth_date,
            "SIN": sin_like_number,
            "SALT": salt,
            "HASHKEY": hash_key,
        })

    return data
        
        
def save_to_mssql(data, conn_str):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for row in data:
            cursor.execute('''
                INSERT INTO [MOCK_DATA].[SYN_CUSTOMER] (UID, GUID, FIRST_NAME, LAST_NAME, EMAIL, BIRTH_DATE, SIN, SALT, HASHKEY)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', row["UID"], row["GUID"], row["FIRST_NAME"], row["LAST_NAME"], row["EMAIL"], row["BIRTH_DATE"], row["SIN"], row["SALT"], row["HASHKEY"])
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully into MSSQL table.")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
       
  
if __name__ == "__main__":
    config = read_config()
    num_records = int(input("Enter the number of synthetic records to generate: "))

    server = r'SQL' #adjusted due to privacy
    database = 'DataLake' #adjusted due to privacy
    conn_str = f'DRIVER={{ODBC DRIVER 10 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;' #adjusted due to privacy

    connection = pyodbc.connect(conn_str, autocommit=False)
    total_records_inserted = 0  # Initialize total records inserted counter

    while total_records_inserted < num_records:
        if not connection:
            connection = pyodbc.connect(conn_str)
            cursor = connection.cursor()
        try:
            for batch in generate_synthetic_data_batch(num_records - total_records_inserted, config, batch_size=1000):
                save_to_mssql(batch, conn_str)
                total_records_inserted += len(batch)  # Update total records inserted counter

            if total_records_inserted >= num_records:
                break  # Exit the loop if total records inserted reaches or exceeds num_records
            
        except pyodbc.Error as pe:
            print("Error:", pe)
            if pe.args[0] == "08S01":
                try:
                    connection.close()
                except:
                    pass
                connection = None
                continue
            raise  # Re-raise any other exception





#986343
