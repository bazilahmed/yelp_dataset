# program to import yelp data to MongoDB 
import pymongo
import pandas as pd
import json
from pprint import pprint
import os

# create a connection
conn = "mongodb://localhost:27017"

# pass connnection to the pymongo instance
client = pymongo.MongoClient(conn)


# connect to a database. It will create one if not already exists
db = client.yelp

# drop collections if exists 
db.business.drop()

# print the list of databases
# print(client.list_database_names())

# function to load data to MongoDB
def load_json_to_mongo(file_path):
    try:
        # get the file name from the file path 
        file_name = os.path.basename(file_path)

        # create an empty list to add the data
        data = []

        # read the json line text file
        with open(file_path, encoding="utf-8") as json_file:
            for line in json_file:
                data.append(json.loads(line))

        # creates a collection in the database and inserts all the records as documents
        # strip the file name off its extension 
        file_name_base = os.path.splitext(file_name)[0]

        # create a collection with the file name
        collection = file_name_base

        # insert data into the collection
        # this is not working so we will hard code the collection name for now
        db.business.insert_many(data)

        # print the closing message
        print(f"Upload {file_name} data successfully!")
        print(f"Uploaded {db.business.count_documents({})} records.")
        print("----------------------------------------------------------------------------")

    except:
        print("Failed to upload the data. \nHint** Please check the filename has spaces.")


# define the file_path to load the data from 
file_path = "yelp_dataset/business.json"

# call the load data load function  to load the data to Mongodb
load_json_to_mongo(file_path)


