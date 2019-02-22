# # program to 
# load the restaurants data from MongoDB by filtering 
# creating a dataframe
# perform the data cleansing, munging
# save as csv output


# import the dependencies
import pandas as pd
import pymongo
from pprint import pprint
import numpy as np

# set the pandas option to display full column data instead of truncated view of 50 chars
pd.set_option('display.max_colwidth', -1)


# create a connection to pymongo database.
# make sure the server is up and running
conn = "mongodb://localhost:27017"

# pass connection to pymongo instance
client = pymongo.MongoClient(conn)
print("DB connection succesful!\n")

# connect to the database. It will create one if not already exists
# database name = yelp
db = client.yelp


# get only the documents which have restaurants data by filtering
# collection name = business
# keyword = Retaurant
print("Importing the documents from MongoDB with the keyword: Restaurant")
total_docs = db.business.find({'categories': {"$regex" : "Restaurant" }}).count()
# pprint(db.business.find_one({'categories': {"$regex" : "Restaurant" }}))
print(f"Total number of documents found: {total_docs}")

# create a cursor and save all the MongoDB search results
restaurants_mongo = db.business.find({'categories': {'$regex' : 'Restaurant'}})

# create an empty list to iterate through and append data in it
restaurants = []

for restaurant in restaurants_mongo:
    restaurants.append(restaurant)


# check if all the records got appended
print(f"Number of records appended: {len(restaurants)}")

print("Printing sample data\n")
# cleanse the data and retreive only selected fields 
pprint(restaurants[59386])

# get the _id, address, attributes.alcohol,
# attributes.BusinessAcceptsCreditCards, attributes.NoiseLevel,
# attributes.RestaurantsGoodForGroups, attributes.RestaurantsReservations
# categories, city, is_open, 
# 'latitude','longitude', 'name', 'postal_code','review_count', 'stars', state'

res_df = pd.DataFrame(restaurants)

res_df.head(3)


# create a function to extract the value from the nested attribute dictionary if that specific attribute exists

print("\n\nData transformations started")

def find_value(restaurant, attribute):
    try: 
        return restaurant["attributes"][attribute]
    except:
        return np.nan

# create new columns to extract the specific attribute values from the restaurants list 
res_df["Alcohol"] = [find_value(restaurant, "Alcohol") for restaurant in restaurants]
res_df["Business Accepts Credit Cards"] = [find_value(restaurant, "BusinessAcceptsCreditCards") for restaurant in restaurants]
res_df["Noise Level"] = [find_value(restaurant, "NoiseLevel") for restaurant in restaurants]
res_df["Restaurants Good For Groups"] = [find_value(restaurant, "RestaurantsGoodForGroups") for restaurant in restaurants]
res_df["Restaurants Reservations"] = [find_value(restaurant, "RestaurantsReservations") for restaurant in restaurants]


# res_df.columns

# clean the dataframe to have unwanted columns removed
res_df_clean = res_df[['address', 'categories', 'city',
       'is_open', 'latitude', 'longitude', 'name', 'postal_code',
       'review_count', 'stars', 'state', 'Alcohol',
       'Business Accepts Credit Cards', 'Noise Level', 'Restaurants Good For Groups',
       'Restaurants Reservations']]

# res_df_clean.head(3)


# # # perform some data analysis
# res_df_clean.describe()
# res_df_clean.info()
# res_df_clean.nunique()


# perform some clean up activities on this dataset
# rename the columns
print("Data cleansing started")
res_df_cleaner = res_df_clean.rename(columns={'address':'Address', 'categories':'Categories', 'city':'City',
       'is_open':'Is Open', 'latitude': 'Latitude', 'longitude':'Longitude', 'name':'Name', 'postal_code':'ZIP',
       'review_count':'Total Reviews', 'stars':'Star Rating', 'state':'State'})

# res_df_cleaner.head(3)


# Data munging
print("Data munging started")
# find the total open and closed restaurants
# res_df_cleaner['Is Open'].value_counts()

# change the values for Is Open column from 1 and 0 to yes and no
res_df_cleaner['Is Open'] = res_df_cleaner['Is Open'].apply(lambda x: 'Yes' if x==1 else 'No')

# res_df_cleaner['Is Open'].value_counts()

# # sample test code
# from itertools import product
# a = pd.DataFrame(list(product([1, 0], [1, 0])), columns=['x', 'y'])

# a['x'] = a['y'].apply(lambda x: 'Yes' if x==1 else 'No')
# a

# check the values in Alcohol column  
# res_df_cleaner['Alcohol'].value_counts()

# replace values in Alcohol column with meaningful text 
res_df_cleaner['Alcohol'] = res_df_cleaner['Alcohol'].replace({"u'none'":"None","'none'": "None",
                                                               "u'full_bar'":"Full Bar", "'full_bar'":"Full Bar",
                                                              "u'beer_and_wine'":"Beer and Wine",
                                                              "'beer_and_wine'":"Beer and Wine"                                                              
                                                              })


# res_df_cleaner['Alcohol'].value_counts()


# check the values in Noise Level column 
# res_df_cleaner['Noise Level'].value_counts()


# replace values in Alcohol column with meaningful text 
res_df_cleaner['Noise Level'] = res_df_cleaner['Noise Level'].replace({"u'average'":"Average","'average'":"Average",
                                                               "u'quiet'":"Quiet", "'quiet'":"Quiet",
                                                              "u'loud'":"Loud", "'loud'":"Loud",
                                                              "u'very_loud'":"Very Loud", "'very_loud'":"Very Loud"                                                               
                                                              })


# res_df_cleaner['Noise Level'].value_counts()

# rearrange the columns 
res_df_cleaner = res_df_cleaner[['Name', 'Categories', 'Address', 'City', 'State', 'ZIP', 'Is Open', 
                                 'Total Reviews', 'Star Rating',  'Business Accepts Credit Cards', 'Alcohol', 'Noise Level',
                                 'Restaurants Good For Groups', 'Restaurants Reservations','Latitude', 'Longitude']]
       
# res_df_cleaner.head(3)

print("\nData cleansing, transformations, munging completed successfully!")
print("Saving the output file to 'Output' folder...")
# save the output to CSV
res_df_cleaner.to_csv("Output/yelp_restaurants.csv", index=False)
print("------------------------------------------------------------------------------------")
