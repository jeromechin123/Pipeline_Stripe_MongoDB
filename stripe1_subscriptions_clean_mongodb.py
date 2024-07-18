import pandas as pd
from pymongo import MongoClient
import numpy as np
import pprint
import stripe
import os 
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
 
# Access environment variables
connection_url = os.getenv("connection_url")


try: 
       
    client = MongoClient(connection_url)
        
    client.admin.command('ping')
    print("Database connection established...")
except Exception as e:
    print(f"Database connection error!! {e}" )

# Connect to collection in database

database_name = "curated_data"
import_collection_name = "stripe1_subscriptions_selected"

db = client[database_name]
collection = db[import_collection_name]

# Extract all data from collection 

data = list(collection.find())    

# Flatten data to get json fields in individual columns -- Use '_' as seperator

flatten_data = pd.json_normalize(data, sep='_')

# Drop '_id' column to prevent upserting errors

flatten_data.drop(columns = ['_id'],axis = 1 , inplace=True)

# Rename field columns

rename_columns = {"stripe_subscriptions_plan_amount":"plan_amount",
                  "stripe_subscriptions_plan_interval_count":"interval_count",
                  "stripe_subscriptions_plan_interval":"interval",
                  "stripe_subscriptions_plan_nickname":"plan_name",
                  "stripe_subscriptions_plan_active":"plan_active",
                  "stripe_subscriptions_id":"id",
                  "stripe_subscriptions_customer":"customer",
                  "stripe_subscriptions_status":"status",
                  "stripe_customers_name":"name",
                  "stripe_subscriptions_created":"created",
                  "stripe_subscriptions_current_period_start":"current_period_start",
                  "stripe_subscriptions_current_period_end":"current_period_end",
                  "stripe_customers_email":"email"
                  }
flatten_data.rename(columns = rename_columns, inplace=True)

# Divide numbers by 100 

flatten_data["plan_amount"] = flatten_data["plan_amount"].div(100)

# Reformat date from UNIX epoch to datetime

flatten_data['created'] = flatten_data['created'].apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours=10))
flatten_data['current_period_start'] = flatten_data['current_period_start'].apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours=10))
flatten_data['current_period_end'] = flatten_data['current_period_end'].apply(lambda x: datetime.utcfromtimestamp(x) + timedelta(hours=10))

# Clean name and emails to remove inconsistencies

flatten_data[['name', 'email']] = flatten_data[['name', 'email']].apply(lambda x: x.str.lower())
flatten_data['name'] = flatten_data['name'].str.title()

# Make new column for how much the plan cost per month after discount
flatten_data['months'] = np.where(flatten_data['interval'] == 'year', 12, flatten_data['interval_count'])
flatten_data['monthly_amount'] = flatten_data['plan_amount'] / flatten_data['months']


# Fill up empty plan_name with plan_amount
plan_dict = {192: "Boomer", 50: "Platinum", 20: "Boomer", 10: "Starter"}
flatten_data['mapped_name'] = flatten_data['plan_amount'].map(plan_dict)
flatten_data['plan_name'] = flatten_data['plan_name'].fillna(flatten_data['mapped_name'])
flatten_data.drop('mapped_name', axis=1, inplace=True)

# Convert data back to a dictionary for insertion into MongoDB 

data_dict = flatten_data.to_dict(orient='records')

# Upsert into MongoDB

def create_collection(db, collection_name):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created successfully.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def upsert_to_mongodb(data, connection_url, db_name, collection_name):
    client = MongoClient(connection_url)
    db = client[db_name]
    # Create collection if it doesn't exist
    create_collection(db, collection_name)
    collection = db[collection_name]
    
    for record in data:     
        filter_query = {"id": record["id"]}
        update_query = {"$set": record}
        collection.update_one(filter_query, update_query, upsert=True)

    if len(data_to_upsert) ==  len(list(collection.find())):
        print(f"All data processed and upserted to collection '{collection_name}' successfully!")
    else:
        print("Numbers do not match..")
    

# Database to upsert into

insert_database = 'curated_data'
collection_name = 'stripe1_subscriptions_cleaned'
data_to_upsert = data_dict

# Upsert

print(f"Upserting {len(data_to_upsert)} records...")
upsert_to_mongodb(data_to_upsert, connection_url, insert_database, collection_name)
print(f"Completed processing {collection_name} data.")
