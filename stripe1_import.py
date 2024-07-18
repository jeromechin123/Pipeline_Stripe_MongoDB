import pandas as pd
from pymongo import MongoClient
import pprint
import stripe
import os 
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
 
# Access environment variables
connection_url = os.getenv("connection_url")
stripe.api_key = os.getenv("stripe.api_key")


try: 
       
    client = MongoClient(connection_url)        
    client.admin.command('ping')
    print("Database connection established...")
except Exception as e:
    print(e)

    

def get_all_data(endpoint_func):
    all_data = []
    starting_after = None

    while True:
        response = endpoint_func(limit=100, starting_after=starting_after)
        all_data.extend(response['data'])
        if not response['has_more']:
            break
        starting_after = response['data'][-1]['id']
    
    return all_data

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
        filter_query = {'id': record['id']}
        update_query = {"$set": record}
        collection.update_one(filter_query, update_query, upsert=True)

    print(f"Data upserted to MongoDB collection '{collection_name}' successfully!")
    
# Define Stripe endpoints and corresponding MongoDB collections
endpoints = {
    'customers': stripe.Customer.list,
    'invoices': stripe.Invoice.list,
    'balance_transactions': stripe.BalanceTransaction.list,
    'charges': stripe.Charge.list
}

collections = {
    'customers': 'stripe_customers',
    'invoices': 'stripe_invoices',
    'balance_transactions': 'stripe_balance_transactions',
    'charges': 'stripe_charges'
}

# Database to upsert into

insert_database = 'landing_zone'

# Loop through each endpoint and process the data
for endpoint_name, endpoint_func in endpoints.items():
    print(f"Processing {endpoint_name} data...")
    all_data = get_all_data(endpoint_func)
    print(len(all_data))
    upsert_to_mongodb(all_data, connection_url, insert_database, collections[endpoint_name])
    print(f"Completed processing {endpoint_name} data.")

# check if len(all_data) != document_count(), then return false

print("All data processed and upserted to MongoDB.")

