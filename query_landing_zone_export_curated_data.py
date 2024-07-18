import pandas as pd
from pymongo import MongoClient
import pprint
import os 
from dotenv import load_dotenv
from bson import SON


# Load environment variables from the .env file
load_dotenv()
 
# Access environment variables
connection_url = os.getenv("connection_url")

def create_collection(db, collection_name):
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created successfully.")
    else:
        print(f"Collection '{collection_name}' already exists.")

def insert_to_mongodb(data, connection_url, db_name, collection_name):
    client = MongoClient(connection_url)
    db = client[db_name]
    # Create collection if it doesn't exist
    create_collection(db, collection_name)
    collection = db[collection_name]

    # Delete all documents in the collection
    collection.delete_many({})
    
    # Cycle through the data and insert one record at a time
    for record in data:
        collection.insert_one(record)

    print(f"Data upserted to MongoDB collection '{collection_name}' successfully!")

# Query stripe_subscription joined with stripe_customer sorted by created date

client = MongoClient(connection_url)
database = client["landing_zone"]
collection = database["stripe_subscriptions"]

# query pipeline for mongoDB

pipeline = [
    {
        u"$project": {
            u"_id": 0,
            u"stripe_subscriptions": u"$$ROOT"
        }
    }, 
    {
        u"$lookup": {
            u"localField": u"stripe_subscriptions.customer",
            u"from": u"stripe_customers",
            u"foreignField": u"id",
            u"as": u"stripe_customers"
        }
    }, 
    {
        u"$unwind": {
            u"path": u"$stripe_customers",
            u"preserveNullAndEmptyArrays": False
        }
    }, 
    {
        u"$sort": SON([ (u"stripe_subscriptions.created", -1) ])
    }, 
    {
        u"$project": {
            u"stripe_subscriptions.id": u"$stripe_subscriptions.id",
            u"stripe_subscriptions.customer": u"$stripe_subscriptions.customer",
            u"stripe_customers.email": u"$stripe_customers.email",
            u"stripe_customers.name": u"$stripe_customers.name",
            u"stripe_subscriptions.status": u"$stripe_subscriptions.status",
            u"stripe_subscriptions.created": u"$stripe_subscriptions.created",
            u"stripe_subscriptions.current_period_start": u"$stripe_subscriptions.current_period_start",
            u"stripe_subscriptions.current_period_end": u"$stripe_subscriptions.current_period_end",
            u"stripe_subscriptions.plan.amount": u"$stripe_subscriptions.plan.amount",
            u"stripe_subscriptions.plan.interval_count": u"$stripe_subscriptions.plan.interval_count",
            u"stripe_subscriptions.plan.interval": u"$stripe_subscriptions.plan.interval",
            u"stripe_subscriptions.plan.nickname": u"$stripe_subscriptions.plan.nickname",
            u"stripe_subscriptions.plan.active": u"$stripe_subscriptions.plan.active",
            u"_id": 0
        }
    }
]


cursor = collection.aggregate(
    pipeline, 
    allowDiskUse = True
)

# export cursor back to mongodb database in the curated_data collection as a stripe1_subscriptions_selected

db_name = "curated_data"
collection_name = "stripe1_subscriptions_selected"

insert_to_mongodb(cursor, connection_url, db_name, collection_name)

# Create pipeline to query stripe_charges 

database = client["landing_zone"]
collection = database["stripe_charges"]

pipeline_charges = {
        "id": "$id",
        "amount": "$amount",
        "amount_captured": "$amount_captured",
        "customer": "$customer",
        "description": "$description",
        "billing_details.name": "$billing_details.name",
        "receipt_email": "$receipt_email",
        "billing_details.address.postal_code": "$billing_details.address.postal_code",
        "captured": "$captured",
        "paid": "$paid",
        "created": "$created",
        "failure_code": "$failure_code",
        "refunded": "$refunded",
        "_id": False
    }


cursor = collection.find(projection=pipeline_charges).sort("created", -1)

# insert to mongodb at stripe1_charges_selected

db_name = "curated_data"
collection_name = "stripe1_charges_selected"

insert_to_mongodb(cursor, connection_url, db_name, collection_name)

# Create pipeline to query stripe2_charges 

database = client["landing_zone"]
collection = database["stripe2_charges"]

pipeline_charges2 = {
        "id": "$id",
        "amount": "$amount", 
        "amount_captured": "$amount_captured", 
        "captured": "$captured", 
        "paid": "$paid", 
        "created": "$created", 
        "outcome.reason": "$outcome.reason", 
        "status": "$status", 
        "refunded": "$refunded", 
        "billing_details.email": 
        "$billing_details.email", 
        "billing_details.name": "$billing_details.name", 
        "billing_details.address.postal_code": "$billing_details.address.postal_code", 
        "_id": False
    }


cursor = collection.find(projection=pipeline_charges2).sort("created", -1)

# insert to mongodb at stripe2_charges_selected

db_name = "curated_data"
collection_name = "stripe2_charges_selected"

insert_to_mongodb(cursor, connection_url, db_name, collection_name)
