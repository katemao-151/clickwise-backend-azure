import os
import json
import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def convert_numpy_to_python(data):
    if isinstance(data, dict):
        return {k: convert_numpy_to_python(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_to_python(v) for v in data]
    elif isinstance(data, np.generic):
        return data.item()
    else:
        return data

def basic_stats_exists(channel_id, collection):
    return collection.find_one({'_id': str(channel_id)}) is not None


def store_basic_stats(channel_id, result, collection):
    db_result = collection.update_one(
    {'_id': channel_id},  # Query matches the document by ID
    {'$set': 
        {'basic_stats': result},
        
    },  # $set operation with your hashmap
    upsert=True  # Inserts a new document if one doesn't exist  
)

    return db_result

def fetch_basic_stats(channel_id, collection):
    try:
        # Fetch the document
        document = collection.find_one({'_id': str(channel_id)})

        # Prepare the data for the DataFrame
        if document:
            # Extract the videos array and the document ID
            basic_stats = document['basic_stats']
            return basic_stats
    except Exception as e:
        print(f"An error occurred: {e}")
        return None