import os
import json
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import pickle



url = os.environ.get('MONGODB_URL')
client = MongoClient(url, server_api=ServerApi('1'))
db = client['clickwise-db']

def get_top_channel_category_id(df):
    # Group by 'channel_id' and 'video_category_id', then count occurrences
    channel_categories = df.groupby(['channel_id', 'video_category_id']).size().reset_index(name='count')

    # Sort by 'count' and 'video_category_id' (to handle ties by selecting the highest 'video_category_id')
    channel_categories = channel_categories.sort_values(by=['count', 'video_category_id'], ascending=[False, False])
    
    # Keep the first occurrence for each channel_id after sorting
    channel_categories = channel_categories.drop_duplicates(subset=['channel_id'], keep='first')

    # If the DataFrame is empty after operations, return None
    if channel_categories.empty:
        return None

    # Directly access the 'video_category_id' of the first row
    top_channel_category_id = channel_categories.iloc[0]['video_category_id']

    return top_channel_category_id

def store_channel_data(df, collection):
    channel_id = df["channel_id"].iloc[0]
    channel_category_id = get_top_channel_category_id(df)
    videos = df.drop(['channel_id'], axis=1).to_dict('records')
    collection = db['channel_data']
    timestamp_ms = int(datetime.now().timestamp() * 1000)
          # Prepare an upsert operation for each channel
    result = collection.update_one(
    {'_id': channel_id},
    {
        '$set': {
            'channel_category': channel_category_id,  # Set the channel category
            'lastModified': timestamp_ms,  # Update lastModified timestamp
        },
        '$push': {'videos': {'$each': videos}}  # Append the videos to the channel
    },
    upsert=True
   )
    return result



def fetch_channel_data(channel_id, collection):
    try:
        # Fetch the document
        document = collection.find_one({'_id': str(channel_id)})

        # Prepare the data for the DataFrame
        if document:
            # Extract the videos array and the document ID
            videos = document['videos']
            doc_id = document['_id']
            
            # Add the document ID to each video object
            for video in videos:
                video['channel_id'] = doc_id
            
            # Create the DataFrame
            df = pd.DataFrame(videos)
            print(df)
            return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

    

def update_user_query_history(user_id, channel_id, collection):
    queried_timestamp = datetime.now()
    update_result = collection.update_one(
        {'_id': user_id},  # Match the document by user_id
        {
            '$set': {
                f'{channel_id}': queried_timestamp  # Dynamically set/update the channel's timestamp
            }
        },
        upsert=True  # Create a new document if user_id does not exist
    )
    return update_result
    


def fetch_user_history(user_id, collection):
    document = collection.find_one({'_id': user_id})
    
    if document:
        return document
    else:
        # Return an empty dict or appropriate message if no document is found
        return "No history found for the given user_id."
    

def channel_id_exists(channel_id, collection):
    return collection.find_one({'_id': str(channel_id)}) is not None


def upload_model_to_mongodb(channel_id, channel_stats, collection):
    result = collection.update_one(
    {'_id': channel_id},  # Query matches the document by ID
    {'$set': 
        {'channel_stats': channel_stats},
        
    },  # $set operation with your hashmap
    upsert=True  # Inserts a new document if one doesn't exist  
)

    return result



def download_model_from_mongodb(channel_id, collection):
    # Construct the file path
    try:
        document = collection.find_one({'_id': str(channel_id)})
        if document:
            channel_stats = document['channel_stats']
            return channel_stats
    except Exception as e:
        return {'error': str(e)}


def key_words_exist(channel_id, collection):
    return collection.find_one({'_id': str(channel_id)}) is not None


def model_exists(channel_id, collection):
    return collection.find_one({'_id': str(channel_id)}) is not None




def store_keywords(channel_id, top_key_words, bottom_key_words, popular_key_words, collection):
    result = collection.update_one(
    {'_id': channel_id},  # Query matches the document by ID
    {'$set': 
        {'top_keywords': top_key_words, 
         'bottom_keywords': bottom_key_words, 
         'popular_keywords': popular_key_words
         },
        
    },  # $set operation with your hashmap
    upsert=True  # Inserts a new document if one doesn't exist  
)

    return result


def fetch_keywords(channel_id, collection):
    try:
        # Fetch the document
        document = collection.find_one({'_id': str(channel_id)})

        # Prepare the data for the DataFrame
        if document:
            # Extract the videos array and the document ID
            top_keywords = document.get('top_keywords', '')
            bottom_keywords = document.get('bottom_keywords', '')
            popular_keywords = document.get('popular_keywords', '')
            

            return top_keywords, bottom_keywords, popular_keywords
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


    