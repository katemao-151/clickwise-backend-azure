from gevent import monkey
monkey.patch_all()

from flask import Flask, request, jsonify
import pandas as pd
from request.database_request import store_channel_data, fetch_channel_data, update_user_query_history, channel_id_exists, fetch_user_history, key_words_exist, store_keywords, model_exists, upload_model_to_mongodb, download_model_from_mongodb,fetch_keywords
from request.basic_stats_request import basic_stats_exists, store_basic_stats, fetch_basic_stats, convert_numpy_to_python
from process_data.train_test_title_model import get_residual, evaluate_video_title, generate_arima_plots, get_channel_model_weights, top_keywords, bottom_keywords, popular_keywords
from request.youtube_request import get_channel_videos, identify_channel_url_type, get_channel_id
from process_data.basic_stats import get_channel_age, get_most_viewed_videos, get_logviews_likes_comments, get_monthly_uploads, get_videoCount_per_category, get_average_views_per_category, get_engagement_score
import re
import traceback
import sys
import logging
from flask_cors import CORS
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import requests
import httpx
import json
from io import StringIO

app = Flask(__name__)
CORS(app)

url = os.environ.get('MONGODB_URL')
client = MongoClient(url, server_api=ServerApi('1'))
db = client['clickwise-db']

@app.route('/add_channel', methods=['POST'])
async def add_channel():
    content = request.json
    channel_url, user_id = content.get('channel_url', ''), content.get('user_id', '')
    channel_url_type = identify_channel_url_type(channel_url)
    channel_id = get_channel_id(channel_url, channel_url_type)
    result = {
        'general_data_exists': False,
        'top_keywords_exists': False,
        'bottom_keywords_exists': False,
        'popular_keywords_exists': False,
        'model_exists': False,
        'added_channel_to_user': False
    }

    try:
        #check if channel data, keywords, model exists
        channel_exists = channel_id_exists(channel_id, collection=db['channel_data'])
        keywords_exist = key_words_exist(channel_id, collection=db['keywords'])
        model_exist_s = model_exists(channel_id,  collection=db['title_models'])
        updated_df, channel_stats = None, None

        # If channel does not exists
        if not channel_exists:
            # Fetch channel data from youtube
            df = await get_channel_videos(channel_id)

            # Add logviews, residuals, and other necessary fields for raw data
            updated_df, problematic_channel = get_residual(df)

            store_channel_data_result=store_channel_data(updated_df, collection=db['channel_data'])
            #except Exception as e:
                #print(f"Error during processing: {e}")
                #result['add_thumbnail_features'] = {'error': str(e)}
                #result['general_data_exists'] = False


            # Update result to be the status of the db call
            result['general_data_exists'] = store_channel_data_result.acknowledged
             

        else:
            # If channel exists
            # Fetch processed data from db
            updated_df = fetch_channel_data(channel_id, collection=db['channel_data'])

            # Set general_data_exists in result to be true
            result['general_data_exists'] =True
            #result['add_thumbnail_features'] = True
        
        # If model weights do not exist in current db 
        if not model_exist_s:
            
            # Compute model weights
            channel_stats = get_channel_model_weights(channel_id, updated_df)

            # Store model weights in db 
            upload_model_result = upload_model_to_mongodb(channel_id, channel_stats, collection=db['title_models'])

             # Set model_exists in result to be the status of the db call
            result['model_exists'] = upload_model_result.acknowledged
            
        # If model weights exists
        else:
            # Fetch model weights from db 
            channel_stats = download_model_from_mongodb(channel_id, collection=db['title_models'])

            # Set model_exists in result to be the True
            result['model_exists'] = True

        # If keywords do not exists in db 
        if not keywords_exist:

            # Calculate top, bottom, and popular keywords
            top_key_words = top_keywords(channel_id, channel_stats)
            bottom_key_words = bottom_keywords(channel_id, channel_stats)
            popular_key_words = popular_keywords(channel_id, channel_stats)

            # Store top, bottom, and popular keywords into db 
            store_keywords_result = store_keywords(channel_id, top_key_words, bottom_key_words, popular_key_words, collection=db['keywords'])

            # Set keywords in result to be the status of the db call
            result.update({
                'top_keywords_exists': store_keywords_result.acknowledged,
                'bottom_keywords_exists': store_keywords_result.acknowledged,
                'popular_keywords_exists': store_keywords_result.acknowledged
            })
       
        # If keywords exists in db
        else:
            # Set keywords in result to be True
            result.update({
                'top_keywords_exists': True,
                'bottom_keywords_exists': True,
                'popular_keywords_exists': True
            })

            
        # Update user history
        update_user_query_history_result = update_user_query_history(user_id, channel_id, collection=db['user_query_history'])
        result['added_channel_to_user'] = update_user_query_history_result.acknowledged


        
    except Exception as e:
        # If errored out 
        error_message = {'error': str(e)}
        result.update({
            'general_data_exists': error_message,
            'top_keywords_exists': error_message,
            'bottom_keywords_exists': error_message,
            'popular_keywords_exists': error_message,
            'model_exists': error_message,
            'added_channel_to_user': error_message
        })
    # TODO: add to task queue and retry? implement retry method?
    result['channel_id'] = channel_id

    return jsonify(result)


@app.route('/get_basic_stats', methods=['POST'])
def get_basic_stats():

    #restructure user_query_history, name/url mapping to channel_id?
    content = request.json
    channel_id, user_id = content.get('channel_id', ''), content.get('user_id', '')

    #TODO: add try except block here in case data does not exist
    updated_df = fetch_channel_data(channel_id, collection=db['channel_data'])  

    result = {
        'total_videos': None,
        'total_views': None,
        'total_likes': None,
        'total_comments': None,
        'channel_age': None,
        'most_viewed_month_title': None,
        'most_viewed_month_views': None,
        'most_viewed_month_thumbnail': None,
        'most_viewed_year_title': None,
        'most_viewed_year_views': None,
        'most_viewed_year_thumbnail': None,
        'most_viewed_all_time_title': None,
        'most_viewed_all_time_views': None,
        'most_viewed_all_time_thumbnail': None,
        'log_plot_xaxis': None,
        'log_views': None,
        'log_likes': None,
        'log_comments': None,
        'monthly_plots_xaxis': None,
        'monthly_plots_yaxis': None,
        'videoCount_per_category_xaxis': None,
        'videoCount_per_category_yaxis': None,
        'avg_views_per_category_xaxis': None,
        'avg_views_per_category_yaxis': None,
        'monthly_engagement_score_xaxis': None,
        'monthly_engagement_score_yaxis': None,
        'basic_stats_exists': False

    }

    if basic_stats_exists(channel_id, collection=db['basic_stats']):
        basic_stats = fetch_basic_stats(channel_id, collection = db['basic_stats'])
        print('here')
        result['basic_stats_exists'] = True
        result.update(basic_stats)
    else:


    # Calculating total metrics across the entire channel
        total_videos, total_views, total_likes, total_comments = len(updated_df), updated_df['views'].sum(), updated_df['likes'].sum(), updated_df['comments'].sum()
        result['total_videos'] = str(total_videos)
        print('#########################')
        print(type(result['total_videos']))
        print('##############################')
        result['total_views'] = str(total_views)
        result['total_likes'] = str(total_likes)
        result['total_comments'] = str(total_comments)

        # Calculate channel age
        channel_end_date, channel_age_dictionary = get_channel_age(updated_df)
        result['channel_age'] =  channel_age_dictionary

        # Calculate most viewed videos
        most_viewed = get_most_viewed_videos(channel_end_date, updated_df)
        result.update({
            'most_viewed_month_title': most_viewed.get('most_viewed_month_title'),
            'most_viewed_month_views': most_viewed.get('most_viewed_month_views'),
            'most_viewed_month_thumbnail': most_viewed.get('most_viewed_month_thumbnail'),
            'most_viewed_year_title': most_viewed.get('most_viewed_year_title'),
            'most_viewed_year_views': most_viewed.get('most_viewed_year_views'),
            'most_viewed_year_thumbnail': most_viewed.get('most_viewed_year_thumbnail'),
            'most_viewed_all_time_title': most_viewed.get('most_viewed_all_time_title'),
            'most_viewed_all_time_views': most_viewed.get('most_viewed_all_time_views'),
            'most_viewed_all_time_thumbnail': most_viewed.get('most_viewed_all_time_thumbnail'),
        })

        # Send time, logviews, logcomments, loglikes
        log_plots = get_logviews_likes_comments(updated_df)
       
        result.update({
            'log_plot_xaxis': log_plots.get('index'),
            'log_views': log_plots.get('log_views'),
            'log_likes': log_plots.get('log_likes'),
            'log_comments': log_plots.get('log_comments')
        })

        # Calculate monthly upload videos
        monthly_uploads = get_monthly_uploads(updated_df)
        result.update({
            'monthly_plots_xaxis': monthly_uploads.get('monthly_uploads_xaxis'),
            'monthly_plots_yaxis': monthly_uploads.get('monthly_uploads_yaxis')
        })

        # Calculate number of videos per category
        videoCount_per_category = get_videoCount_per_category(updated_df)
        result.update({
            'videoCount_per_category_xaxis': videoCount_per_category.get('videoCount_per_category_xaxis'),
            'videoCount_per_category_yaxis': videoCount_per_category.get('videoCount_per_category_yaxis')
        })

        average_views_per_category = get_average_views_per_category(updated_df)
        result.update({
            'avg_views_per_category_xaxis': average_views_per_category.get('avg_views_per_category_xaxis'),
            'avg_views_per_category_yaxis': average_views_per_category.get('avg_views_per_category_yaxis')
        })

        # Calculate engagement score
        engagement_score_calculation = get_engagement_score(updated_df)
        result.update({
            'monthly_engagement_score_xaxis': engagement_score_calculation.get('monthly_engagement_score_xaxis'),
            'monthly_engagement_score_yaxis': engagement_score_calculation.get('monthly_engagement_score_yaxis')
        })

        result = convert_numpy_to_python(result)

        store_basic_stats_result = store_basic_stats(channel_id, result, collection=db['basic_stats'])

        result['basic_stats_exists'] = store_basic_stats_result.acknowledged


    return jsonify(result)

@app.route('/evaluate_title', methods=['POST'])
async def evaluate_title():
    content = request.json
    channel_id, title = content.get('channel_id', ''), content.get('title', '')
    channel_exists = channel_id_exists(channel_id, collection=db['channel_data'])
    keywords_exist = key_words_exist(channel_id, collection=db['keywords'])
    model_exist_s = model_exists(channel_id,  collection=db['title_models'])

    result = {
        'title_score': None,
        'general_data_exists': None,
        'model_exists': None,
        'top_keywords': None,
        'bottom_keywords': None,
        'popular_keywords': None,
        'top_keywords_exists': None,
        'bottom_keywords_exists': None,
        'popular_keywords_exists': None
        }

    if model_exist_s:
        channel_stats = download_model_from_mongodb(channel_id, collection=db['title_models'])
        score = evaluate_video_title(channel_id, title, channel_stats)
        result['title_score'] = score
    else:
        if not channel_exists:
            # Fetch channel data from youtube
            df = await get_channel_videos(channel_id)

            # Add logviews, residuals, and other necessary fields for raw data
            updated_df, problematic_channel = get_residual(df)

            # Make db request to store processed data
            store_channel_data_result=store_channel_data(updated_df, collection=db['channel_data'])

            # Update result to be the status of the db call
            result['general_data_exists'] = store_channel_data_result.acknowledged

        else:
            # If channel exists
            # Fetch processed data from db
            updated_df = fetch_channel_data(channel_id, collection=db['channel_data'])
        
        channel_stats = get_channel_model_weights(channel_id, updated_df)

        # Store model weights in db 
        upload_model_result = upload_model_to_mongodb(channel_id, channel_stats, collection=db['title_models'])

        # Set model_exists in result to be the status of the db call
        result['model_exists'] = upload_model_result.acknowledged
        score = evaluate_video_title(channel_id, title, channel_stats)
        result['title_score'] = score
    
    if keywords_exist:
        top_key_words, bottom_key_words, popular_key_words = fetch_keywords(channel_id, collection=db['keywords'])
        result['top_keywords'] = top_key_words
        result['bottom_keywords'] = bottom_key_words
        result['popular_keywords'] = popular_key_words
    else:
        top_key_words = top_keywords(channel_id, channel_stats)
        bottom_key_words = bottom_keywords(channel_id, channel_stats)
        popular_key_words = popular_keywords(channel_id, channel_stats)

        # Store top, bottom, and popular keywords into db 
        store_keywords_result = store_keywords(channel_id, top_key_words, bottom_key_words, popular_key_words, collection=db['keywords'])

        # Set keywords in result to be the status of the db call
        result.update({
            'top_keywords_exists': store_keywords_result.acknowledged,
            'bottom_keywords_exists': store_keywords_result.acknowledged,
            'popular_keywords_exists': store_keywords_result.acknowledged
        })
        result['top_keywords'] = top_key_words
        result['bottom_keywords'] = bottom_key_words
        result['popular_keywords'] = popular_key_words

    return jsonify(result)


@app.route('/get_user_queried_channels', methods=['POST'])
def get_user_queried_channels():
    content = request.json
    user_id= content.get('user_id', '')
    result = {}

    doc = fetch_user_history(user_id, collection=db['user_query_history'])

    result['user_history'] = doc

    return jsonify(result)




@app.route('/get_channel_info', methods=['POST'])
def get_channel_info():
    content = request.json
    channel_url = content['channel_url']
    input_title = content.get('input_title', '')  #in case 
    user_id = content.get('user_id', '')

    channel_url_type = identify_channel_url_type(channel_url)
    print('channel_url type is, ', channel_url_type)
    print(type(channel_url_type))
    channel_id = get_channel_id(channel_url, channel_url_type) 
    print('channel_id is, ', channel_id)

    #match = re.search(r"(?<=youtube\.com/)(?:c/|channel/|user/|@)?([^/]+)", channel_url)
   # if not match:
    #    raise ValueError("The URL provided doesn't seem to be a valid YouTube channel URL")
    #channel_id = match.group(1)
    result = {}
    try:
        print(channel_id_exists(channel_id))
        if channel_id_exists(channel_id):
            #fetch channel data directly from current db
            print("found existing channel")
            updated_df = fetch_channel_data(channel_id)
            scores, fragment, counts, rows, ngram_dictionary, words = preprocess(updated_df)
            top_key_words = print_top_keywords(scores, fragment, counts)    #save scores, fragment, counts, all these into db as well?
            model = download_model_from_firebase(channel_id)
            expected_features = model.n_features_in_
            #print('number of expected features is: ', expected_features)
            #print('len of words are : ', len(words))
            if expected_features != len(words):
                model = TrainModel(updated_df, ngram_dictionary, words)
                upload_model_to_firebase(model, channel_id)
                update_user_query_history(user_id, channel_id)  #TODO: can streamline this to be one call
        else:
            df = get_channel_videos(channel_id)
            updated_df, problematic_channel  = get_residual(df)
            store_channel_data(updated_df)
            
            scores, fragment, counts, rows, ngram_dictionary, words = preprocess(updated_df)
            top_key_words = print_top_keywords(scores, fragment, counts)
            model = TrainModel(updated_df, ngram_dictionary, words)
            upload_model_to_firebase(model, channel_id)
            update_user_query_history(user_id, channel_id)
        if input_title: #if input title is not null, user wants to check out 
            title_score = evaluate_video_title(input_title, ngram_dictionary, words, model)
            result['title_score'] = title_score   
        result['top_key_words'] = top_key_words
        return jsonify(result)
    except Exception as e:
        print('inside this')
        tb = traceback.format_exc()
        app.logger.error(tb)
        response = {'error': str(e)}
        return response, 400


@app.route('/get_user_channel_query_history', methods=['POST'])
def get_user_channel_query_history():
    content = request.json
    user_id = content.get('user_id', '')
    return fetch_user_history(user_id)

@app.route('/get_channel_model', methods=['POST'])
def get_channel_model():
    content = request.json
    channel_id = content.get('channel_id', '')
    input_title = content.get('input_title', '')
    model = download_model_from_firebase(channel_id)
    result = {}
    if not isinstance(model, str):
        if input_title:
            updated_df = fetch_channel_data(channel_id)
            scores, fragment, counts, rows, ngram_dictionary, words = preprocess(updated_df)
            title_score = evaluate_video_title(input_title, ngram_dictionary, words, model)
            result['title_score'] = title_score 
        else:
            result['title_score'] = 1
    else:
        result['title_score']= 'model not found'
    return result

@app.route('/get_arima', methods=['POST'])
def get_arima():
    content = request.json
    channel_url = content['channel_url']
    channel_url_type = identify_channel_url_type(channel_url)
    channel_id = get_channel_id(channel_url, channel_url_type) 
    user_id = content.get('user_id', '')
    try:
        if channel_id_exists(user_id, channel_id):
            print('channel already existed, going in here')
            updated_df = fetch_channel_data(channel_id)  #TODO can be optimized since logview model was trained twice
            ret_df = generate_arima_plots(updated_df)
            return jsonify(ret_df.to_dict(orient='records'))
        else:
            df = get_channel_videos(channel_id)
            updated_df, problematic_channel  = get_residual(df)
            ret_df = generate_arima_plots(updated_df)
            return jsonify(ret_df.to_dict(orient='records'))
            
    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error(tb)
        response = {'error': str(e)}
        return response, 400
    




if __name__ == '__main__':
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s '
                                  '[in %(pathname)s:%(lineno)d]')
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.DEBUG)

    port = int(os.getenv('PORT', 80))  # Default to 80 if PORT not found
    app.run(threaded=True, port=port)

    #app.run(threaded=True, port=5000)
