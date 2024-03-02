import pandas as pd
import numpy as np
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
import time
from collections import defaultdict

def get_residual(fetched_video_details):
    table = fetched_video_details
    ret_df = pd.DataFrame([])
    problematic_channel = []
    channels = list(table['channel_id'].unique())
    for channel in channels:
      channel_df = table[table['channel_id'] == channel]
      channel_df.dropna(subset=['views'], inplace=True)
      channel_df.dropna(subset=['video_publish_date'], inplace=True)
      video_publish_dates = channel_df['video_publish_date']

      utc = np.array([])
      date_time_format = '%Y-%m-%dT%H:%M:%SZ'  # ISO 8601 format

      for date_time_str in video_publish_dates:
          date_time_obj = datetime.strptime(date_time_str, date_time_format)
          time_tuple = date_time_obj.timetuple()
          epoch_timestamp = time.mktime(time_tuple)
          utc = np.append(utc, epoch_timestamp)

      seconds_in_year = 365.25 * 24 * 60 * 60
      years = (utc / seconds_in_year) + 1970

      # Convert to numpy array for easier handling
      channel_df['years'] = years
      channel_df = channel_df.sort_values(by='years').reset_index(drop=True)

      years = channel_df['years']
      views = channel_df['views'].astype(int)
      logviews = np.log10(views + 1).tolist()

      try:
        model = ARIMA(logviews, order=(1, 0, 1))
        model_fit = model.fit()

        # Generate predictions
        predictions = model_fit.predict()

        # Plot the results

        # Calculate residuals
        residuals = list(model_fit.resid)

        channel_df['logviews'] = logviews
        channel_df['residuals'] = residuals
        ret_df = pd.concat([ret_df, channel_df], ignore_index=True)
      except:
        print(channel)
        problematic_channel.append(channel)

    return ret_df, problematic_channel


def get_channel_model_weights(channel_id, df):
    channel_stats, instances = {}, {}
    channel_stats[channel_id] = {'scores': {}, 'counts': {}, 'fragments': {}}
    table = df.reset_index(drop=True)
    residuals, video_ids, titles  = table['residuals'], table['video_id'], table['title']
    for n, title in enumerate(titles):
            tokens = title.split()
            for j in range(len(tokens)):
                for k in range(1, min(5, len(tokens) - j + 1)):
                    ngram = ' '.join(tokens[j:j+k])
                    if ngram not in instances:
                        instances[ngram]= {'scores': [], 'video_ids': []}
                    instances[ngram]['video_ids'] += [video_ids[n]]
                    instances[ngram]['scores'] += [residuals[n]]
        
    for ngram in instances:
            channel_stats[channel_id]['scores'][ngram] = sum(instances[ngram]['scores']) / (len(instances[ngram]['scores']) + 10)
            channel_stats[channel_id]['counts'][ngram] = len(instances[ngram]['scores'])
    for ngram in instances:
        if len(ngram.split()) > 1 and channel_stats[channel_id]['counts'][ngram] > 5:
                prefix = ' '.join(ngram.split()[:-1])
                suffix = ' '.join(ngram.split()[1:])
                if channel_stats[channel_id]['counts'][prefix] < 2 * channel_stats[channel_id]['counts'][ngram]:
                    channel_stats[channel_id]['fragments'][prefix] = True
                if channel_stats[channel_id]['counts'][suffix] < 2 * channel_stats[channel_id]['counts'][ngram]:
                    channel_stats[channel_id]['fragments'][suffix] = True
    return channel_stats

def top_keywords(channel_id, channel_stats):
    counts = channel_stats[channel_id]['counts']
    scores = channel_stats[channel_id]['scores']
    fragments = channel_stats[channel_id]['fragments']

    sorted_tuples = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    sorted_keys = [k for k, v in sorted_tuples]
    printed_count = 0

    ret = []

    for key in sorted_keys:
        if printed_count >= 100:
            break
        if key not in fragments and counts[key] > 10:
            rounded = round(10 ** scores[key], 2)
            ret.append([key, rounded, counts[key]])
            printed_count += 1
    return ret

def bottom_keywords(channel_id, channel_stats):
    counts = channel_stats[channel_id]['counts']
    scores = channel_stats[channel_id]['scores']
    fragments = channel_stats[channel_id]['fragments']

    sorted_tuples = sorted(scores.items(), key=lambda item: item[1])
    sorted_keys = [k for k, v in sorted_tuples]
    printed_count = 0

    ret = []

    for key in sorted_keys:
        if printed_count >= 100:
            break
        if key not in fragments and counts[key] > 10:
            rounded = round(10 ** scores[key], 2)
            ret.append([key, rounded, counts[key]])
            printed_count += 1
    return ret

def popular_keywords(channel_id, channel_stats):
    counts = channel_stats[channel_id]['counts']
    scores = channel_stats[channel_id]['scores']
    fragments = channel_stats[channel_id]['fragments']

    sorted_tuples = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    sorted_keys = [k for k, v in sorted_tuples]
    printed_count = 0

    ret = []

    for key in sorted_keys:
        if printed_count >= 100:
            break
        if key not in fragments and counts[key] > 10:
            rounded = round(10 ** scores[key], 2)
            ret.append([key, rounded, counts[key]])
            printed_count += 1
    return ret



def evaluate_video_title(channel, title, channel_stats, verbose = True):
    counts = defaultdict(lambda: 1, channel_stats[channel]['counts'])
    scores = defaultdict(lambda: 0, channel_stats[channel]['scores'])

    tokens = title.split()

    while True:
      pairings = []
      for i in range(len(tokens) - 1):
          merged = tokens[i] + ' ' + tokens[i + 1]
          pairings += [(counts[merged] - 1) ** 2 / (counts[tokens[i]] * counts[tokens[i + 1]])]
      m = np.argmax(pairings)
      if pairings[m] > 0.1:
          tokens = tokens[:m] + [tokens[m] + ' ' + tokens[m+1]] + tokens[m+2:]
      else:
          if verbose == True:
              print([[token, counts[token], np.round(10 ** scores[token], 3)] for token in tokens])
          return 10 ** sum([scores[token] for token in tokens])

def generate_arima_plots(table):
    problematic_channel = []
    channels = list(table['channel_id'].unique())
    for channel in channels:
      channel_df = table[table['channel_id'] == channel]
      channel_df.dropna(subset=['views'], inplace=True)
      channel_df.dropna(subset=['comments'], inplace=True)
      channel_df.dropna(subset=['likes'], inplace=True)
      channel_df.dropna(subset=['video_publish_date'], inplace=True)
      video_publish_dates = channel_df['video_publish_date']

      utc = np.array([])
      date_time_format = '%Y-%m-%dT%H:%M:%SZ'  # ISO 8601 format

      for date_time_str in video_publish_dates:
          date_time_obj = datetime.strptime(date_time_str, date_time_format)
          time_tuple = date_time_obj.timetuple()
          epoch_timestamp = time.mktime(time_tuple)
          utc = np.append(utc, epoch_timestamp)

      seconds_in_year = 365.25 * 24 * 60 * 60
      years = (utc / seconds_in_year) + 1970

      # Convert to numpy array for easier handling
      channel_df['years'] = years
      channel_df = channel_df.sort_values(by='years').reset_index(drop=True)

      years = channel_df['years']
      views = channel_df['views'].astype(int)
      comments = channel_df['comments'].astype(int)
      likes = channel_df['likes'].astype(int)
      logviews = np.log10(views + 1).tolist()
      logcomments = np.log10(comments + 1).tolist()
      loglikes = np.log10(likes + 1).tolist()
      comment_per_view = [comment / view if view != 0 else 0 for comment, view in zip(comments, views)]
      likes_per_view = [like / view if view != 0 else 0 for like, view in zip(likes, views)]


      try:
        #logview models
        model_logviews = ARIMA(logviews, order=(1, 0, 1))
        model_logview_fit = model_logviews.fit()
        logview_predictions = model_logview_fit .predict()
        logview_residuals = list(model_logview_fit .resid)

        #logcomments models
        model_logcomments = ARIMA(logcomments, order=(1, 0, 1))
        model_logcomments_fit = model_logcomments.fit()
        logcomments_predictions = model_logcomments_fit .predict()
        logcomments_residuals = list(model_logcomments_fit .resid)

         #logilkes models
        model_loglikes = ARIMA(loglikes, order=(1, 0, 1))
        model_loglikes_fit = model_loglikes.fit()
        loglikes_predictions = model_loglikes_fit .predict()
        loglikes_residuals = list(model_loglikes_fit .resid)

        #comment_per_view models
        model_comment_per_view = ARIMA(comment_per_view, order=(1, 0, 1))
        model_comment_per_view_fit = model_comment_per_view.fit()
        comment_per_view_predictions = model_comment_per_view_fit .predict()
        comment_per_view_residuals = list(model_comment_per_view_fit .resid)

        #likes_per_view models
        model_likes_per_view = ARIMA(likes_per_view, order=(1, 0, 1))
        model_likes_per_view_fit = model_likes_per_view.fit()
        likes_per_view_predictions = model_likes_per_view_fit  .predict()
        likes_per_view_residuals = list(model_comment_per_view_fit .resid)

        df = pd.DataFrame({
           'years': years,
           'logviews': logviews,
           'logview_predictions': logview_predictions,
           'logview_residuals': logview_residuals,
           'logcomments': logcomments,
           'logcomments_predictions':  logcomments_predictions,
           'logcomments_residuals': logcomments_residuals,
           'loglikes': loglikes,
           'loglikes_predictions': loglikes_predictions,
           'loglikes_residuals': loglikes_residuals,
           'comment_per_view': comment_per_view,
           'comment_per_view_predictions': comment_per_view_predictions,
           'comment_per_view_residuals': comment_per_view_residuals,
           'comment_per_view_predictions': comment_per_view_predictions,
           'comment_per_view_residuals': comment_per_view_residuals,
           'likes_per_view': likes_per_view,
           'likes_per_view_predictions': likes_per_view_predictions,
           'likes_per_view_residuals': likes_per_view_residuals
        })
        print('got here')
        return df
      except Exception as e:
        print(e)
        problematic_channel.append(channel)

    print('errored out while trying to get arima')
    return None