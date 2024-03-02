import pandas as pd
import numpy as np
def convert_numpy_to_python(data):
    if isinstance(data, dict):
        return {k: convert_numpy_to_python(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_to_python(v) for v in data]
    elif isinstance(data, np.generic):
        return data.item()
    else:
        return data
    
def get_channel_age(df):
    df['video_publish_date'] = pd.to_datetime(df['video_publish_date'], format='%Y-%m-%dT%H:%M:%SZ')
    df.set_index('video_publish_date', inplace=True)

    # Assuming the age of the channel can be determined from the range of 'video_publish_date'
    channel_start_date = df.index.min()
    channel_end_date = df.index.max()

    # Calculate the age of the channel
    channel_age = channel_end_date - channel_start_date

    # Convert age to years, months, days
    # Note: Approximating 1 month as 30 days for simplicity
    years = channel_age.days // 365
    months = (channel_age.days % 365) // 30
    days = (channel_age.days % 365) % 30
    return channel_end_date, {'years': years, 'months': months, 'days': days}


def get_most_viewed_videos(channel_end_date, df):
    result = {}
    most_viewed_month = df[(df.index.month == channel_end_date.month) & (df.index.year == channel_end_date.year)].sort_values(by='views', ascending=False).iloc[0]

# Finding the most viewed video of the year
    most_viewed_year = df[df.index.year == channel_end_date.year].sort_values(by='views', ascending=False).iloc[0]

    # Finding the most viewed video of all time
    most_viewed_all_time = df.sort_values(by='views', ascending=False).iloc[0]

    most_viewed_month_title = most_viewed_month['title']
    most_viewed_month_views = most_viewed_month['views']
    most_viewed_month_thumbnail = most_viewed_month['video_thumbnail']

    most_viewed_year_title = most_viewed_year['title']
    most_viewed_year_views = most_viewed_year['views']
    most_viewed_year_thumbnail = most_viewed_year['video_thumbnail']

    most_viewed_all_time_title = most_viewed_all_time['title']
    most_viewed_all_time_views = most_viewed_all_time['views']
    most_viewed_all_time_thumbnail = most_viewed_all_time['video_thumbnail']  

    result['most_viewed_month_title'] = most_viewed_month_title
    result['most_viewed_month_views'] = most_viewed_month_views
    result['most_viewed_month_thumbnail'] = most_viewed_month_thumbnail

    result['most_viewed_year_title '] = most_viewed_year_title 
    result['most_viewed_year_views'] = most_viewed_year_views
    result['most_viewed_year_thumbnail'] = most_viewed_year_thumbnail 

    result['most_viewed_all_time_title'] = most_viewed_all_time_title
    result['most_viewed_all_time_views'] = most_viewed_all_time_views
    result['most_viewed_all_time_thumbnail'] = most_viewed_all_time_thumbnail

    return result

def get_logviews_likes_comments(df):
    #df['video_publish_date'] = pd.to_datetime(df['video_publish_date'], format='%Y-%m-%dT%H:%M:%SZ')
    #df.set_index('video_publish_date', inplace=True)

    df['log_likes'] = np.log(df['likes'] + 1)
    df['log_comments'] = np.log(df['comments'] + 1)
    result = {
        'index': df.index.tolist(),
        'log_views': df['logviews'].tolist(),
        'log_likes': df['log_likes'].tolist(),
        'log_comments': df['log_comments'].tolist()
    }

    return result


def get_monthly_uploads(df):
    #df['video_publish_date'] = pd.to_datetime(df['video_publish_date'], format='%Y-%m-%dT%H:%M:%SZ')
    #df.set_index('video_publish_date', inplace=True)

    monthly_uploads = df.resample('ME').size()
    monthly_uploads.index = monthly_uploads.index.strftime('%Y-%m')
    monthly_uploads_dict = {
        "monthly_uploads_xaxis": monthly_uploads.index.tolist(),
        "monthly_uploads_yaxis": monthly_uploads.tolist()
    }
    return monthly_uploads_dict


def get_videoCount_per_category(df):
    category_counts = df['video_category_id'].value_counts().sort_index()
    categories = category_counts.index.tolist()  # Category IDs
    counts = category_counts.values.tolist()  # Counts of videos in each category
    result = {
        "videoCount_per_category_xaxis": categories,
        "videoCount_per_category_yaxis": counts,
    }
    return result

def get_average_views_per_category(df):
    avg_views_per_category = df.groupby('video_category_id')['views'].mean()
    avg_views_per_category_df = avg_views_per_category.reset_index()
    avg_views_per_category_df.columns = ['Video Category ID', 'Average Views']

    result = {
        'avg_views_per_category_xaxis': avg_views_per_category_df['Video Category ID'].tolist(),
        'avg_views_per_category_yaxis': avg_views_per_category_df['Average Views'].tolist()
    }
    return result


def get_engagement_score(df):
    df.fillna(0, inplace=True)

    # Ensure 'views' column will never be 0 to avoid division by zero.
    # You could set a minimum value for 'views' to a small positive number if it's 0.
    # This avoids changing the original meaning of your data as much as possible.
    df['views'] = df['views'].replace(0, 1)

    # Calculate engagement score with the updated 'views' column
    df['engagement_score'] = (10 * df['likes'] + 100 * df['comments']) / df['views']

    # Resample to get monthly engagement score, mean of engagement scores within each month
    monthly_engagement = df.resample('M')['engagement_score'].mean()

    # Convert the index to 'YYYY-MM' format
    monthly_engagement.index = monthly_engagement.index.strftime('%Y-%m')

    monthly_engagement.fillna(0, inplace=True)

    # Print the monthly engagement for debugging
    print('##########################################')
    print(monthly_engagement)
    print('##########################################')

    

    # Prepare the data dictionary
    result = {
        "monthly_engagement_score_xaxis": monthly_engagement.index.tolist(),
        "monthly_engagement_score_yaxis": monthly_engagement.tolist(),
    }
    return result





