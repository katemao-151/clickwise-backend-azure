import re
from googleapiclient.discovery import build
import pandas as pd
import aiohttp
import asyncio



def identify_channel_url_type(url):
    standard_pattern = r'^https:\/\/www\.youtube\.com\/channel\/UC[-_A-Za-z0-9]+\/?$'
    custom_pattern = r'^https:\/\/www\.youtube\.com\/c\/[-_A-Za-z0-9]+\/?$'
    legacy_pattern = r'^https:\/\/www\.youtube\.com\/user\/[-_A-Za-z0-9]+\/?$'
    short_custom_pattern = r'^https:\/\/www\.youtube\.com\/@[-_A-Za-z0-9]+\/?$'

    if re.match(standard_pattern, url):
        return 1  # Standard channel URL
    elif re.match(custom_pattern, url):
        return 2  # Custom channel URL
    elif re.match(legacy_pattern, url):
        return 3  # Legacy username-based channel URL
    elif re.match(short_custom_pattern, url):
        return 4  # Shortened custom URL
    else:
        return -1  # Invalid or not recognized URL
    

def get_channel_id(url, url_type, api_key='AIzaSyBqU1fe2sP7kizG_h3NmsIewH_7x0RG5mM'):
    youtube = build('youtube', 'v3', developerKey=api_key)

    
    if url_type in [1]:  # Standard channel URL
        print('got here')
        channel_id = url.split('/')[-1]
        print(channel_id)
        return channel_id

    elif url_type in [2, 3, 4]:  # Custom, legacy or shortened custom URL
        channel_name = url.split('/')[-1]
        print('got in here')
        
        search_response = youtube.search().list(
            part="snippet",
            type="channel",
            q=channel_name,
            maxResults=1
        ).execute()

        if len(search_response['items']) > 0:
            print('found matching results')
            print(search_response['items'][0]['snippet']['channelId'])
            return search_response['items'][0]['snippet']['channelId']
        else:
            return 'channel not found' # Channel not found

    else:
        return 'unsupported url'  # Unsupported or unrecognized URL
    

def convert_duration(duration):
    # Parse the duration string into a timedelta object
    duration_regex = re.compile(
        r'P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?',
        re.IGNORECASE
    )
    matches = duration_regex.match(duration)
    if not matches:
        return 0

    parts = {name: int(param) if param else 0 for name, param in zip(('days', 'hours', 'minutes', 'seconds'), matches.groups())}
    total_seconds = parts['days'] * 86400 + parts['hours'] * 3600 + parts['minutes'] * 60 + parts['seconds']

    return total_seconds


async def fetch_url(session, url):
    async with session.get(url) as response:
        return await response.json()

async def fetch_all_video_ids_from_playlist(session, playlist_id, api_key):
    base_url = "https://www.googleapis.com/youtube/v3/playlistItems"
    video_ids = []
    page_token = None

    while True:
        # Construct the API URL with optional page token for pagination
        playlist_url = f"{base_url}?part=contentDetails&playlistId={playlist_id}&maxResults=50&key={api_key}"
        if page_token:
            playlist_url += f"&pageToken={page_token}"
        
        # Fetch the playlist page
        response = await fetch_url(session, playlist_url)
        items = response.get("items", [])
        
        # Extract video IDs from the current page
        for item in items:
            video_ids.append(item['contentDetails']['videoId'])
        
        # Check for next page
        page_token = response.get("nextPageToken")
        if not page_token:
            break  # Exit the loop if there are no more pages

    return video_ids

async def fetch_video_details(session, channel_id, vid_ids, api_key):
    base_url = "https://www.googleapis.com/youtube/v3/videos"
    # Join the video IDs into a comma-separated string
    ids = ','.join(vid_ids)
    # Specify the parts of the video details you want to retrieve
    parts = "statistics,snippet,contentDetails"
    # Construct the request URL
    videos_url = f"{base_url}?part={parts}&id={ids}&key={api_key}"

    async with session.get(videos_url) as response:
            # Ensure the response status is OK
            if response.status == 200:
                # Parse the response JSON
                data = await response.json()
                return data
            else:
                # Handle potential errors (for simplicity, just raising an exception here)
                raise Exception(f"Failed to fetch video details, status code: {response.status}")




async def get_channel_videos(channel_id, api_key='AIzaSyBqU1fe2sP7kizG_h3NmsIewH_7x0RG5mM'):

    youtube = build('youtube', 'v3', developerKey=api_key)
    async with aiohttp.ClientSession() as session:
        base_url = "https://www.googleapis.com/youtube/v3"
        
        # Determine if channel_id is valid and get uploads playlist ID
        channel_url = f"{base_url}/channels?part=contentDetails,snippet,statistics&id={channel_id}&key={api_key}"
        if channel_id.startswith('UC') and len(channel_id) == 24:
            channel_response = await fetch_url(session, channel_url)
            if not channel_response.get("items"):
                raise ValueError("No channel found with the provided identifier")
            for item in channel_response.get('items', []):
              subscriber_count= item['statistics']['subscriberCount']
              channel_name= item['snippet']['title']
              channel_logo_url= item['snippet']['thumbnails']['default']['url']
              creation_date= item['snippet']['publishedAt']
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        # Fetch all video IDs from the playlist (simplified; implement pagination as needed)
        video_ids = await fetch_all_video_ids_from_playlist(session, uploads_playlist_id, api_key)
        
        # Split video IDs into chunks for batching
        vid_ids_chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]  # Adjust the chunk size as needed
        
        # Parallel fetching of video details for each chunk
        tasks = [fetch_video_details(session, channel_id, chunk, api_key) for chunk in vid_ids_chunks]
        responses = await asyncio.gather(*tasks)
        videos = []
        for response in responses:
            for item in response['items']:
                # Process each video item as needed...
                videos.append({
                    'video_id': item['id'],
                    'channel_id': channel_id,
                    'subscriber_count' :subscriber_count,
                    'channel_name': channel_name,
                    'channel_logo_url': channel_logo_url,
                    'creation_date': creation_date,
                    'title': item['snippet']['title'],
                    'video_thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'video_length': item['contentDetails']['duration'],
                    'video_publish_date': item['snippet']['publishedAt'],
                    'views': int(item['statistics'].get('viewCount', 0)),
                    'likes': int(item['statistics'].get('likeCount', 0)),
                    'comments': int(item['statistics'].get('commentCount', 0)),
                    'video_category_id': item['snippet']['categoryId']

        })
    df = pd.DataFrame(videos[::-1])
    return df
