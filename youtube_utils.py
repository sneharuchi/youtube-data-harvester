from googleapiclient.discovery import build
from datetime import datetime
import isodate

def youtube_data_api():
    api_key = "########"
    youtube = build("youtube", "v3", developerKey=api_key)
    return youtube

def get_channel_data(youtube, channel_id):
    # Implement the logic to retrieve channel data using the YouTube API
    # Return the relevant channel data as a dictionary
    channels = youtube.channels()
    channel_data = channels.list(part = "snippet,contentDetails,statistics,status", id = channel_id).execute()
    channel_name = channel_data["items"][0]["snippet"]["title"]
    subscriber_count = int(channel_data["items"][0]["statistics"]["subscriberCount"])
    video_count = int(channel_data["items"][0]["statistics"]["videoCount"])
    view_count = int(channel_data["items"][0]["statistics"].get("viewCount", 0))
    uploaded_playlist = channel_data["items"][0]["contentDetails"].get("relatedPlaylists", {}).get("uploads", None)
    description = channel_data["items"][0]["snippet"]["description"]
    privacy_status = channel_data["items"][0]["status"]["privacyStatus"]
    channel_for_kids = channel_data["items"][0]["status"]["madeForKids"]
    return {
        "channel_id": channel_id,
        "channel_name": channel_name,
        "channel_description": description,
        "channel_subscribers": subscriber_count, 
        "channel_videos": video_count,
        "channel_views": view_count,
        "channel_privacy_status": privacy_status,
        "channel_for_kids": channel_for_kids,
        "uploads_playlist": uploaded_playlist
    }

def __process_playlist_items(playlist_items):
    return_data = []
    for item in playlist_items:
        return_data.append(
            {
                "playlist_id": item["id"],
                "title": item["snippet"]["title"],
                "channel_id": item["snippet"]["channelId"]
            }
        )
    return return_data

def get_playlists(youtube, channel_id):
    playlists = youtube.playlists()
    playlists_data = playlists.list(part = "snippet,contentDetails", channelId = channel_id).execute()
    next_page_token = None
    if "nextPageToken" in playlists_data:
        next_page_token = playlists_data["nextPageToken"]
    yield __process_playlist_items(playlists_data["items"])
    while next_page_token is not None:
        playlists_data = playlists.list(part = "snippet,contentDetails", channelId = channel_id, pageToken=next_page_token).execute()
        if "nextPageToken" in playlists_data:
            next_page_token = playlists_data["nextPageToken"]
        else:
            next_page_token = None
        yield __process_playlist_items(playlists_data["items"])
    
def __process_playlist_items_items(playlist_items, playlist_id):
    return_data = []
    for item in playlist_items:
        return_data.append(
            {
                "video_id": item["contentDetails"].get("videoId", ""),
                "title": item["snippet"].get("title", ""),
                "playlist_id": playlist_id,
                "channel_id": item["snippet"].get("channelId", "")
            }
        )
    return return_data

def get_videos_in_playlist(youtube, playlist_id):
    playlist_items = youtube.playlistItems()
    playlist_items_data = playlist_items.list(part = "snippet,contentDetails", playlistId = playlist_id, maxResults = 25).execute()
    next_page_token = None
    if "nextPageToken" in playlist_items_data:
        next_page_token = playlist_items_data["nextPageToken"]
    yield __process_playlist_items_items(playlist_items_data["items"], playlist_id)
    while next_page_token is not None:
        playlist_items_data = playlist_items.list(part = "snippet,contentDetails", playlistId = playlist_id, maxResults = 25, pageToken = next_page_token).execute()    
        if "nextPageToken" in playlist_items_data:
            next_page_token = playlist_items_data["nextPageToken"]
        else:
            next_page_token = None
        yield __process_playlist_items_items(playlist_items_data["items"], playlist_id)
    
def get_video_details(youtube, video_ids, video_playlist_map):
    videos = youtube.videos()
    for i in range(0, len(video_ids), 50):
        return_data = []
        response = videos.list(part="snippet,contentDetails,statistics",id=','.join(video_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(
                channel_name = video['snippet']['channelTitle'],
                channel_id = video['snippet']['channelId'],
                video_id = video['id'],
                title = video['snippet']['title'],
                tags = video['snippet'].get('tags'),
                thumbnail = video['snippet']['thumbnails']['default']['url'],
                description = video['snippet']['description'],
                published_date = datetime.strptime(video['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"),
                duration = isodate.parse_duration(video['contentDetails']['duration']).seconds,
                views = int(video['statistics'].get('viewCount', 0)),
                likes = int(video['statistics'].get('likeCount', 0)),
                comments = int(video['statistics'].get('commentCount', 0)),
                favorite_count = int(video['statistics']['favoriteCount']),
                definition = video['contentDetails']['definition'],
                caption_status = video['contentDetails']['caption'],
                playlist_id = video_playlist_map.get(video['id'], "")
            )
            return_data.append(video_details)
        yield return_data

def __process_comment_thread_items(comment_thread_items, channel_id):
    return_data = []
    for item in comment_thread_items:
        return_data.append({
            "comment_id": item["id"],
            "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
            "comment_author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
            "comment_published_date": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
            "video_id": item["snippet"]["topLevelComment"]["snippet"]["videoId"],
            "channel_id": channel_id
        })
    return return_data

def get_comments(youtube, video_id, channel_id, max_comments = 50):
    return_data = []
    comment_threads = youtube.commentThreads()
    try:
        comment_thread_data = comment_threads.list(part = "snippet", videoId = video_id, maxResults = 25).execute()
        next_page_token = None
        if "nextPageToken" in comment_thread_data:
            next_page_token = comment_thread_data["nextPageToken"]
        return_data.extend(__process_comment_thread_items(comment_thread_data["items"], channel_id))
        while next_page_token is not None:
            comment_thread_data = comment_threads.list(part = "snippet", videoId = video_id, maxResults = 25, pageToken = next_page_token).execute()    
            if "nextPageToken" in comment_thread_data:
                next_page_token = comment_thread_data["nextPageToken"]
            else:
                next_page_token = None
            return_data.extend(__process_comment_thread_items(comment_thread_data["items"], channel_id))
            if (len(return_data) >= max_comments):
                break    
    except Exception as e:
        print(e)
    return return_data