import youtube_utils as yt
import mongo_utils as datalake
import streamlit as st
import pandas as pd
import sql_utils as warehouse

# Periodic videos channel id: UCtESv1e7ntJaLJYKIO1FoYw

youtube = yt.youtube_data_api()

data_fetch_complete = True
channel_id = None
selected_channel_name = None
selected_channel_id = None

sidebar = st.sidebar
sidebar.header('Youtube Data Harvester')

channels_in_datalake = datalake.get_all_data("channels")

sql_query_map = {
    "What are the names of all the videos and their corresponding channels?": "SELECT video_name, channel_name FROM videos v INNER JOIN channels c ON v.channel_id = c.channel_id WHERE c.channel_id = :channel_id",
    "Which channels have the most number of videos, and how many videos do they have?": "SELECT channel_name, COUNT(video_id) video_count FROM channels c INNER JOIN videos v ON c.channel_id = v.channel_id GROUP BY c.channel_id ORDER BY video_count DESC",
    "What are the top 10 most viewed videos and their respective channels?": "SELECT video_name, view_count, channel_name FROM videos v INNER JOIN channels c ON c.channel_id = v.channel_id ORDER BY view_count DESC LIMIT 10"
}

def save_data_to_lake():
    print("Saving data to warehouse for channel: {}".format(channel_id))
    datalake.save_data("channels", channel_id, channel_data)
    datalake.save_data("playlists", channel_id, playlists_data)
    datalake.save_data("videos", channel_id, videos_details)
    datalake.save_data("comments", channel_id, comments_data)

def save_data_to_warehouse():
    warehouse.save_channel_data(channel_data)
    warehouse.save_playlists_data(playlists_data)
    warehouse.save_videos_data(videos_details)
    warehouse.save_comments_data(comments_data)

def render_data():
    st.button("Save data to lake", on_click=save_data_to_lake)
    st.button("Save data to warehouse", on_click=save_data_to_warehouse)
    st.write('Channel Name      : {}'.format(channel_data["channel_name"]))
    st.write('Total Subscribers : {}'.format(channel_data["channel_subscribers"]))
    st.write('Total Videos      : {}'.format(channel_data["channel_videos"]))
    st.write('Total Views       : {}'.format(channel_data["channel_views"]))
    st.write("Total playlists   : {}".format(len(playlists_data)))

    playlist_df = pd.DataFrame.from_dict(playlists_data)
    st.write("Playlist details")
    st.dataframe(playlist_df, use_container_width=True)

    video_df = pd.DataFrame.from_dict(videos_details)
    st.write("Video details")
    st.dataframe(video_df, use_container_width=True)
    
    comments_df = pd.DataFrame.from_dict(comments_data)
    st.write("Comment details")
    st.dataframe(comments_df, use_container_width=True)

def fetch_data_from_youtube():
    global data_fetch_complete
    global channel_id
    global channel_data
    global videos_data
    global videos_details
    global playlists_data 
    global comments_data
    data_fetch_complete = False
    videos_data = []
    videos_details = []
    playlists_data = []
    comments_data = []
    channel_data = {}
    
    channel_data = yt.get_channel_data(youtube, channel_id)
    sidebar.write('Channel Name      : {}'.format(channel_data["channel_name"]))
    sidebar.write('Total Subscribers : {}'.format(channel_data["channel_subscribers"]))
    sidebar.write('Total Videos      : {}'.format(channel_data["channel_videos"]))
    sidebar.write('Total Views       : {}'.format(channel_data["channel_views"]))
    total_videos = channel_data["channel_videos"]
    uploads_playlist = channel_data["uploads_playlist"]
    videos_data = []
    videos_details = []
    playlists_data = []
    video_ids = []
    video_playlist_map = {}
    
    for playlist_data in yt.get_playlists(youtube, channel_id):
        playlists_data.extend(playlist_data)

    sidebar.write("Total playlists   : {}".format(len(playlists_data)))
    upload_playlist_exists = False
    for playlist in playlist_data:
        if playlist["playlist_id"] == uploads_playlist:
            upload_playlist_exists = True

    progress_text = "Getting data all videos in playlists, please wait..." 
    progress_bar = sidebar.progress(0, text=progress_text)
    
    # Get videos list
    number_of_playlists = len(playlists_data)
    
    for i in range(number_of_playlists):
        progress_text = "Getting videos in playlist {}...".format(playlists_data[i]["title"]) 
        progress_bar.progress((i/number_of_playlists), progress_text)
        for video_data in yt.get_videos_in_playlist(youtube, playlists_data[i]["playlist_id"]):
            for item in video_data:
                if item["video_id"] not in video_ids and item["channel_id"] == channel_id:
                    videos_data.append(item)
                    video_ids.append(item["video_id"])
            

    progress_text = "Getting videos from all uploads, please wait..."
    for video_data in yt.get_videos_in_playlist(youtube, uploads_playlist):
        for item in video_data:
                if item["video_id"] not in video_ids and item["channel_id"] == channel_id:
                    videos_data.append(item)
                    video_ids.append(item["video_id"])

        progress = (len(video_ids)/float(total_videos))
        if progress > 1:
            progress = 1
        progress_bar.progress(progress, progress_text)

    if not upload_playlist_exists:
        playlists_data.append(
            {
                "playlist_id": uploads_playlist,
                "title": "All Uploads in Channel",
                "channel_id": channel_id
            }
        )
    # Get video details
    video_ids = [video_data["video_id"] for video_data in videos_data]
    video_playlist_map = {video_data["video_id"]: video_data["playlist_id"] for video_data in videos_data}
    total_videos = len(video_ids)
    progress_text = "Getting video details, please wait..."
    videos_details = []
    for video_detail in yt.get_video_details(youtube, video_ids, video_playlist_map):
        progress_bar.progress((len(videos_details)/total_videos), progress_text)
        for item in video_detail:
            if item["channel_id"] != channel_id:
                continue
            videos_details.append(item)
    
    # progress_text = "Getting comments for videos, please wait..."
    # for i in range(total_videos):
    #     progress_bar.progress((i/total_videos), progress_text)
    #     comments_data.extend(yt.get_comments(youtube, video_ids[i], channel_id))
    data_fetch_complete = True
    render_data()

def fetch_data_from_datalake():
    print("Fetching data from datalake for channel_id {}".format(selected_channel_id))
    if selected_channel_id is None or selected_channel_id == "None":
        return
    channels = datalake.get_all_channel_data("channels", selected_channel_id)
    global channel_data
    channel_data = channels[0]
    global playlists_data
    playlists_data = datalake.get_all_channel_data("playlists", selected_channel_id)
    global videos_details 
    videos_details = datalake.get_all_channel_data("videos", selected_channel_id)
    global comments_data 
    comments_data = datalake.get_all_channel_data("comments", selected_channel_id)
    render_data()
    
if len(channels_in_datalake) > 0:
    channel_names = ["Select a channel"]
    channel_names.extend([channel["channel_name"] for channel in channels_in_datalake])
    selected_channel_name = sidebar.selectbox("Channels available in lake:", channel_names)
    channel_id_found = False
    if selected_channel_name is not None and selected_channel_name != "Select a channel":
        for channel in channels_in_datalake:
            if channel["channel_name"] == selected_channel_name:
                selected_channel_id = channel["channel_id"]
                channel_id_found = True
                break
    if not channel_id_found:
        selected_channel_id = None
    if selected_channel_id and channel_id_found:
        sidebar.button("Fetch data from Datalake", on_click=fetch_data_from_datalake)

if selected_channel_id is not None:
    available_queries = [key for key, value in sql_query_map.items()]
    selected_query_name = st.selectbox("Select a query to run against warehouse", available_queries)
    selected_query = sql_query_map[selected_query_name]
    st.write(warehouse.query_data(selected_query, {"channel_id": selected_channel_id}))
channel_id = sidebar.text_input('Fetch for a new channel', "")
sidebar.button("Fetch data from Youtube", on_click=fetch_data_from_youtube)