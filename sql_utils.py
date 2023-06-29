import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd

user = 'root'
password = 'root'
host = '127.0.0.1'
port = 3306
database = 'youtube_data_harvest'
engine = create_engine(
    url="mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}".format(
        user, password, host, port, database
    )
)

def save_channel_data(channel_data):
    if not channel_data:
        return
    with engine.connect() as connection:
        connection.execute(text("DELETE FROM channels WHERE channel_id = :channel_id"), {"channel_id": channel_data["channel_id"]})
        connection.execute(
            text(
                "INSERT INTO channels (channel_id, channel_name, channel_views, channel_description, channel_privacy_status) \
                VALUES (:channel_id, :channel_name, :channel_views, :channel_description, :channel_privacy_status)"
            ), 
            channel_data
        )
        connection.commit()

def save_playlists_data(playlists_data):
    if len(playlists_data) == 0:
        return
    with engine.connect() as connection:
        channel_id = playlists_data[0]["channel_id"]
        connection.execute(text("DELETE FROM playlists WHERE channel_id = :channel_id"), {"channel_id": channel_id})
        connection.execute(
            text(
                "INSERT INTO playlists (playlist_id, channel_id, playlist_name) \
                VALUES (:playlist_id, :channel_id, :title)"
            ), 
            playlists_data
        )
        connection.commit()

def save_videos_data(videos_data):
    if len(videos_data) == 0:
        return
    with engine.connect() as connection:
        channel_id = videos_data[0]["channel_id"]
        connection.execute(text("DELETE FROM videos WHERE channel_id = :channel_id"), {"channel_id": channel_id})
        for i in range(0, len(videos_data), 50):
            try:
                connection.execute(
                    text(
                        "INSERT INTO videos (video_id, channel_id, playlist_id, video_name, video_description, published_date, view_count, like_count, favourite_count, comment_count, duration, thumbnail, caption_status) \
                            VALUES (:video_id, :channel_id, :playlist_id, :title, :description, STR_TO_DATE(:published_date,'%Y-%m-%dT%H:%i:%sZ'), :views, :likes, :favorite_count, :comments, :duration, :thumbnail, :caption_status)"
                    ),
                    videos_data[i:i+50]
                )
                connection.commit()
            except Exception as e:
                print(e)
                pass

def save_comments_data(comments_data):
    if len(comments_data) == 0:
        return
    with engine.connect() as connection:
        channel_id = comments_data[0]["channel_id"]
        connection.execute(text("DELETE FROM comments WHERE channel_id = :channel_id"), {"channel_id": channel_id})
        for i in range(0, len(comments_data), 50):
            try:
                connection.execute(
                    text(
                        "INSERT INTO comments (comment_id, video_id, channel_id, comment_text, comment_author, comment_published_date) \
                            VALUES (:comment_id, :video_id, :channel_id, :comment_text, :comment_author, STR_TO_DATE(:comment_published_date,'%Y-%m-%dT%H:%i:%sZ'))"
                    ),
                    comments_data[i:i+50]
                )
                connection.commit()
            except Exception as e:
                print(e)
                pass

def query_data(sql_query, parameters = None):
    try:
        df = pd.read_sql_query(sql = sql_query, con = engine)
        return df
    except:
        pass
    return pd.DataFrame([])