CREATE TABLE videos(
	video_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255),
    playlist_id VARCHAR(255),
    video_name TEXT,
    video_description TEXT,
    published_date DATETIME,
    view_count BIGINT,
    like_count BIGINT,
    favourite_count BIGINT,
    comment_count BIGINT,
    duration BIGINT,
    thumbnail TEXT,
    caption_status VARCHAR(255),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
    FOREIGN KEY (playlist_id) REFERENCES playlists(playlist_id) ON DELETE CASCADE
);