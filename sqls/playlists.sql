CREATE TABLE playlists(
	playlist_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255),
    playlist_name VARCHAR(255),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE
);