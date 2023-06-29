CREATE TABLE comments(
	comment_id VARCHAR(255) PRIMARY KEY,
	video_id VARCHAR(255),
    channel_id VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_date DATETIME,
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE CASCADE,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);