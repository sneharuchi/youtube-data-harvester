CREATE TABLE channels(
	channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    channel_views BIGINT,
    channel_description TEXT,
    channel_privacy_status VARCHAR(255)
);