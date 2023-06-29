# Youtube Data Harvester
A software to extract information about channel(s) and make them available in a queryable form.

## Requirements
1. MongoDB
2. Postgres

## Libraries used
1. pymongo
2. sqlalchemy
3. streamlit
4. google-api-python-client
5. isodate

## Configurations needed

### Setup Youtube API Key
Goto youtube-utils.py and replace the api_key with your API key.

### Setup Mongo DB
Create a database 'youtube_data_harvester' in MongoDB.
Create the following collections under the database.
1. channels
2. playlists
3. videos
4. comments
#### If not using local, update the connection parameters in mongo-utils.py

### Setup MySQL
Create a database 'youtube_data_harvester' in MySQL.
Create the following tables under the database using the SQL scripts mentioned.
1. channels - sql/channels.sql
2. playlists - sql/playlists.sql
3. videos - sql/videos.sql
4. comments - sql/comments.sql
#### If not using local, update the connection parameters in sql-utils.py

# To run
``` 
streamlit run main.py
````