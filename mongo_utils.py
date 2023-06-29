from pymongo import MongoClient
client = MongoClient("localhost", 27017)
db = client["youtube_data_harvester"]

def save_data(collection_name, channel_id, data):
    collection = db[collection_name]
    collection.delete_many({"channel_id": channel_id})
    if type(data) is list:
        if len(data) > 0:
            collection.insert_many(data)
    else:
        collection.insert_one(data)

def delete_all_data(collection_name, channel_id):
    collection = db[collection_name]
    collection.delete_many({"channel_id": channel_id})

def get_all_channel_data(collection_name, channel_id):
    return_data = []
    collection = db[collection_name]
    for item in collection.find({"channel_id": channel_id}):
        del(item["_id"])
        return_data.append(item)
    return return_data

def get_all_data(collection_name):
    return_data = []
    collection = db[collection_name]
    for item in collection.find():
        del(item["_id"])
        return_data.append(item)
    return return_data