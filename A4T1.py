from pymongo import MongoClient
import collections
import json
from bson import json_util
import pprint

client = MongoClient('localhost', 27017)

# Create or open the A4dbNorm database on server.
db = client["A4dbNorm"]

# Create or read the two collections
collection_artists = db["Artists"]
collection_tracks = db["Tracks"]

# delete all previous entries in the two collections
collection_artists.delete_many({})
collection_tracks.delete_many({})

# get data from json files
with open("./artists.json") as f1:
    data_artists = json.load(f1)

with open("./tracks.json") as f2:
    data_tracks = json.load(f2)

print("Artist size: " + str(len(data_artists)))
print("Tracks size: " + str(len(data_tracks)))


# Populates the Artists collection
for artist_doc in data_artists:
    temp_str = json.dumps(artist_doc)
    ret = collection_artists.insert_one(json_util.loads(temp_str))

# Populates the Tracks collection
for track_doc in data_tracks:
    temp_str = json.dumps(track_doc)
    ret = collection_tracks.insert_one(json_util.loads(temp_str))


print(collection_artists.count_documents({}))
print(collection_tracks.count_documents({}))






