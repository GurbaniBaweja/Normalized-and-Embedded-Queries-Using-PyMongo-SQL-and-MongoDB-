from pymongo import MongoClient
import collections
import json
from bson import json_util
import pprint

client = MongoClient('localhost', 27017)

# Create or open the A4dbNorm database on server.
db = client["A4dbEmbed"]

# Create or read the two collections
collection_artists = db["ArtistsTracks"]

pipeline = [
    {"$match": {"tracks": {"$not": { "$size": 0 } }}},
    {
    "$project":{
        "_id":1,
        "artist_id":1,
        "name":1,
        "num_tracks":{ "$size": "$tracks"}}
    }
]

aggregate_result = list(collection_artists.aggregate(pipeline))

#output the result
for entry in aggregate_result:
    print(entry)