from pymongo import MongoClient
import collections
import json
from bson import json_util
import pprint
import datetime

client = MongoClient('localhost', 27017)

# Create or open the A4dbNorm database on server.
db = client["A4dbNorm"]

# Create or read the two collections
collection_artists = db["Artists"]
collection_tracks = db["Tracks"]

start = datetime.datetime(1950, 1, 1, 0, 0, 0, 0)

pipeline = [
    {"$lookup": 
        {
            "localField": "tracks",
            "from": "Tracks",
            "foreignField": "track_id",
            "as": "tracks"
        }
    },
    { "$unwind": "$tracks"},
    { "$match": {"tracks.release_date": {"$gte": start}}},
    {"$project":{
        "name":"$name",
        "t_name":"$tracks.name",
        "t_release_date": "$tracks.release_date"}
    }
]

aggregate_result = list(collection_artists.aggregate(pipeline))

#output the result
for entry in aggregate_result:
    print(entry)