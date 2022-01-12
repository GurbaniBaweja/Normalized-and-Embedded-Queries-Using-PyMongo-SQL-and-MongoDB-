from pymongo import MongoClient
import collections
import json
from bson import json_util
import pprint

client = MongoClient('localhost', 27017)

# Create or open the A4dbNorm database on server.
db = client["A4dbEmbed"]

# Create or read the two collections
collection_tracks = db["ArtistsTracks"]


pipeline = [
    { "$unwind": "$tracks" },
    { "$match": 
        { 
            "tracks.track_id": { "$regex": "^70.*"} 
        } 
    },
    { "$group": 
        { 
            "_id": '', 
            "avg_danceability": { "$avg": "$tracks.danceability" } 
        }
    }
]

aggregate_result = list(collection_tracks.aggregate(pipeline))

#output the result
for entry in aggregate_result:
    print(entry)
