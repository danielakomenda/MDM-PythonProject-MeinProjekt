import os
import dotenv

import pymongo
import pandas as pd


def connect_to_db():
    """Open the connection to the DB and return the collection
    Create collection with unique index, if there is not yet one"""
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Get MongoDB-URI
    mongodb_uri = os.getenv("MONGODB_URI")
    DBclient = pymongo.MongoClient(mongodb_uri)
    db = DBclient["MDM-Python-MeinProjekt"]

    return db["Wetter"]


def extract_daily_average_weather():

    collection = connect_to_db()

    pipeline = [
        {"$addFields": {"date": {"$substr": ["$datetime", 0, 10]}}},
        {
            "$group": {
                "_id": "$date",
                "avg_temp": {"$avg": "$temp_C"},
                "min_temp": {"$min": "$temp_C"},
                "max_temp": {"$max": "$temp_C"},
                "rain": {"$avg": "$rain_mm"},
                "wind_speed": {"$avg": "$wind_kmh"},
                "clouds": {"$avg": "$cloud_percent"},
            }
        },
    ]

    results = []
    for x in collection.aggregate(pipeline):
        results.append(x)

    df = pd.DataFrame(results)
    df = df.set_index("_id")
    df = df.sort_index()
    df.index = df.index.rename("date")
    df["wind_speed"] /= 3.6
    df["wind_power"] = df["wind_speed"] ** 3

    return df
